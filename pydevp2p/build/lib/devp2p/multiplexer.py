from gevent.queue import Queue
from collections import OrderedDict
import rlp
from rlp.utils import str_to_bytes, is_integer
import struct
import sys

sys.setrecursionlimit(10000)  # frames are generated recursively

# chunked-0: rlp.list(protocol-type, sequence-id, total-packet-size)
header_data_sedes = rlp.sedes.List([rlp.sedes.big_endian_int] * 3, strict=False)


def ceil16(x):
    return x if x % 16 == 0 else x + 16 - (x % 16)


def rzpad16(data):
    if len(data) % 16:
        data += b'\x00' * (16 - len(data) % 16)
    return data


class MultiplexerError(Exception):
    pass


class DeserializationError(MultiplexerError):
    pass


class FormatError(MultiplexerError):
    pass


class FrameCipherBase(object):
    mac_len = 16
    header_len = 32
    dummy_mac = '\x00' * mac_len
    block_size = 16

    def encrypt(self, header, frame):
        assert len(header) == self.header_len
        assert len(frame) % self.block_size == 0
        return header + self.mac + frame + self.mac

    def decrypt_header(self, data):
        assert len(data) >= self.header_len + self.mac_len + 1 + self.mac_len
        return data[:self.header_len]

    def decrypt_body(self, data, body_size):
        assert len(data) >= self.header_len + self.mac_len + body_size + self.mac_len
        frame_offset = self.header_len + self.mac_len
        return data[frame_offset:frame_offset + body_size]


class Frame(object):

    """
    When sending a packet over RLPx, the packet will be framed.
    The frame provides information about the size of the packet and the packet's
    source protocol. There are three slightly different frames, depending on whether
    or not the frame is delivering a multi-frame packet. A multi-frame packet is a
    packet which is split (aka chunked) into multiple frames because it's size is
    larger than the protocol window size (pws; see Multiplexing). When a packet is
    chunked into multiple frames, there is an implicit difference between the first
    frame and all subsequent frames.
    Thus, the three frame types are
    normal, chunked-0 (first frame of a multi-frame packet),
    and chunked-n (subsequent frames of a multi-frame packet).


    Single-frame packet:
    header || header-mac || frame || mac

    Multi-frame packet:
    header || header-mac || frame-0 ||
    [ header || header-mac || frame-n || ... || ]
    header || header-mac || frame-last || mac
    """

    header_size = 16
    mac_size = 16
    padding = 16
    is_chunked_0 = False
    total_payload_size = None  # only used with chunked_0
    frame_cipher = None
    cipher_called = False

    def __init__(self, protocol_id, cmd_id, payload, sequence_id, window_size,
                 is_chunked_n=False, frames=None, frame_cipher=None):
        payload = memoryview(payload)
        assert is_integer(window_size)
        assert window_size % self.padding == 0
        assert isinstance(cmd_id, int) and cmd_id < 256
        self.cmd_id = cmd_id
        self.payload = payload
        if frame_cipher:
            self.frame_cipher = frame_cipher
        self.frames = frames or []
        assert protocol_id < 2**16
        self.protocol_id = protocol_id
        assert sequence_id is None or sequence_id < 2**16
        self.sequence_id = sequence_id
        self.is_chunked_n = is_chunked_n
        self.frames.append(self)

        # chunk payloads resulting in frames exceeding window_size
        fs = self.frame_size()
        if fs > window_size:
            if not is_chunked_n:
                self.is_chunked_0 = True
                self.total_payload_size = self.body_size()
            # chunk payload
            self.payload = payload[:window_size - fs]
            assert self.frame_size() <= window_size
            remain = payload[len(self.payload):]
            assert len(remain) + len(self.payload) == len(payload)
            Frame(protocol_id, cmd_id, remain, sequence_id, window_size,
                  is_chunked_n=True,
                  frames=self.frames,
                  frame_cipher=frame_cipher)
        assert self.frame_size() <= window_size

    def __repr__(self):
        return '<Frame(%s, len=%d sid=%r)>' % \
            (self._frame_type(), self.frame_size(), self.sequence_id)

    def _frame_type(self):
        return 'normal' * self.is_normal or 'chunked_0' * self.is_chunked_0 or 'chunked_n'

    def body_size(self, padded=False):
        # frame-size: 3-byte integer size of frame, big endian encoded (excludes padding)
        # frame relates to body w/o padding w/o mac
        l = len(self.enc_cmd_id) + len(self.payload)
        if padded:
            l = ceil16(l)
        return l

    def frame_size(self):
        # header16 || mac16 || dataN + [padding] || mac16
        return self.header_size + self.mac_size + self.body_size(padded=True) + self.mac_size

    @property
    def is_normal(self):
        return not self.is_chunked_n and not self.is_chunked_0

    @property
    def header(self):
        """
        header: frame-size || header-data || padding
        frame-size: 3-byte integer size of frame, big endian encoded
        header-data:
            normal: rlp.list(protocol-type[, sequence-id])
            chunked-0: rlp.list(protocol-type, sequence-id, total-packet-size)
            chunked-n: rlp.list(protocol-type, sequence-id)
            normal, chunked-n: rlp.list(protocol-type[, sequence-id])
            values:
                protocol-type: < 2**16
                sequence-id: < 2**16 (this value is optional for normal frames)
                total-packet-size: < 2**32
        padding: zero-fill to 16-byte boundary
        """
        assert self.protocol_id < 2**16
        assert self.sequence_id is None or self.sequence_id < 2**16
        l = [self.protocol_id]
        if self.is_chunked_0:
            assert self.sequence_id is not None
            l.append(self.sequence_id)
            l.append(self.total_payload_size)
        elif self.sequence_id is not None:  # normal, chunked_n
            l.append(self.sequence_id)
        header_data = rlp.encode(l, sedes=header_data_sedes)
        assert tuple(l) == rlp.decode(header_data, sedes=header_data_sedes, strict=False)
        # write body_size to header
        # frame-size: 3-byte integer size of frame, big endian encoded (excludes padding)
        # frame relates to body w/o padding w/o mac
        body_size = self.body_size()
        assert body_size < 256**3
        header = struct.pack('>I', body_size)[1:] + header_data
        header = rzpad16(header)  # padding
        assert len(header) == self.header_size
        return header

    @property
    def enc_cmd_id(self):
        if not self.is_chunked_n:
            return rlp.encode(self.cmd_id, sedes=rlp.sedes.big_endian_int)  # unsigned byte
        return b''

    @property
    def body(self):
        """
        frame:
            normal: rlp(packet-type) [|| rlp(packet-data)] || padding
            chunked-0: rlp(packet-type) || rlp(packet-data...)
            chunked-n: rlp(...packet-data) || padding
        padding: zero-fill to 16-byte boundary (only necessary for last frame)
        """
        b = self.enc_cmd_id  # packet-type
        length = len(b) + len(self.payload)
        assert isinstance(self.payload, memoryview)
        return b + self.payload.tobytes() + b'\x00' * (ceil16(length) - length)

    def get_frames(self):
        return self.frames

    def as_bytes(self):
        assert not self.cipher_called  # must only be called once
        if not self.frame_cipher:
            assert len(self.header) == 16 == self.header_size
            assert len(self.body) == self.body_size(padded=True)
            dummy_mac = b'\x00' * self.mac_size
            r = self.header + dummy_mac + self.body + dummy_mac
            assert len(r) == self.frame_size()
            return r
        else:
            self.cipher_called = True
            e = self.frame_cipher.encrypt(self.header, self.body)
            assert len(e) == self.frame_size()
            return e


class Packet(object):

    """
    Packets are emitted and received by subprotocols
    """

    def __init__(self, protocol_id=0, cmd_id=0, payload=b'', prioritize=False):
        self.protocol_id = protocol_id
        self.cmd_id = cmd_id
        self.payload = payload
        self.prioritize = prioritize

    def __repr__(self):
        return 'Packet(%r)' % dict(protocol_id=self.protocol_id,
                                   cmd_id=self.cmd_id,
                                   payload_len=len(self.payload),
                                   prioritize=self.prioritize)

    def __eq__(self, other):
        s = dict(self.__dict__)
        s.pop('prioritize')
        o = dict(other.__dict__)
        o.pop('prioritize')
        return s == o

    def __len__(self):
        return len(self.payload)


class Multiplexer(object):

    """
    Multiplexing of protocols is performed via dynamic framing and fair queueing.
    Dequeuing packets is performed in a cycle which dequeues one or more packets
    from the queue(s) of each active protocol. The multiplexor determines the
    amount of bytes to send for each protocol prior to each round of dequeuing packets.

    If the size of an RLP-encoded packet is less than 1 KB then the protocol may
    request that the network layer prioritize the delivery of the packet.
    This should be used if and only if the packet must be delivered before all other packets.
    The network layer maintains two queues and three buffers per protocol:
    a queue for normal packets, a queue for priority packets,
    a chunked-frame buffer, a normal-frame buffer, and a priority-frame buffer.


    Implemented Variant:

    each sub protocol has three queues
        prio
        normal
        chunked

    protocols are queried round robin

    """

    max_window_size = 8 * 1024
    max_priority_frame_size = 1024
    max_payload_size = 10 * 1024**2
    frame_cipher = None
    _cached_decode_header = None

    def __init__(self, frame_cipher=None):
        if frame_cipher:
            # assert isinstance(frame_cipher, FrameCipherBase)
            self.frame_cipher = frame_cipher
        self.queues = OrderedDict()  # protocol_id : dict(normal=queue, chunked=queue, prio=queue)
        self.sequence_id = dict() # protocol_id : counter
        self.last_protocol = None  # last protocol, which sent data to the buffer
        self.chunked_buffers = dict()  # decode: protocol_id: dict(sequence_id: buffer)
        self._decode_buffer = bytearray()

    @property
    def num_active_protocols(self):
        "A protocol is considered active if it's queue contains one or more packets."
        return sum(1 for p_id in self.queues if self.is_active_protocol(p_id))

    def is_active_protocol(self, protocol_id):
        return True if sum(q.qsize() for q in self.queues[protocol_id].values()) else False

    def protocol_window_size(self, protocol_id=None):
        """
        pws = protocol-window-size = window-size / active-protocol-count
        initial pws = 8kb
        """
        if protocol_id and not self.is_active_protocol(protocol_id):
            s = self.max_window_size // (1 + self.num_active_protocols)
        else:
            s = self.max_window_size // max(1, self.num_active_protocols)
        return s - s % 16  # should be a multiple of padding size

    def add_protocol(self, protocol_id):
        assert protocol_id not in self.queues
        self.queues[protocol_id] = dict(normal=Queue(),
                                        chunked=Queue(),
                                        priority=Queue())
        self.sequence_id[protocol_id] = 0
        self.chunked_buffers[protocol_id] = dict()
        self.last_protocol = protocol_id

    @property
    def next_protocol(self):
        protocols = tuple(self.queues.keys())
        if self.last_protocol == protocols[-1]:
            next_protocol = protocols[0]
        else:
            next_protocol = protocols[protocols.index(self.last_protocol) + 1]
        self.last_protocol = next_protocol
        return next_protocol

    def add_packet(self, packet):
        #protocol_id, cmd_id, rlp_data, prioritize=False
        sid = self.sequence_id[packet.protocol_id]
        self.sequence_id[packet.protocol_id] = (sid + 1) % 2**16
        frames = Frame(packet.protocol_id, packet.cmd_id, packet.payload,
                       sequence_id=sid,
                       window_size=self.protocol_window_size(packet.protocol_id),
                       frame_cipher=self.frame_cipher
                       ).frames
        queues = self.queues[packet.protocol_id]
        if packet.prioritize:
            assert len(frames) == 1
            assert frames[0].frame_size() <= self.max_priority_frame_size
            queues['priority'].put(frames[0])
        elif len(frames) == 1:
            queues['normal'].put(frames[0])
        else:
            for f in frames:
                queues['chunked'].put(f)

    def pop_frames_for_protocol(self, protocol_id):
        """
        If priority packet and normal packet exist:
            send up to pws/2 bytes from each (priority first!)
        else if priority packet and chunked-frame exist:
            send up to pws/2 bytes from each
        else
            if normal packet and chunked-frame exist: send up to pws/2 bytes from each
        else
            read pws bytes from active buffer

        If there are bytes leftover -- for example, if the bytes sent is < pws,
            then repeat the cycle.
        """

        pws = self.protocol_window_size()
        queues = self.queues[protocol_id]
        frames = []
        # size = lambda:
        size = 0
        while size < pws:
            frames_added = 0
            for qn in ('priority', 'normal', 'chunked'):
                q = queues[qn]
                if q.qsize():
                    fs = q.peek().frame_size()
                    if size + fs <= pws:
                        frames.append(q.get())
                        size += fs
                        frames_added += 1
                # add no more than two in order to send normal and priority first
                if frames_added == 2:
                    break  # i.e. next is 'priority' again
            # empty queues
            if frames_added == 0:
                break
        # the following can not be guaranteed, as pws might have been different
        # at the time where packets were framed and added to the queues
        # assert sum(f.frame_size() for f in frames) <= pws
        return frames

    def pop_frames(self):
        """
        returns the frames for the next protocol up to protocol window size bytes
        """
        protocols = tuple(self.queues.keys())
        idx = protocols.index(self.next_protocol)
        protocols = protocols[idx:] + protocols[:idx]
        assert len(protocols) == len(self.queues.keys())
        for p in protocols:
            frames = self.pop_frames_for_protocol(p)
            if frames:
                return frames
        return []

    def pop_all_frames(self):
        frames = []
        while True:
            r = self.pop_frames()
            frames.extend(r)
            if not r:
                break
        return frames

    def pop_all_frames_as_bytes(self):
        return b''.join(f.as_bytes() for f in self.pop_all_frames())

    def decode_header(self, buffer):
        assert isinstance(buffer, memoryview)
        assert len(buffer) >= 32
        if self.frame_cipher:
            header = self.frame_cipher.decrypt_header(
                buffer[:Frame.header_size + Frame.mac_size].tobytes())
        else:
            # header: frame-size || header-data || padding
            header = buffer[:Frame.header_size].tobytes()
        return header

    def decode_body(self, buffer, header=None):
        """
        w/o encryption
        peak into buffer for body_size

        return None if buffer is not long enough to decode frame
        """
        assert isinstance(buffer, memoryview)
        if len(buffer) < Frame.header_size:
            return None, buffer

        if not header:
            header = self.decode_header(buffer[:Frame.header_size + Frame.mac_size].tobytes())

        body_size = struct.unpack('>I', b'\x00' + header[:3])[0]

        if self.frame_cipher:
            body = self.frame_cipher.decrypt_body(buffer[Frame.header_size + Frame.mac_size:].tobytes(),
                                                  body_size)
            assert len(body) == body_size
            bytes_read = Frame.header_size + Frame.mac_size + ceil16(len(body)) + Frame.mac_size
        else:
            # header: frame-size || header-data || padding
            header = buffer[:Frame.header_size].tobytes()
            # frame-size: 3-byte integer size of frame, big endian encoded (excludes padding)
            # frame relates to body w/o padding w/o mac
            body_offset = Frame.header_size + Frame.mac_size
            body = buffer[body_offset:body_offset + body_size].tobytes()
            assert len(body) == body_size
            bytes_read = ceil16(body_offset + body_size + Frame.mac_size)
        assert bytes_read % Frame.padding == 0

        # normal, chunked-n: rlp.list(protocol-type[, sequence-id])
        # chunked-0: rlp.list(protocol-type, sequence-id, total-packet-size)
        try:
            header_data = rlp.decode(header[3:], sedes=header_data_sedes, strict=False)
        except rlp.RLPException:
            raise DeserializationError('invalid rlp data')

        if len(header_data) == 3:
            chunked_0 = True
            total_payload_size = header_data[2]
            assert total_payload_size < 2**32
        else:
            chunked_0 = False
            total_payload_size = None

        # protocol-type: < 2**16
        protocol_id = header_data[0]
        assert protocol_id < 2**16
        # sequence-id: < 2**16 (this value is optional for normal frames)
        if len(header_data) > 1:
            sequence_id = header_data[1]
            assert sequence_id < 2**16
        else:
            sequence_id = None

        # build packet
        if protocol_id not in self.chunked_buffers:
            raise MultiplexerError('unknown protocol_id %d' % (protocol_id))
        chunkbuf = self.chunked_buffers[protocol_id]
        if sequence_id in chunkbuf:
            # body chunked-n: packet-data || padding
            packet = chunkbuf[sequence_id]
            if chunked_0:
                raise MultiplexerError('received chunked_0 frame for existing buffer %d of protocol %d' %
                                       (sequence_id, protocol_id))
            if len(body) > packet.total_payload_size - len(packet.payload):
                raise MultiplexerError('too much data for chunked buffer %d of protocol %d' %
                                       (sequence_id, protocol_id))
            # all good
            packet.payload += body
            if packet.total_payload_size == len(packet.payload):
                del packet.total_payload_size
                del chunkbuf[sequence_id]
                return packet
        else:
            # body normal, chunked-0: rlp(packet-type) [|| rlp(packet-data)] || padding
            item, end = rlp.codec.consume_item(body, 0)
            cmd_id = rlp.sedes.big_endian_int.deserialize(item)
            if chunked_0:
                payload = bytearray(body[end:])
                total_payload_size -= end
            else:
                payload = body[end:]

            packet = Packet(protocol_id=protocol_id, cmd_id=cmd_id, payload=payload)
            if chunked_0:
                if total_payload_size < len(payload):
                    raise MultiplexerError('total payload size smaller than initial chunk')
                if total_payload_size == len(payload):
                    return packet # shouldn't have been chunked, whatever
                assert sequence_id is not None
                packet.total_payload_size = total_payload_size
                chunkbuf[sequence_id] = packet
            else:
                return packet # normal (non-chunked)

    def decode(self, data=''):
        if data:
            self._decode_buffer.extend(data)
        if not self._cached_decode_header:
            if len(self._decode_buffer) < Frame.header_size + Frame.mac_size:
                return []
            else:
                self._cached_decode_header = self.decode_header(memoryview(self._decode_buffer))
                assert isinstance(self._cached_decode_header, bytes)

        body_size = struct.unpack('>I', b'\x00' + self._cached_decode_header[:3])[0]
        required_len = Frame.header_size + Frame.mac_size + ceil16(body_size) + Frame.mac_size
        if len(self._decode_buffer) >= required_len:
            packet = self.decode_body(memoryview(self._decode_buffer), self._cached_decode_header)
            self._cached_decode_header = None
            self._decode_buffer = self._decode_buffer[required_len:]
            if packet:
                return [packet] + self.decode()
            else:
                return self.decode()
        return []
