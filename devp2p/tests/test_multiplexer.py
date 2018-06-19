from devp2p.multiplexer import Multiplexer, Packet, Frame, DeserializationError


def test_frame():
    mux = Multiplexer()
    p0 = 0
    mux.add_protocol(p0)

    # test normal packet
    packet0 = Packet(p0, cmd_id=0, payload=b'x' * 100)
    mux.add_packet(packet0)
    frames = mux.pop_frames()
    assert len(frames) == 1
    f = frames[0]
    message = f.as_bytes()

    # check framing
    fs = f.frame_size()
    assert len(message) == fs
    _fs = 16 + 16 + len(f.enc_cmd_id) + len(packet0.payload) + 16
    _fs += Frame.padding - _fs % Frame.padding
    assert fs == _fs
    assert message[32 + len(f.enc_cmd_id):].startswith(packet0.payload)

    packets = mux.decode(message)
    assert len(mux._decode_buffer) == 0
    assert len(packets[0].payload) == len(packet0.payload)
    assert packets[0].payload == packet0.payload
    assert packets[0] == packet0


def test_chunked():
    mux = Multiplexer()
    p0, p1, p2 = 0, 1, 2
    mux.add_protocol(p0)
    mux.add_protocol(p1)
    mux.add_protocol(p2)

    # big packet
    print('size', mux.max_window_size * 2)
    packet1 = Packet(p1, cmd_id=0, payload=b'\x00' * mux.max_window_size * 2 + b'x')
    mux.add_packet(packet1)
    frames = mux.pop_all_frames()
    all_frames_length = sum(f.frame_size() for f in frames)
    assert sum(len(f.payload) for f in frames) == len(packet1.payload)
    for i, f, in enumerate(frames):
        print(i, f.frame_size())
        print('frame payload', len(f.payload))
        print(f._frame_type())
    mux.add_packet(packet1)
    message = mux.pop_all_frames_as_bytes()
    assert len(message) == all_frames_length
    packets = mux.decode(message)
    assert len(mux._decode_buffer) == 0
    assert packets[0].payload == packet1.payload
    assert packets[0] == packet1
    assert len(packets) == 1


def test_chunked_big():
    import time
    mux = Multiplexer()
    p0 = 0
    mux.add_protocol(p0)

    # big packet
    payload = b'\x00' * 10 * 1024**2
    print('size', len(payload))
    packet1 = Packet(p0, cmd_id=0, payload=payload)

    # framing
    st = time.time()
    mux.add_packet(packet1)
    print('framing', time.time() - st)

    # popping frames
    st = time.time()
    messages = [f.as_bytes() for f in mux.pop_all_frames()]
    print('popping frames', time.time() - st)

    st = time.time()
    # decoding
    for m in messages:
        packets = mux.decode(m)
        if packets:
            break
    print('decoding frames', time.time() - st)
    assert len(mux._decode_buffer) == 0
    assert packets[0].payload == packet1.payload
    assert packets[0] == packet1
    assert len(packets) == 1


def test_remain():
    mux = Multiplexer()
    p0, p1, p2 = 0, 1, 2
    mux.add_protocol(p0)
    mux.add_protocol(p1)
    mux.add_protocol(p2)

    # test buffer remains, incomplete frames
    packet1 = Packet(p1, cmd_id=0, payload=b'\x00' * 100)
    mux.add_packet(packet1)
    message = mux.pop_all_frames_as_bytes()
    tail = message[:50]
    message += tail
    packets = mux.decode(message)
    assert packets[0] == packet1
    assert len(packets) == 1
    assert len(mux._decode_buffer) == len(tail)

    # test buffer decode with invalid data
    message = message[1:]
    exception_raised = False
    try:
        packets = mux.decode(message)
    except DeserializationError:
        exception_raised = True
    assert exception_raised


def test_multiplexer():
    mux = Multiplexer()
    p0, p1, p2 = 0, 1, 2
    mux.add_protocol(p0)
    mux.add_protocol(p1)
    mux.add_protocol(p2)

    assert mux.next_protocol == p0
    assert mux.next_protocol == p1
    assert mux.next_protocol == p2
    assert mux.next_protocol == p0

    assert mux.pop_frames() == []
    assert mux.num_active_protocols == 0

    # test normal packet
    packet0 = Packet(p0, cmd_id=0, payload=b'x' * 100)

    mux.add_packet(packet0)
    assert mux.num_active_protocols == 1

    frames = mux.pop_frames()
    assert len(frames) == 1
    f = frames[0]
    assert len(f.as_bytes()) == f.frame_size()

    mux.add_packet(packet0)
    assert mux.num_active_protocols == 1
    message = mux.pop_all_frames_as_bytes()
    packets = mux.decode(message)
    assert len(packets[0].payload) == len(packet0.payload)
    assert packets[0].payload == packet0.payload
    assert packets[0] == packet0

    # nothing left to pop
    assert len(mux.pop_frames()) == 0

    # big packet
    packet1 = Packet(p1, cmd_id=0, payload=b'\x00' * mux.max_window_size * 2)
    mux.add_packet(packet1)

    # decode packets from buffer
    message = mux.pop_all_frames_as_bytes()
    packets = mux.decode(message)
    assert packets[0].payload == packet1.payload
    assert packets[0] == packet1
    assert len(packets) == 1

    # mix packet types
    packet2 = Packet(p0, cmd_id=0, payload=b'\x00' * 200, prioritize=True)
    mux.add_packet(packet1)
    mux.add_packet(packet0)
    mux.add_packet(packet2)
    message = mux.pop_all_frames_as_bytes()
    packets = mux.decode(message)
    assert packets == [packet2, packet0, packet1]

    # packets with different protocols
    packet3 = Packet(p1, cmd_id=0, payload=b'\x00' * 3000, prioritize=False)
    mux.add_packet(packet1)
    mux.add_packet(packet0)
    mux.add_packet(packet2)
    mux.add_packet(packet3)
    mux.add_packet(packet3)
    mux.add_packet(packet3)
    assert mux.next_protocol == p0
    # thus next with data is p1 w/ packet3
    message = mux.pop_all_frames_as_bytes()
    packets = mux.decode(message)
    assert packets == [packet3, packet2, packet0, packet3, packet3, packet1]

    # test buffer remains, incomplete frames
    packet1 = Packet(p1, cmd_id=0, payload=b'\x00' * 100)
    mux.add_packet(packet1)
    message = mux.pop_all_frames_as_bytes()
    tail = message[:50]
    message += tail
    packets = mux.decode(message)
    assert packets[0] == packet1
    assert len(packets) == 1
    assert len(mux._decode_buffer) == len(tail)


def test_rlpx_alpha():
    """
    protocol_id: 0
    sequence_id: 0

    Single-frame packet:
    header || header-mac || frame || frame-mac

    header: frame-size || header-data || padding

    frame-size: 3-byte integer size of frame, big endian encoded (excludes padding)

    header-data:
        normal: rlp.list(protocol-type[, sequence-id])

    values:
        protocol-type: < 2**16
        sequence-id: < 2**16 (this value is optional for normal frames)
        total-packet-size: < 2**32

    padding: zero-fill to 16-byte boundary
    """
    pass
