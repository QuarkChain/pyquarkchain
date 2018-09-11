import asyncio
from enum import Enum

from quarkchain.core import Serializable
from quarkchain.utils import Logger

ROOT_SHARD_ID = 0


class ConnectionState(Enum):
    CONNECTING = 0  # connecting before the Connection can be used
    ACTIVE = 1  # the peer is active
    CLOSED = 2  # the peer connection is closed


class Metadata(Serializable):
    """ Metadata contains the extra info that needs to be encoded in the RPC layer"""

    FIELDS = []

    def __init__(self):
        pass

    @staticmethod
    def get_byte_size():
        """ Returns the size (in bytes) of the serialized object """
        return 0


class AbstractConnection:
    conn_id = 0
    aborted_rpc_count = 0

    @classmethod
    def __get_next_connection_id(cls):
        cls.conn_id += 1
        return cls.conn_id

    def __init__(
        self,
        op_ser_map,
        op_non_rpc_map,
        op_rpc_map,
        loop=None,
        metadata_class=Metadata,
        name=None,
    ):
        self.op_ser_map = op_ser_map
        self.op_non_rpc_map = op_non_rpc_map
        self.op_rpc_map = op_rpc_map
        self.state = ConnectionState.CONNECTING
        # Most recently received rpc id
        self.peer_rpc_id = -1
        self.rpc_id = 0  # 0 is for non-rpc (fire-and-forget)
        self.rpc_future_map = dict()
        loop = loop if loop else asyncio.get_event_loop()
        self.active_future = loop.create_future()
        self.close_future = loop.create_future()
        self.metadata_class = metadata_class
        if name is None:
            name = "conn_{}".format(self.__get_next_connection_id())
        self.name = name if name else "[connection name missing]"

    async def read_metadata_and_raw_data(self):
        raise NotImplementedError()

    def write_raw_data(self, metadata, raw_data):
        raise NotImplementedError()

    def __parse_command(self, raw_data):
        op = raw_data[0]
        rpc_id = int.from_bytes(raw_data[1:9], byteorder="big")
        ser = self.op_ser_map[op]
        cmd = ser.deserialize(raw_data[9:])
        return op, cmd, rpc_id

    async def read_command(self):
        # TODO: distinguish clean disconnect or unexpected disconnect
        try:
            metadata, raw_data = await self.read_metadata_and_raw_data()
            if metadata is None:
                return (None, None, None)
        except Exception as e:
            self.close_with_error("Error reading command: {}".format(e))
            return (None, None, None)
        op, cmd, rpc_id = self.__parse_command(raw_data)

        # we don't return the metadata to not break the existing code
        return (op, cmd, rpc_id)

    def write_raw_command(self, op, cmd_data, rpc_id=0, metadata=None):
        metadata = metadata if metadata else self.metadata_class()
        ba = bytearray()
        ba.append(op)
        ba.extend(rpc_id.to_bytes(8, byteorder="big"))
        ba.extend(cmd_data)
        self.write_raw_data(metadata, ba)

    def write_command(self, op, cmd, rpc_id=0, metadata=None):
        data = cmd.serialize()
        self.write_raw_command(op, data, rpc_id, metadata)

    def write_rpc_request(self, op, cmd, metadata=None):
        rpc_future = asyncio.Future()

        if self.state != ConnectionState.ACTIVE:
            rpc_future.set_exception(RuntimeError("Peer connection is not active"))
            return rpc_future

        self.rpc_id += 1
        rpc_id = self.rpc_id
        self.rpc_future_map[rpc_id] = rpc_future

        self.write_command(op, cmd, rpc_id, metadata)
        return rpc_future

    def __write_rpc_response(self, op, cmd, rpc_id, metadata):
        self.write_command(op, cmd, rpc_id, metadata)

    async def __handle_request(self, op, request):
        handler = self.op_non_rpc_map[op]
        # TODO: remove rpcid from handler signature
        await handler(self, op, request, 0)

    async def __handle_rpc_request(self, op, request, rpc_id, metadata):
        resp_op, handler = self.op_rpc_map[op]
        resp = await handler(self, request)
        self.__write_rpc_response(resp_op, resp, rpc_id, metadata)

    def validate_and_update_peer_rpc_id(self, metadata, rpc_id):
        if rpc_id <= self.peer_rpc_id:
            raise RuntimeError("incorrect rpc request id sequence")
        self.peer_rpc_id = rpc_id

    async def handle_metadata_and_raw_data(self, metadata, raw_data):
        """ Subclass can override this to provide customized handler """
        op, cmd, rpc_id = self.__parse_command(raw_data)

        if op not in self.op_ser_map:
            raise RuntimeError("{}: unsupported op {}".format(self.name, op))

        if op in self.op_non_rpc_map:
            if rpc_id != 0:
                raise RuntimeError(
                    "{}: non-rpc command's id must be zero".format(self.name)
                )
            await self.__handle_request(op, cmd)
        elif op in self.op_rpc_map:
            # Check if it is a valid RPC request
            self.validate_and_update_peer_rpc_id(metadata, rpc_id)

            await self.__handle_rpc_request(op, cmd, rpc_id, metadata)
        else:
            # Check if it is a valid RPC response
            if rpc_id not in self.rpc_future_map:
                raise RuntimeError(
                    "{}: unexpected rpc response {}".format(self.name, rpc_id)
                )
            future = self.rpc_future_map[rpc_id]
            del self.rpc_future_map[rpc_id]
            future.set_result((op, cmd, rpc_id))

    async def __internal_handle_metadata_and_raw_data(self, metadata, raw_data):
        try:
            await self.handle_metadata_and_raw_data(metadata, raw_data)
        except Exception as e:
            Logger.log_exception()
            self.close_with_error(
                "{}: error processing request: {}".format(self.name, e)
            )

    async def loop_once(self):
        try:
            metadata, raw_data = await self.read_metadata_and_raw_data()
            if metadata is None:
                # Hit EOF
                self.close()
                return
        except Exception as e:
            Logger.log_exception()
            self.close_with_error("{}: error reading request: {}".format(self.name, e))
            return

        asyncio.ensure_future(
            self.__internal_handle_metadata_and_raw_data(metadata, raw_data)
        )

    async def active_and_loop_forever(self):
        if self.state == ConnectionState.CONNECTING:
            self.state = ConnectionState.ACTIVE
            self.active_future.set_result(None)
        while self.state == ConnectionState.ACTIVE:
            await self.loop_once()

        assert self.state == ConnectionState.CLOSED

        # Abort all in-flight RPCs
        for rpc_id, future in self.rpc_future_map.items():
            future.set_exception(RuntimeError("{}: connection abort".format(self.name)))
        AbstractConnection.aborted_rpc_count += len(self.rpc_future_map)
        self.rpc_future_map.clear()

    async def wait_until_active(self):
        await self.active_future

    async def wait_until_closed(self):
        await self.close_future

    def close(self):
        if self.state != ConnectionState.CLOSED:
            self.state = ConnectionState.CLOSED
            self.close_future.set_result(None)

    def close_with_error(self, error):
        self.close()
        return error

    def is_active(self):
        return self.state == ConnectionState.ACTIVE

    def is_closed(self):
        return self.state == ConnectionState.CLOSED


class Connection(AbstractConnection):
    """ A TCP/IP connection based on socket stream
    """

    def __init__(
        self,
        env,
        reader,
        writer,
        op_ser_map,
        op_non_rpc_map,
        op_rpc_map,
        loop=None,
        metadata_class=Metadata,
        name=None,
    ):
        loop = loop if loop else asyncio.get_event_loop()
        super().__init__(
            op_ser_map, op_non_rpc_map, op_rpc_map, loop, metadata_class, name=name
        )
        self.env = env
        self.reader = reader
        self.writer = writer

    async def __read_fully(self, n, allow_eof=False):
        ba = bytearray()
        bs = await self.reader.read(n)
        if allow_eof and len(bs) == 0 and self.reader.at_eof():
            return None

        ba.extend(bs)
        while len(ba) < n:
            bs = await self.reader.read(n - len(ba))
            if len(bs) == 0 and self.reader.at_eof():
                raise RuntimeError("{}: read unexpected EOF".format(self.name))
            ba.extend(bs)
        return ba

    async def read_metadata_and_raw_data(self):
        """ Override AbstractConnection.read_metadata_and_raw_data()
        """
        size_bytes = await self.__read_fully(4, allow_eof=True)
        if size_bytes is None:
            self.close()
            return None, None
        size = int.from_bytes(size_bytes, byteorder="big")

        if size > self.env.quark_chain_config.P2P_COMMAND_SIZE_LIMIT:
            raise RuntimeError("{}: command package exceed limit".format(self.name))

        metadata_bytes = await self.__read_fully(self.metadata_class.get_byte_size())
        metadata = self.metadata_class.deserialize(metadata_bytes)

        raw_data_without_size = await self.__read_fully(1 + 8 + size)
        return metadata, raw_data_without_size

    def write_raw_data(self, metadata, raw_data):
        """ Override AbstractConnection.write_raw_data()
        """
        cmd_length_bytes = (len(raw_data) - 8 - 1).to_bytes(4, byteorder="big")
        self.writer.write(cmd_length_bytes)
        self.writer.write(metadata.serialize())
        self.writer.write(raw_data)

    def close(self):
        """ Override AbstractConnection.close()
        """
        self.writer.close()
        super().close()
