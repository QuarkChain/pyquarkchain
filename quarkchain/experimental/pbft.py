import asyncio
import hashlib

from event_driven_simulator import Connection

RPC_TIMEOUT_MS = 100


class ClientRequest:
    def __init__(self, data):
        self.data = data

    def digest(self):
        m = hashlib.sha256()
        m.update(self.data)
        return m.digest()


class PrePrepareMsg:
    def __init__(self, view, seq_num, digest, m, sig):
        self.view = view
        # Seq number
        self.seq_num = seq_num
        # Client request digest
        self.digest = digest
        # Client request (can be carried by different transport)
        self.m = m
        # Siganture
        self.sig = sig


class PrepareMsg:
    def __init__(self, view, seq_num, digest, node_id, sig):
        # View
        self.view = view
        # Seq number
        self.seq_num = seq_num
        # Digest
        self.digest = digest
        # Node id (may not needed as node_id can be identified from sig)
        self.node_id = node_id
        self.sig = sig


class CommitMsg:
    def __init__(self, view, seq_num, digest, node_id, sig):
        self.view = view
        self.seq_num = seq_num
        self.digest = digest
        self.node_id = node_id
        self.sig = sig


class CheckpointMsg:
    def __init__(self, seq_num, state_digest, sign):
        self.seq_num = seq_num
        self.state_digest = state_digest
        self.sign = sign


class Node:
    def __init__(self, node_id, view, is_primary=False):
        self.node_id = node_id
        self.is_primary = is_primary
        # TODO
        self.primary_node_id = 0
        self.view = view
        self.connection_list = []
        self.isCrashing = False
        self.state = b""

        # TODO
        self.h = 0
        self.H = 10000
        self.seq_num = 0

        # Received messages. all should be persisted
        # Could be removed after checkpoint
        self.pre_prepare_msg_map = dict()
        self.prepare_msg_map = dict()
        self.commit_msg_map = dict()
        self.committed_set = set()

    def addConnection(self, conn):
        self.connection_list.append(conn)

    def __get_seq_num(self):
        # TODO: H check
        self.seq_num += 1
        return self.seq_num

    async def start(self):
        while True:
            await asyncio.sleep(1)

    def sendClientRequest(self, m):
        if not self.is_primary:
            return None

        msg = PrePrepareMsg(
            self.view, self.__get_seq_num(), m.digest(), m, self.node_id
        )
        self.pre_prepare_msg_map[msg.seq_num] = msg

        print(
            "Node {}: sending pre-prepare msg, seq no {}, digest {}".format(
                self.node_id, msg.seq_num, msg.digest.hex()
            )
        )
        for conn in self.connection_list:
            asyncio.ensure_future(conn.sendPrePrepareMsgAsync(msg))

    # RPC handling
    def handlePrePrepareMsg(self, msg):
        if self.view != msg.view:
            return

        if self.primary_node_id != msg.sig:
            return

        if msg.seq_num < self.h or msg.seq_num > self.H:
            return

        if msg.seq_num in self.pre_prepare_msg_map:
            return

        print(
            "Node {}: processing pre-prepare msg, seq no {}, digest {}".format(
                self.node_id, msg.seq_num, msg.digest.hex()
            )
        )

        self.pre_prepare_msg_map[msg.seq_num] = msg
        self.prepare_msg_map.setdefault(msg.seq_num, set()).add(self.node_id)

        prepare_msg = PrepareMsg(
            msg.view, msg.seq_num, msg.digest, self.node_id, self.node_id
        )
        for conn in self.connection_list:
            asyncio.ensure_future(conn.sendPrepareMsgAsync(prepare_msg))

    def __num_2f(self):
        f = (len(self.connection_list) + 1 - 1) // 3
        return 2 * f

    def __is_prepared(self, seq_num):
        return len(self.prepare_msg_map.get(seq_num, set())) >= self.__num_2f()

    def handlePrepareMsg(self, msg):
        if self.view != msg.view:
            return

        # TODO: May cache the prepare message until pre_prepare is received.
        if msg.seq_num not in self.pre_prepare_msg_map:
            return

        pre_prepare_msg = self.pre_prepare_msg_map[msg.seq_num]
        if pre_prepare_msg.digest != msg.digest:
            return

        print(
            "Node {}: processing prepare msg from {}, seq no {}, digest {}".format(
                self.node_id, msg.node_id, msg.seq_num, msg.digest.hex()
            )
        )

        is_prepared_before = self.__is_prepared(msg.seq_num)
        self.prepare_msg_map.setdefault(msg.seq_num, set()).add(msg.node_id)

        if not is_prepared_before and self.__is_prepared(msg.seq_num):
            # Broadcast commit
            self.commit_msg_map.setdefault(msg.seq_num, set()).add(self.node_id)

            commit_msg = CommitMsg(
                msg.view, msg.seq_num, msg.digest, self.node_id, self.node_id
            )
            print(
                "Node {}: sending commit msg, seq no {}, digest {}".format(
                    self.node_id, msg.seq_num, msg.digest.hex()
                )
            )
            for conn in self.connection_list:
                asyncio.ensure_future(conn.sendCommitMsgAsync(commit_msg))

    def handleCommitMsg(self, msg):
        if self.view != msg.view:
            return

        if msg.seq_num not in self.pre_prepare_msg_map:
            return

        pre_prepare_msg = self.pre_prepare_msg_map[msg.seq_num]
        if pre_prepare_msg.digest != msg.digest:
            return

        print(
            "Node {}: processing commit msg from {}, seq no {}, digest {}".format(
                self.node_id, msg.node_id, msg.seq_num, msg.digest.hex()
            )
        )

        self.commit_msg_map.setdefault(msg.seq_num, set()).add(msg.node_id)

        if (
            len(self.commit_msg_map[msg.seq_num]) >= self.__num_2f() + 1
            and msg.seq_num not in self.committed_set
        ):
            # TODO: Check the requests with lower sequences are executed (finalized)
            # Message is irreversible/finalized.
            # May discard all logs of the message,
            # but current view-change protocol needs prepare messages.
            # May replace with the digest as key
            self.committed_set.add(msg.seq_num)

            # Simple state execution
            s = hashlib.sha256()
            s.update(self.state)
            s.update(pre_prepare_msg.m.digest())
            self.state = s.digest()
            print(
                "Node {}: msg with digest {} commited, state {}".format(
                    self.node_id, msg.digest.hex(), self.state.hex()
                )
            )

            checkpoint_msg = CheckpointMsg(msg.seq_num, self.state, self.node_id)
            for conn in self.connection_list:
                asyncio.ensure_future(conn.sendCheckpointMsgAsync(checkpoint_msg))

    def handleCheckpointMsg(self, msg):
        pass


class PbftConnection(Connection):
    def __init__(
        self,
        source,
        destination,
        timeoutMs=RPC_TIMEOUT_MS,
        networkDelayGenerator=lambda: 0,
    ):
        super().__init__(source, destination, timeoutMs, networkDelayGenerator)

    async def sendPrePrepareMsgAsync(self, request):
        return await self.callWithDelayOrTimeout(
            lambda: self.destination.handlePrePrepareMsg(request)
        )

    async def sendPrepareMsgAsync(self, request):
        return await self.callWithDelayOrTimeout(
            lambda: self.destination.handlePrepareMsg(request)
        )

    async def sendCommitMsgAsync(self, request):
        return await self.callWithDelayOrTimeout(
            lambda: self.destination.handleCommitMsg(request)
        )

    async def sendCheckpointMsgAsync(self, request):
        return await self.callWithDelayOrTimeout(
            lambda: self.destination.handleCheckpointMsg(request)
        )


N = 4
nodeList = [Node(i, view=0, is_primary=i == 0) for i in range(N)]
connectionMap = {}
for i in range(N):
    for j in range(N):
        if i == j:
            continue
        source = nodeList[i]
        dest = nodeList[j]
        source.addConnection(PbftConnection(source, dest))

for i in range(N):
    asyncio.get_event_loop().create_task(nodeList[i].start())

# nodeList[-1].isCrashing = True
# nodeList[-2].isCrashing = True

nodeList[0].sendClientRequest(ClientRequest(b""))

try:
    asyncio.get_event_loop().run_forever()
except Exception as e:
    print(e)
