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


class Node:
    def __init__(self, node_id, view, is_primary=False):
        self.node_id = node_id
        self.is_primary = is_primary
        # TODO
        self.primary_node_id = 0
        self.view = view
        self.connectionList = []
        self.isCrashing = False

        # TODO
        self.h = 0
        self.H = 10000
        self.seq_num = 0

        self.pre_prepare_msg_map = dict()
        self.prepare_msg_map = dict()
        self.commit_sent_set = set()
        self.commit_msg_map = dict()
        self.committed_set = set()

    def addConnection(self, conn):
        self.connectionList.append(conn)

    def __get_seq_num(self):
        # TODO
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
        for conn in self.connectionList:
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

        prepareMsg = PrepareMsg(
            msg.view, msg.seq_num, msg.digest, self.node_id, self.node_id
        )
        for conn in self.connectionList:
            asyncio.ensure_future(conn.sendPrepareMsgAsync(prepareMsg))

    def __num_2f(self):
        f = (len(self.connectionList) + 1 - 1) // 3
        return 2 * f

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

        self.prepare_msg_map.setdefault(msg.seq_num, set()).add(msg.node_id)

        if (
            len(self.prepare_msg_map[msg.seq_num]) >= self.__num_2f()
            and msg.seq_num not in self.commit_sent_set
        ):
            # Broadcast commit
            self.commit_sent_set.add(msg.seq_num)
            self.commit_msg_map.setdefault(msg.seq_num, set()).add(self.node_id)

            commitMsg = CommitMsg(
                msg.view, msg.seq_num, msg.digest, self.node_id, self.node_id
            )
            print(
                "Node {}: sending commit msg, seq no {}, digest {}".format(
                    self.node_id, msg.seq_num, msg.digest.hex()
                )
            )
            for conn in self.connectionList:
                asyncio.ensure_future(conn.sendCommitMsgAsync(commitMsg))

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
            # May replace with the digest as key
            self.committed_set.add(msg.seq_num)
            print(
                "Node {}: msg with digest {} commited".format(
                    self.node_id, msg.digest.hex()
                )
            )


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

nodeList[0].sendClientRequest(ClientRequest(b""))

try:
    asyncio.get_event_loop().run_forever()
except Exception as e:
    print(e)
