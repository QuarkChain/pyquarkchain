import asyncio
import event_driven_simulator


RPC_TIMEOUT_MS = 10000  # 10 seconds


class NodeState:
    LEADER = 0
    FOLLOWER = 1
    CANDIDATE = 2


class RequestVoteRequest:
    def __init__(self, term, candidateId, lastLogIndex, lastLogTerm):
        self.term = term
        self.candidateId = candidateId
        self.lastLogIndex = lastLogIndex
        self.lastLogTerm = lastLogTerm


class RequestVoteResponse:
    def __init__(self, term, voteGranted):
        self.term = term
        self.voteGranted = voteGranted


class Node:
    def __init__(self, nodeId):
        self.nodeId = nodeId
        self.currentTerm = 0
        self.voteFor = None
        self.log = []
        self.state = NodeState.CANDIDATE
        self.connectionList = []

    def addConnection(self, conn):
        self.connectionList.append(conn)

    def crash():
        pass

    def handleAppendEntriesRequest(self, request):
        pass

    def handleRequestVoteRequest(self, request):
        # if request.term < self.currentTerm:
        #     return RequestVoteResponse(self.currentTerm, False)

        # if self.voteFor is None or
        pass

    async def __broadcastAppendEntriesRequest(self):
        request = None
        for conn in connectionList:
            await conn.appendEntriesAsync(request)

    async def start(self):
        # self.electForLeader()
        pass


class Connection:
    def __init__(
        self,
        source,
        destination,
        timeoutMs=RPC_TIMEOUT_MS,
        networkDelayGenerator=lambda: 0,
    ):
        self.source = source
        self.destination = destination
        self.timeoutMs = timeoutMs
        self.networkDelayGenerator = networkDelayGenerator

    async def callWithDelayOrTimeout(self, callFunc):
        """ Simulate a RPC with network delay (round trip).
        Raise TimeoutError if the round-trip delay is greater than timeout
        """
        latencyMs0 = self.networkDelayGenerator()

        if latencyMs0 >= self.timeoutMs:
            # We don't cancel the RPC, while the response will be discarded
            asyncio.create_task(
                await asyncio.sleep(self.latencyMs0 / 1000), callFunc(request)
            )

            await asyncio.sleep(self.timeoutMs / 1000)
            raise TimeoutError()

        await asyncio.sleep(self.timeoutMs / 1000)
        resp = callFunc(request)

        latencyMs1 = self.networkDelayGenerator()
        if latencyMs0 + latencyMs1 >= self.timeoutMs:
            await asyncio.sleep((self.timeoutMs - latencyMs0) / 1000)
            raise TimeoutError()

        return resp

    async def appendEntriesAsync(self, request):
        return await self.callWithDelayOrTimeout(
            lambda: destination.handleAppendEntriesRequest(request)
        )

    async def requestVoteAsync(self, request):
        return await self.callWithDelayOrTimeout(
            lambda: destination.handleAppendEntriesRequest(request)
        )


N = 5
nodeList = [Node(i) for i in range(N)]
connectionMap = {}
for i in range(N):
    for j in range(N):
        if i == j:
            continue
        source = nodeList[i]
        dest = nodeList[j]
        source.addConnection(Connection(source, dest))

try:
    asyncio.get_event_loop().run_forever()
except Exception as e:
    print(e)
