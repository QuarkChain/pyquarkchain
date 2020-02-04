import asyncio
import event_driven_simulator
import random
import time


RPC_TIMEOUT_MS = 10000  # 10 seconds
ELECTION_TIMEOUT_MAX_MS = 1000
HEART_BEAT_TIMEOUT_MS = 200


class NodeState:
    LEADER = 0
    FOLLOWER = 1
    CANDIDATE = 2

    @classmethod
    def to_string(cls, state):
        return {
            cls.LEADER: "LEADER",
            cls.FOLLOWER: "FOLLOWER",
            cls.CANDIDATE: "CANDIDATE",
        }[state]


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


class LogEntry:
    def __init__(self, term):
        self.term = term


class Node:
    def __init__(
        self,
        nodeId,
        electionTimeoutMsGenerator=lambda: random.randint(1, ELECTION_TIMEOUT_MAX_MS),
    ):
        self.nodeId = nodeId
        self.currentTerm = 0
        self.voteFor = None
        self.log = []
        self.state = NodeState.CANDIDATE
        self.connectionList = []
        self.electionTimeoutMsGenerator = electionTimeoutMsGenerator
        self.heartBeatTimetoutMs = HEART_BEAT_TIMEOUT_MS
        self.lastHeartBeatReceived = None

    def addConnection(self, conn):
        self.connectionList.append(conn)

    def crash():
        pass

    def handleAppendEntriesRequest(self, request):
        pass

    def hasAtLeastUpdatedLog(self, lastLogTerm, lastLogIndex):
        if len(self.log) == 0:
            return True

        if self.log[-1].term > lastLogTerm:
            return False

        if self.log[-1].term < lastLogTerm:
            return True

        if len(self.log) <= lastLogIndex:
            return True

        return False

    def handleRequestVoteRequest(self, request):
        if request.term < self.currentTerm:
            return RequestVoteResponse(self.currentTerm, False)

        if request.term > self.currentTerm:
            self.state = NodeState.FOLLOWER
            self.voteFor = None
            self.currentTerm = request.term

        if (
            self.voteFor is None or self.voteFor == request.candidateId
        ) and self.hasAtLeastUpdatedLog(request.lastLogTerm, request.lastLogIndex):
            self.voteFor = request.candidateId
            return RequestVoteResponse(self.currentTerm, True)
        return RequestVoteResponse(self.currentTerm, False)

    async def start(self):
        print("Node {}: Starting".format(self.nodeId))
        while True:
            print(
                "Node {}: State {}".format(self.nodeId, NodeState.to_string(self.state))
            )
            if self.state == NodeState.CANDIDATE:
                await self.electForLeader()
            elif self.state == NodeState.LEADER:
                await self.proposeLog()
            elif self.state == NodeState.FOLLOWER:
                await self.waitForHeartBeatTimeout()
                # HB timed out.  Elect for leader.
                self.state = NodeState.CANDIDATE
            else:
                assert false

    def __majority(self):
        return (1 + len(self.connectionList) + 2) // 2

    async def electForLeader(self):
        assert self.state == NodeState.CANDIDATE
        self.currentTerm += 1
        self.voteFor = self.nodeId
        print(
            "Node {}: Electing for leader for term {}".format(
                self.nodeId, self.currentTerm
            )
        )

        timeoutMs = self.electionTimeoutMsGenerator()

        print("Election timeout", timeoutMs)
        try:
            await asyncio.wait_for(self.collectVotes(), timeout=timeoutMs / 1000)
        except asyncio.TimeoutError:
            pass

        if self.state == NodeState.LEADER:
            print(
                "Node {}: Elected as leader for term {}".format(
                    self.nodeId, self.currentTerm
                )
            )

    async def collectVotes(self):
        # Self vote
        votes = 1

        # Perform RPCs and collect returns
        request = RequestVoteRequest(
            self.currentTerm,
            self.nodeId,
            len(self.log),
            0 if len(self.log) == 0 else log[-1].term,
        )
        pending = [conn.requestVoteAsync(request) for conn in self.connectionList]

        while len(pending) != 0:
            try:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
            except asyncio.TimeoutError:
                return False
            # The node may move to follower state, stop election immediately.
            if self.state != NodeState.CANDIDATE:
                break
            for d in done:
                if d.exception() is None and d.result().voteGranted:
                    votes += 1
                if votes >= self.__majority():
                    self.state = NodeState.LEADER
                    return True

        # Even we know that the node is not leader,
        # we will wait until timeout and move to next election
        while True:
            await asyncio.sleep(1)
        return False

    async def proposeLog(self):
        assert self.state == NodeState.LEADER
        while self.state == NodeState.LEADER:
            print("Node {}: Proposing log".format(self.nodeId))
            await asyncio.sleep(1)

    async def waitForHeartBeatTimeout(self):
        while True:
            now = time.monotonic()
            if self.lastHeartBeatReceived is None:
                self.lastHeartBeatReceived = now
            if now >= self.lastHeartBeatReceived + self.heartBeatTimetoutMs / 1000:
                return
            await asyncio.sleep(
                self.lastHeartBeatReceived + self.heartBeatTimetoutMs / 1000 - now
            )


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
            asyncio.get_event_loop().call_later(latencyMs0 / 1000, callFunc)

            await asyncio.sleep(self.timeoutMs / 1000)
            raise TimeoutError()

        latencyMs1 = self.networkDelayGenerator()
        await asyncio.sleep(latencyMs0 / 1000)
        resp = callFunc()

        if latencyMs0 + latencyMs1 >= self.timeoutMs:
            await asyncio.sleep((self.timeoutMs - latencyMs0) / 1000)
            raise TimeoutError()

        await asyncio.sleep(latencyMs1 / 1000)
        return resp

    async def appendEntriesAsync(self, request):
        return await self.callWithDelayOrTimeout(
            lambda: self.destination.handleAppendEntriesRequest(request)
        )

    async def requestVoteAsync(self, request):
        return await self.callWithDelayOrTimeout(
            lambda: self.destination.handleRequestVoteRequest(request)
        )


N = 3
nodeList = [Node(i) for i in range(N)]
connectionMap = {}
for i in range(N):
    for j in range(N):
        if i == j:
            continue
        source = nodeList[i]
        dest = nodeList[j]
        source.addConnection(Connection(source, dest))

for i in range(N):
    asyncio.get_event_loop().create_task(nodeList[i].start())


try:
    asyncio.get_event_loop().run_forever()
except Exception as e:
    print(e)
