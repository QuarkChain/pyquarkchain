import asyncio
import event_driven_simulator
import random
import time


RPC_TIMEOUT_MS = 10000  # 10 seconds
ELECTION_TIMEOUT_MAX_MS = 1000
HEART_BEAT_TIMEOUT_MS = 4000
HEART_BEAT_INTERVAL_MS = 1000


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


class AppendEntriesRequest:
    def __init__(
        self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit
    ):
        self.term = term
        self.leaderId = leaderId
        self.prevLogIndex = prevLogIndex
        self.prevLogTerm = prevLogTerm
        self.entries = entries
        self.leaderCommit = leaderCommit


class AppendEntriesResponse:
    def __init__(self, term, success):
        self.term = term
        self.success = success


class LogEntry:
    def __init__(self, term):
        self.term = term

    @staticmethod
    def create_genesis_log():
        return LogEntry(0)


class Node:
    def __init__(
        self,
        nodeId,
        electionTimeoutMsGenerator=lambda: random.randint(1, ELECTION_TIMEOUT_MAX_MS),
    ):
        # Constant
        self.electionTimeoutMsGenerator = electionTimeoutMsGenerator
        self.heartBeatTimetoutMs = HEART_BEAT_TIMEOUT_MS

        # Persisted data
        self.nodeId = nodeId
        self.currentTerm = 0
        self.voteFor = None
        self.log = [LogEntry.create_genesis_log()]

        # The simulation assumes the connections are constant (no membership feature)
        self.connectionList = []

        # Volatile (reinitialized after restart)
        self.initializeVolatileVariables()

    def initializeVolatileVariables(self):
        # Raft-specific variables
        self.state = NodeState.FOLLOWER
        self.lastHeartBeatReceived = None
        self.commitIndex = 0
        self.lastApplied = 0
        self.nextIndexMap = {}
        self.matchIndexMap = {}

        # System-level variables
        self.currentTask = None
        self.isCrashing = False
        self.crashedFuture = None

    def addConnection(self, conn):
        self.connectionList.append(conn)

    def crash(self):
        self.isCrashing = True
        self.crashedFuture = asyncio.get_event_loop().create_future()
        # if self.currentTask is not None:
        self.currentTask.cancel()
        return self.crashedFuture

    def changeStateAndCancelCurrenTask(self, newState):
        assert self.state != newState
        self.state = newState
        if self.currentTask is not None:
            self.currentTask.cancel()
            self.currentTask = None

    def handleAppendEntriesRequest(self, request):
        if request.term < self.currentTerm:
            return AppendEntriesResponse(self.currentTerm, False)

        # Any append entries are treated as HB.
        self.lastHeartBeatReceived = time.monotonic()

        # Detect if there is a state change
        if self.state == NodeState.CANDIDATE:
            if self.currentTerm <= request.term:
                # Find a concurrent leader with majority votes, switch to follower immediately
                self.changeStateAndCancelCurrenTask(NodeState.FOLLOWER)
        elif self.state == NodeState.LEADER:
            # We will never enter a network state that two nodes are leaders of the same term
            assert self.currentTerm < request.term
            self.changeStateAndCancelCurrenTask(NodeState.FOLLOWER)
        else:
            assert self.state == NodeState.FOLLOWER
        self.currentTerm = max(request.term, self.currentTerm)

        # Reorg detected.  Asked for previous logs to find the ancestor.
        if (
            len(self.log) < request.prevLogIndex
            or self.log[request.prevLogIndex].term != request.prevLogTerm
        ):
            return AppendEntriesResponse(self.currentTerm, False)

        # Append entries
        for i in range(len(request.entries)):
            appendIdx = i + request.prevLogIndex + 1
            if appendIdx >= len(self.log):
                self.log.append(request.entries)
            elif request.entries[i].term != self.log[appendIdx].term:
                # Reorg detected.  Rewrite the terms.
                # The reorg should not touch commited logs
                assert self.appendIdex < self.commitIndex
                del self.log[appendIdx:-1]
                self.log.append(request.entries)

        # It is possible that leaderCommit < commitIndex?
        if request.leaderCommit > self.commitIndex:
            self.commitIndex = min(
                request.leaderCommit, len(request.entries) + request.prevLogIndex
            )

        if self.commitIndex > self.lastApplied:
            # TODO: Apply logs to state machine
            self.lastApplied = self.commitIndex

        return AppendEntriesResponse(self.currentTerm, True)

    def hasAtLeastUpdatedLog(self, lastLogTerm, lastLogIndex):
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
        self.initializeVolatileVariables()

        print(
            "Node {}: Starting as {}".format(
                self.nodeId, NodeState.to_string(self.state)
            )
        )
        prevState = self.state
        while not self.isCrashing:
            if prevState != self.state:
                print(
                    "Node {}: State {} => {}".format(
                        self.nodeId,
                        NodeState.to_string(prevState),
                        NodeState.to_string(self.state),
                    )
                )
                prevState = self.state
            if self.state == NodeState.CANDIDATE:
                self.currentTask = asyncio.get_event_loop().create_task(
                    self.electForLeader()
                )
                await self.currentTask
            elif self.state == NodeState.LEADER:
                self.currentTask = asyncio.get_event_loop().create_task(
                    self.proposeLogAndHeartBeat()
                )
                await self.currentTask
            elif self.state == NodeState.FOLLOWER:
                self.currentTask = asyncio.get_event_loop().create_task(
                    self.waitForHeartBeatTimeout()
                )
                await self.currentTask
                # HB timed out.  Elect for leader.
                self.state = NodeState.CANDIDATE
            else:
                assert false
        if self.crashedFuture is not None:
            self.crashedFuture.set_result(None)

    def __majority(self):
        return (1 + len(self.connectionList) + 2) // 2

    async def electForLeader(self):
        if self.state != NodeState.CANDIDATE:
            return

        self.currentTerm += 1
        self.voteFor = self.nodeId
        timeoutMs = self.electionTimeoutMsGenerator()
        print(
            "Node {}: Electing for leader for term {} with timeout {}".format(
                self.nodeId, self.currentTerm, timeoutMs
            )
        )

        try:
            await asyncio.wait_for(self.collectVotes(), timeout=timeoutMs / 1000)
        except asyncio.TimeoutError:
            # Election timeout.  Will try another election if no leader is found.
            pass
        except asyncio.CancelledError:
            # The election is canceled because a leader is found
            pass

        if self.state == NodeState.LEADER:
            print(
                "Node {}: Elected as leader for term {}".format(
                    self.nodeId, self.currentTerm
                )
            )

    async def collectVotes(self):
        if self.state != NodeState.CANDIDATE:
            return

        # Self vote
        votes = 1

        # Perform RPCs and collect returns
        request = RequestVoteRequest(
            self.currentTerm, self.nodeId, len(self.log), self.log[-1].term
        )
        pending = [conn.requestVoteAsync(request) for conn in self.connectionList]

        while len(pending) != 0:
            try:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
            except asyncio.TimeoutError:
                # RPC timeout
                continue
            except asyncio.CancelledError:
                # The vote collection of the election is canceled.
                # This can be caused either by election timeout or a leader is found.
                for p in pending:
                    p.cancel()
                return

            # The node may move to follower/leader state, stop election immediately.
            if self.state != NodeState.CANDIDATE:
                break
            for d in done:
                if d.exception() is None and d.result().voteGranted:
                    votes += 1
                if votes >= self.__majority():
                    self.state = NodeState.LEADER
                    return True

        # Even we know that the node is not leader,
        # we will wait until timeout and move to next election (randomized restart)
        while self.state == NodeState.CANDIDATE:
            await asyncio.sleep(1)

    async def proposeLogAndHeartBeat(self):
        # Initialize leader variables
        self.nextLogIndex = {
            conn.getDestinationId(): len(self.log) for conn in self.connectionList
        }
        self.matchIndexMap = {
            conn.getDestinationId(): 0 for conn in self.connectionList
        }
        while self.state == NodeState.LEADER:
            print("Node {}: Send heart beat".format(self.nodeId))
            pending = []
            pendingMap = {}
            entries = []
            for conn in self.connectionList:
                req = AppendEntriesRequest(
                    term=self.currentTerm,
                    leaderId=self.nodeId,
                    prevLogIndex=self.nextLogIndex[conn.getDestinationId()] - 1,
                    prevLogTerm=self.log[
                        self.nextLogIndex[conn.getDestinationId()] - 1
                    ].term,
                    entries=entries,
                    leaderCommit=self.commitIndex,
                )
                p = asyncio.get_event_loop().create_task(conn.appendEntriesAsync(req))
                pending = [p]
                pendingMap[p] = conn.getDestinationId()

            while len(pending) != 0:
                try:
                    done, pending = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED
                    )
                except asyncio.TimeoutError:
                    # RPC timeout
                    continue
                except asyncio.CancelledError:
                    for p in pending:
                        p.cancel()
                    return

                # The node may move to follower state, stop immediately.
                if self.state != NodeState.LEADER:
                    return

                for d in done:
                    if d.exception() is not None:
                        continue
                    if not d.result().success and d.result().term > self.currentTerm:
                        # The follower has a leader with greater term.
                        # TODO: Move to FOLLOWER?
                        continue
                    if not d.result().success:
                        # TODO: Need to actively retry the RPC to find the ancestor
                        # Right now, rely on heart beat
                        self.nextLogIndex[pendingMap[d]] -= 1
                    else:
                        self.nextLogIndex[pendingMap[d]] += len(entries)
                        self.matchIndexMap[pendingMap[d]] = self.nextLogIndex[
                            pendingMap[d]
                        ]

            # Determine the commitIndex that majority have
            replicatedIndexList = [v for v in self.matchIndexMap.values()]
            replicatedIndexList.sort()
            newCommitIndex = replicatedIndexList[self.__majority() - 1]
            assert newCommitIndex >= self.commitIndex
            if newCommitIndex > self.commitIndex:
                self.commitIndex = newCommitIndex
                # Apply the log to state machine
                self.lastApplied = self.commitIndex

            try:
                await asyncio.sleep(HEART_BEAT_INTERVAL_MS / 1000)
            except asyncio.CancelledError:
                return

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
        if self.destination.isCrashing:
            await asyncio.sleep((self.timeoutMs - latencyMs0) / 1000)
            raise TimeoutError()

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

    def getDestinationId(self):
        return self.destination.nodeId


async def random_crash(nodeList):
    while True:
        await asyncio.sleep(8)
        nodeToCrash = None
        for node in nodeList:
            if node.state == NodeState.LEADER:
                nodeToCrash = node
                break

        if nodeToCrash is None:
            continue

        print("Node {}: Crashing".format(nodeToCrash.nodeId))
        await nodeToCrash.crash()
        print("Node {}: Crashed".format(nodeToCrash.nodeId))
        await asyncio.sleep(2)
        asyncio.get_event_loop().create_task(nodeToCrash.start())


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

asyncio.get_event_loop().create_task(random_crash(nodeList))


try:
    asyncio.get_event_loop().run_forever()
except Exception as e:
    print(e)
