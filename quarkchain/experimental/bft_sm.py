# Implement SM(m) algorithm in L. Lamport, R. Shostak, and M. Pease, 1982, "The Byzantine Generals Problem"
# Some notes:
# - Order is an integer
# - Commander is always traitor as loyal command case is trival

import collections
import random


def choice(orders):
    return min(orders)


class Message:
    def __init__(self, order, sigs):
        self.order = order
        self.sigs = sigs

    def signMessage(self, nodeId):
        newSigs = self.sigs[:]
        newSigs.append(nodeId)
        return Message(self.order, newSigs)

    def isAllTraitors(self, M):
        for sig in self.sigs:
            if sig >= M:
                return False
        return True

    def toString(self):
        s = str(self.order)
        for sig in self.sigs:
            s = "{}:{}".format(s, sig)
        return s


# # of nodes
N = 5

# # of traitors
M = 3

# SM(m)
m = 2

trials = 10

for trial in range(trials):
    orders_set = [set() for i in range(N)]

    order = random.randint(0, 10000)

    message_queue = collections.deque()
    message_queue.append((Message(order, []), 0))

    while len(message_queue) != 0:
        msg, nodeId = message_queue.popleft()
        print("Node {}: Receive {}".format(nodeId, msg.toString()))
        if nodeId < M:
            # Traitor
            if len(msg.sigs) == m + 1:
                continue

            canForge = msg.isAllTraitors(M)
            for i in range(1, N):
                if i in msg.sigs or i == nodeId:
                    continue
                newMsg = msg.signMessage(nodeId)
                if canForge:
                    newMsg.order = random.randint(0, 10000)
                message_queue.append((newMsg, i))

        else:
            # Loyal

            # Order exist, do nothing.
            if msg.order in orders_set[nodeId]:
                continue

            orders_set[nodeId].add(msg.order)

            if len(msg.sigs) == m + 1:
                continue

            for i in range(1, N):
                if i in msg.sigs or i == nodeId:
                    continue
                message_queue.append((msg.signMessage(nodeId), i))

    c = None
    for i in range(M, N):
        if c is None:
            c = choice(orders_set[i])
        assert c == choice(orders_set[i])
    print("All choices are consistent")
