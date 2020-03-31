from quarkchain.utils import sha3_256


class Accumulator:
    def __init__(self):
        self.hlist = []

    def append(self, data):
        h = sha3_256(data)
        for i in reversed(range(len(self.hlist))):
            if self.hlist[i] is None:
                self.hlist[i] = h
                return
            else:
                h = sha3_256(self.hlist[i] + h)
                self.hlist[i] = None
        self.hlist.insert(0, h)

    def hash(self):
        d = b""
        for v in self.hlist:
            if v is None:
                d += bytes(32)
            else:
                d += v
        return sha3_256(d)


def calc_list_hash(l):
    hl = [sha3_256(d) for d in l]
    hlist = []
    while len(hl) != 0:
        nhl = []
        for i in range(len(hl) // 2):
            nhl.append(sha3_256(hl[i * 2] + hl[i * 2 + 1]))
        if len(hl) % 2 != 0:
            hlist.append(hl[-1])
        else:
            hlist.append(None)
        hl = nhl

    d = b""
    for v in reversed(hlist):
        if v is None:
            d += bytes(32)
        else:
            d += v
    return sha3_256(d)


def main():
    ll = []
    n = 10
    a = Accumulator()
    assert calc_list_hash(ll) == a.hash()
    for i in range(n):
        ll.append(i.to_bytes(4, byteorder="little"))
        a.append(i.to_bytes(4, byteorder="little"))
        assert calc_list_hash(ll) == a.hash()
    print("passed")


if __name__ == "__main__":
    main()
