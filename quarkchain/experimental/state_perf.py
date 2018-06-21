import os
import time
from quarkchain.evm.state import State
from quarkchain.db import InMemoryDb, PersistentDb


def timer(func):

    def wrapper(*args):
        start = time.time()
        func(*args)
        end = time.time()
        print("{:.2f}".format(end - start))
    return wrapper


@timer
def commit(state):
    state.commit()


def test(db):
    state = State(db=db)
    for i in range(10000):
        state.delta_balance(os.urandom(20), 12)
    commit(state)

d
def main():
    db = PersistentDb("/tmp/abc")
    test(db)
    test(db)
    test(db)
    test(InMemoryDb())


if __name__ == "__main__":
    main()