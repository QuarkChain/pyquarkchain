import multiprocessing as mp
import time
import asyncio
from concurrent.futures import ProcessPoolExecutor


def mine(input, output):
    while True:
        try:
            i = input.get_nowait()
            print(i)
            output.put(i + 1)
        except Exception:
            pass


def main():
    mp.set_start_method('spawn')
    input = mp.Queue()
    output = mp.Queue()
    p = mp.Process(target=mine, args=(input, output))
    p.start()
    while True:
        time.sleep(1)
        input.put(1)
        print(output.get())
    p.join()


if __name__ == "__main__":
    main()