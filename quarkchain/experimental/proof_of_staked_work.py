import random

# Hash power of each miner
# h = [100, 100, 100, 100]
# window_size = 64
alpha = 2
# sp = [1, 2, 4, 13]


h = [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 10, 10, 10, 20, 20]
window_size = 256
sp = h

# Maximum number of blocks produced by each miner in the window
s = [int(alpha * v * window_size / sum(sp)) for v in sp]


def main():
    print("Total hash power: ", sum(h))
    print("Window size: ", window_size)
    print("Max blocks in window: ", s)
    blocks = []
    N = 100000
    blocks_in_window = [0] * len(h)
    ch = [0] * len(h)
    for i in range(N):
        for j in range(len(s)):
            if blocks_in_window[j] >= s[j]:
                ch[j] = 0
            else:
                ch[j] = h[j]
        H = sum(ch)
        c = random.randint(0, H - 1)
        bp = -1
        for j in range(len(h)):
            if ch[j] == 0:
                continue
            if c < ch[j]:
                bp = j
                break
            c -= ch[j]

        blocks.append(bp)
        blocks_in_window[bp] += 1

        if len(blocks) > window_size:
            bp_remove = blocks[len(blocks) - window_size - 1]
            blocks_in_window[bp_remove] -= 1

    bc = [0] * len(h)
    for b in blocks:
        bc[b] += 1

    for i in range(len(bc)):
        print("Miner %d: %.2f%%" % (i, bc[i] / N * 100))


if __name__ == '__main__':
    main()
