# Initial state
s = [
    0x6A09E667,
    0xBB67AE85,
    0x3C6EF372,
    0xA54FF53A,
    0x510E527F,
    0x9B05688C,
    0x1F83D9AB,
    0x5BE0CD19,
]

k = [
    0x428A2F98,
    0x71374491,
    0xB5C0FBCF,
    0xE9B5DBA5,
    0x3956C25B,
    0x59F111F1,
    0x923F82A4,
    0xAB1C5ED5,
    0xD807AA98,
    0x12835B01,
    0x243185BE,
    0x550C7DC3,
    0x72BE5D74,
    0x80DEB1FE,
    0x9BDC06A7,
    0xC19BF174,
    0xE49B69C1,
    0xEFBE4786,
    0x0FC19DC6,
    0x240CA1CC,
    0x2DE92C6F,
    0x4A7484AA,
    0x5CB0A9DC,
    0x76F988DA,
    0x983E5152,
    0xA831C66D,
    0xB00327C8,
    0xBF597FC7,
    0xC6E00BF3,
    0xD5A79147,
    0x06CA6351,
    0x14292967,
    0x27B70A85,
    0x2E1B2138,
    0x4D2C6DFC,
    0x53380D13,
    0x650A7354,
    0x766A0ABB,
    0x81C2C92E,
    0x92722C85,
    0xA2BFE8A1,
    0xA81A664B,
    0xC24B8B70,
    0xC76C51A3,
    0xD192E819,
    0xD6990624,
    0xF40E3585,
    0x106AA070,
    0x19A4C116,
    0x1E376C08,
    0x2748774C,
    0x34B0BCB5,
    0x391C0CB3,
    0x4ED8AA4A,
    0x5B9CCA4F,
    0x682E6FF3,
    0x748F82EE,
    0x78A5636F,
    0x84C87814,
    0x8CC70208,
    0x90BEFFFA,
    0xA4506CEB,
    0xBEF9A3F7,
    0xC67178F2,
]
k = [0] * len(k)

UINT32_LIMIT = 2 ** 32
UINT32_MAX = UINT32_LIMIT - 1


def u32_not(v):
    return UINT32_MAX ^ v


def right_rotate(v, n):
    for i in range(n):
        bit = v % 2
        v = v >> 1
        v += bit << 31
    return v


def sha2_256_block(s, block):
    assert len(block) == 512 / 8
    w = []
    for i in range(64):
        if i < 16:
            w.append(int.from_bytes(block[i * 4 : (i + 1) * 4], byteorder="big"))
        else:
            s0 = (
                right_rotate(w[i - 15], 7)
                ^ right_rotate(w[i - 15], 18)
                ^ (w[i - 15] >> 3)
            )
            s1 = (
                right_rotate(w[i - 2], 17)
                ^ right_rotate(w[i - 2], 19)
                ^ (w[i - 2] >> 10)
            )
            w.append((w[i - 16] + s0 + w[i - 7] + s1) & UINT32_MAX)

    a = s[0]
    b = s[1]
    c = s[2]
    d = s[3]
    e = s[4]
    f = s[5]
    g = s[6]
    h = s[7]

    for i in range(64):
        S1 = right_rotate(e, 6) ^ right_rotate(e, 11) ^ right_rotate(e, 25)
        ch = (e & f) ^ (u32_not(e) & g)
        temp1 = (h + S1 + ch + k[i] + w[i]) & UINT32_MAX
        S0 = right_rotate(a, 2) ^ right_rotate(a, 13) ^ right_rotate(a, 22)
        maj = (a & b) ^ (a & c) ^ (b & c)
        temp2 = (S0 + maj) & UINT32_MAX
        print(S1, ch, temp1, S0, maj)

        h = g
        g = f
        f = e
        e = (d + temp1) & UINT32_MAX
        d = c
        c = b
        b = a
        a = (temp1 + temp2) & UINT32_MAX
        print(state_to_digest([a, b, c, d, e, f, g, h]).hex())

    s[0] = (s[0] + a) & UINT32_MAX
    s[1] = (s[1] + b) & UINT32_MAX
    s[2] = (s[2] + c) & UINT32_MAX
    s[3] = (s[3] + d) & UINT32_MAX
    s[4] = (s[4] + e) & UINT32_MAX
    s[5] = (s[5] + f) & UINT32_MAX
    s[6] = (s[6] + g) & UINT32_MAX
    s[7] = (s[7] + h) & UINT32_MAX
    return s


def state_to_digest(s):
    digest = b""
    for v in s:
        digest += v.to_bytes(4, byteorder="big")
    return digest


# print(state_to_digest(sha2_256_block(s, block=b"\x80" + b"\x00" * 63)).hex())
# print(state_to_digest(sha2_256_block([0] * 8, block=b"\x00" + b"\x00" * 63)).hex())
# print(state_to_digest(sha2_256_block([0] * 8, block=b"\x80" + b"\x00" * 63)).hex())
print(state_to_digest(sha2_256_block([0] * 8, block=b"\x00" * 63 + b"\x01")).hex())
