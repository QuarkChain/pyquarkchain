import devp2p.crypto as crypto
from rlp.utils import decode_hex


def test_ecies_enc():
    bob = crypto.ECCx()
    msg = b'test yeah'
    ciphertext = crypto.ECCx.ecies_encrypt(msg, bob.raw_pubkey)
    _dec = bob.ecies_decrypt(ciphertext)
    assert _dec == msg


# tests from cpp client ##############################################

def fromHex(x):
    return decode_hex(x[2:])


def test_sha256():
    emptyExpected = fromHex("0xe3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    empty = b''
    emptyTestOut = crypto.sha256(empty).digest()
    assert(emptyExpected == emptyTestOut)

    hash1Expected = fromHex("0x8949b278bbafb8da1aaa18cb724175c5952280f74be5d29ab4b37d1b45c84b08")
    hash1input = fromHex("0x55a53b55afb12affff3c")
    hash1Out = crypto.sha256(hash1input).digest()
    assert(hash1Out == hash1Expected)


def test_hmac():
    k_mac = fromHex("0x07a4b6dfa06369a570f2dcba2f11a18f")
    indata = fromHex("0x4dcb92ed4fc67fe86832")
    hmacExpected = fromHex("0xc90b62b1a673b47df8e395e671a68bfa68070d6e2ef039598bb829398b89b9a9")
    hmacOut = crypto.hmac_sha256(k_mac, indata)
    assert(hmacExpected == hmacOut)

    # go messageTag
    tagSecret = fromHex("0xaf6623e52208c596e17c72cea6f1cb09")
    tagInput = fromHex("0x3461282bcedace970df2")
    tagExpected = fromHex("0xb3ce623bce08d5793677ba9441b22bb34d3e8a7de964206d26589df3e8eb5183")
    hmacOut = crypto.hmac_sha256(tagSecret, tagInput)
    assert(hmacOut == tagExpected)


def test_kdf():
    input1 = fromHex("0x0de72f1223915fa8b8bf45dffef67aef8d89792d116eb61c9a1eb02c422a4663")
    expect1 = fromHex("0x1d0c446f9899a3426f2b89a8cb75c14b")
    test1 = crypto.eciesKDF(input1, 16)
    assert len(test1) == len(expect1)
    assert(test1 == expect1)

    kdfInput2 = fromHex("0x961c065873443014e0371f1ed656c586c6730bf927415757f389d92acf8268df")
    kdfExpect2 = fromHex("0x4050c52e6d9c08755e5a818ac66fabe478b825b1836fd5efc4d44e40d04dabcc")
    kdfTest2 = crypto.eciesKDF(kdfInput2, 32)
    assert(len(kdfTest2) == len(kdfExpect2))
    assert(kdfTest2 == kdfExpect2)


def test_agree():
    secret = fromHex("0x332143e9629eedff7d142d741f896258f5a1bfab54dab2121d3ec5000093d74b")
    public = fromHex(
        "0xf0d2b97981bd0d415a843b5dfe8ab77a30300daab3658c578f2340308a2da1a07f0821367332598b6aa4e180a41e92f4ebbae3518da847f0b1c0bbfe20bcf4e1")
    agreeExpected = fromHex("0xee1418607c2fcfb57fda40380e885a707f49000a5dda056d828b7d9bd1f29a08")
    e = crypto.ECCx(raw_privkey=secret)
    agreeTest = e.raw_get_ecdh_key(pubkey_x=public[:32], pubkey_y=public[32:])
    assert(agreeExpected == agreeTest)


def test_decrypt():
    kmK = fromHex("0x57baf2c62005ddec64c357d96183ebc90bf9100583280e848aa31d683cad73cb")
    kmCipher = fromHex(
        "0x04ff2c874d0a47917c84eea0b2a4141ca95233720b5c70f81a8415bae1dc7b746b61df7558811c1d6054333907333ef9bb0cc2fbf8b34abb9730d14e0140f4553f4b15d705120af46cf653a1dc5b95b312cf8444714f95a4f7a0425b67fc064d18f4d0a528761565ca02d97faffdac23de10")
    kmExpected = b"a"
    kmPlain = crypto.ECCx(raw_privkey=kmK).ecies_decrypt(kmCipher)
    assert(kmExpected == kmPlain)


def test_privtopub():
    kenc = fromHex("0x472413e97f1fd58d84e28a559479e6b6902d2e8a0cee672ef38a3a35d263886b")
    penc = fromHex(
        "0x7a2aa2951282279dc1171549a7112b07c38c0d97c0fe2c0ae6c4588ba15be74a04efc4f7da443f6d61f68a9279bc82b73e0cc8d090048e9f87e838ae65dd8d4c")
    assert(penc == crypto.privtopub(kenc))
    return kenc, penc


def test_decrypt1():
    kenc, penc = test_privtopub()
    cipher1 = fromHex(
        "0x046f647e1bd8a5cd1446d31513bac233e18bdc28ec0e59d46de453137a72599533f1e97c98154343420d5f16e171e5107999a7c7f1a6e26f57bcb0d2280655d08fb148d36f1d4b28642d3bb4a136f0e33e3dd2e3cffe4b45a03fb7c5b5ea5e65617250fdc89e1a315563c20504b9d3a72555")

    expectedPlain1 = b"a"
    plainTest1 = crypto.ECCx(raw_privkey=kenc).ecies_decrypt(cipher1)
    assert(expectedPlain1 == plainTest1)


def test_decrypt2():
    kenc, penc = test_privtopub()
    cipher2 = fromHex(
        "0x0443c24d6ccef3ad095140760bb143078b3880557a06392f17c5e368502d79532bc18903d59ced4bbe858e870610ab0d5f8b7963dd5c9c4cf81128d10efd7c7aa80091563c273e996578403694673581829e25a865191bdc9954db14285b56eb0043b6288172e0d003c10f42fe413222e273d1d4340c38a2d8344d7aadcbc846ee")
    expectedPlain2 = b"aaaaaaaaaaaaaaaa"
    plainTest2 = crypto.ECCx(raw_privkey=kenc).ecies_decrypt(cipher2)
    assert(expectedPlain2 == plainTest2)


def test_decrypt3():
    kenc, penc = test_privtopub()
    cipher3 = fromHex(
        "0x04c4e40c86bb5324e017e598c6d48c19362ae527af8ab21b077284a4656c8735e62d73fb3d740acefbec30ca4c024739a1fcdff69ecaf03301eebf156eb5f17cca6f9d7a7e214a1f3f6e34d1ee0ec00ce0ef7d2b242fbfec0f276e17941f9f1bfbe26de10a15a6fac3cda039904ddd1d7e06e7b96b4878f61860e47f0b84c8ceb64f6a900ff23844f4359ae49b44154980a626d3c73226c19e")
    expectedPlain3 = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    plainTest3 = crypto.ECCx(raw_privkey=kenc).ecies_decrypt(cipher3)
    assert(expectedPlain3 == plainTest3)
