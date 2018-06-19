# https://gist.github.com/fjl/3a78780d17c755d22df2 # data used here
# https://github.com/ethereum/cpp-ethereum/blob/develop/test/rlpx.cpp#L183
# https://gist.github.com/fjl/6dd7f51f1bf226488e00

from devp2p.rlpxcipher import RLPxSession
from devp2p.crypto import ECCx, privtopub
from rlp.utils import decode_hex
test_values = \
    {
        "initiator_private_key": "5e173f6ac3c669587538e7727cf19b782a4f2fda07c1eaa662c593e5e85e3051",
        "receiver_private_key": "c45f950382d542169ea207959ee0220ec1491755abe405cd7498d6b16adb6df8",
        "initiator_ephemeral_private_key": "19c2185f4f40634926ebed3af09070ca9e029f2edd5fae6253074896205f5f6c",
        "receiver_ephemeral_private_key": "d25688cf0ab10afa1a0e2dba7853ed5f1e5bf1c631757ed4e103b593ff3f5620",
        "auth_plaintext": "884c36f7ae6b406637c1f61b2f57e1d2cab813d24c6559aaf843c3f48962f32f46662c066d39669b7b2e3ba14781477417600e7728399278b1b5d801a519aa570034fdb5419558137e0d44cd13d319afe5629eeccb47fd9dfe55cc6089426e46cc762dd8a0636e07a54b31169eba0c7a20a1ac1ef68596f1f283b5c676bae4064abfcce24799d09f67e392632d3ffdc12e3d6430dcb0ea19c318343ffa7aae74d4cd26fecb93657d1cd9e9eaf4f8be720b56dd1d39f190c4e1c6b7ec66f077bb1100",
        "authresp_plaintext": "802b052f8b066640bba94a4fc39d63815c377fced6fcb84d27f791c9921ddf3e9bf0108e298f490812847109cbd778fae393e80323fd643209841a3b7f110397f37ec61d84cea03dcc5e8385db93248584e8af4b4d1c832d8c7453c0089687a700",
        "auth_ciphertext": "04a0274c5951e32132e7f088c9bdfdc76c9d91f0dc6078e848f8e3361193dbdc43b94351ea3d89e4ff33ddcefbc80070498824857f499656c4f79bbd97b6c51a514251d69fd1785ef8764bd1d262a883f780964cce6a14ff206daf1206aa073a2d35ce2697ebf3514225bef186631b2fd2316a4b7bcdefec8d75a1025ba2c5404a34e7795e1dd4bc01c6113ece07b0df13b69d3ba654a36e35e69ff9d482d88d2f0228e7d96fe11dccbb465a1831c7d4ad3a026924b182fc2bdfe016a6944312021da5cc459713b13b86a686cf34d6fe6615020e4acf26bf0d5b7579ba813e7723eb95b3cef9942f01a58bd61baee7c9bdd438956b426a4ffe238e61746a8c93d5e10680617c82e48d706ac4953f5e1c4c4f7d013c87d34a06626f498f34576dc017fdd3d581e83cfd26cf125b6d2bda1f1d56",
        "authresp_ciphertext": "049934a7b2d7f9af8fd9db941d9da281ac9381b5740e1f64f7092f3588d4f87f5ce55191a6653e5e80c1c5dd538169aa123e70dc6ffc5af1827e546c0e958e42dad355bcc1fcb9cdf2cf47ff524d2ad98cbf275e661bf4cf00960e74b5956b799771334f426df007350b46049adb21a6e78ab1408d5e6ccde6fb5e69f0f4c92bb9c725c02f99fa72b9cdc8dd53cff089e0e73317f61cc5abf6152513cb7d833f09d2851603919bf0fbe44d79a09245c6e8338eb502083dc84b846f2fee1cc310d2cc8b1b9334728f97220bb799376233e113",
        "ecdhe_shared_secret": "e3f407f83fc012470c26a93fdff534100f2c6f736439ce0ca90e9914f7d1c381",
        "initiator_nonce": "cd26fecb93657d1cd9e9eaf4f8be720b56dd1d39f190c4e1c6b7ec66f077bb11",
        "receiver_nonce": "f37ec61d84cea03dcc5e8385db93248584e8af4b4d1c832d8c7453c0089687a7",
        "aes_secret": "c0458fa97a5230830e05f4f20b7c755c1d4e54b1ce5cf43260bb191eef4e418d",
        "mac_secret": "48c938884d5067a1598272fcddaa4b833cd5e7d92e8228c0ecdfabbe68aef7f1",
        "token": "3f9ec2592d1554852b1f54d228f042ed0a9310ea86d038dc2b401ba8cd7fdac4",
        "initial_egress_MAC": "09771e93b1a6109e97074cbe2d2b0cf3d3878efafe68f53c41bb60c0ec49097e",
        "initial_ingress_MAC": "75823d96e23136c89666ee025fb21a432be906512b3dd4a3049e898adb433847",
        "initiator_hello_packet": "6ef23fcf1cec7312df623f9ae701e63b550cdb8517fefd8dd398fc2acd1d935e6e0434a2b96769078477637347b7b01924fff9ff1c06df2f804df3b0402bbb9f87365b3c6856b45e1e2b6470986813c3816a71bff9d69dd297a5dbd935ab578f6e5d7e93e4506a44f307c332d95e8a4b102585fd8ef9fc9e3e055537a5cec2e9",
        "receiver_hello_packet": "6ef23fcf1cec7312df623f9ae701e63be36a1cdd1b19179146019984f3625d4a6e0434a2b96769050577657247b7b02bc6c314470eca7e3ef650b98c83e9d7dd4830b3f718ff562349aead2530a8d28a8484604f92e5fced2c6183f304344ab0e7c301a0c05559f4c25db65e36820b4b909a226171a60ac6cb7beea09376d6d8"
    }

for k, v in test_values.items():
    test_values[k] = decode_hex(v)


keys = ['initiator_private_key',
        'receiver_private_key',
        'initiator_ephemeral_private_key',
        'receiver_ephemeral_private_key',
        'initiator_nonce',
        'receiver_nonce',
        # auth
        'auth_plaintext',
        'auth_ciphertext',
        # auth response
        'authresp_plaintext',
        'authresp_ciphertext',
        # on ack receive
        'ecdhe_shared_secret',
        'aes_secret',
        'mac_secret',
        'token',
        'initial_egress_MAC',
        'initial_ingress_MAC',
        # messages
        'initiator_hello_packet',
        'receiver_hello_packet'
        ]

assert set(keys) == set(test_values.keys())

# see also
# https://github.com/ethereum/cpp-ethereum/blob/develop/test/rlpx.cpp#L183


def test_ecies_decrypt():
    tv = test_values
    e = ECCx(raw_privkey=tv['receiver_private_key'])
    _dec = e.ecies_decrypt(tv['auth_ciphertext'])
    assert len(_dec) == len(tv['auth_plaintext'])
    assert _dec == tv['auth_plaintext']


def test_handshake():
    tv = test_values

    initiator = RLPxSession(ECCx(raw_privkey=tv['initiator_private_key']),
                            is_initiator=True,
                            ephemeral_privkey=tv['initiator_ephemeral_private_key'])
    initiator_pubkey = initiator.ecc.raw_pubkey
    responder = RLPxSession(ECCx(raw_privkey=tv['receiver_private_key']),
                            ephemeral_privkey=tv['receiver_ephemeral_private_key'])
    responder_pubkey = responder.ecc.raw_pubkey

    # test encryption
    _enc = initiator.encrypt_auth_message(tv['auth_plaintext'], responder_pubkey)
    assert len(_enc) == len(tv['auth_ciphertext'])
    assert len(tv['auth_ciphertext']) == 113 + len(tv['auth_plaintext'])  # len

    # test auth_msg plain
    auth_msg = initiator.create_auth_message(remote_pubkey=responder_pubkey,
                                             ephemeral_privkey=tv['initiator_ephemeral_private_key'],
                                             nonce=tv['initiator_nonce'])

    # test auth_msg plain
    assert len(auth_msg) == len(tv['auth_plaintext']) == 194
    assert auth_msg[65:] == tv['auth_plaintext'][65:]  # starts with non deterministic k

    _auth_msg_cipher = initiator.encrypt_auth_message(auth_msg, responder_pubkey)

    # test shared
    responder.ecc.get_ecdh_key(initiator_pubkey) == \
        initiator.ecc.get_ecdh_key(responder_pubkey)

    # test decrypt
    assert auth_msg == responder.ecc.ecies_decrypt(_auth_msg_cipher)

    # check receive
    responder_ephemeral_pubkey = privtopub(tv['receiver_ephemeral_private_key'])
    auth_msg_cipher = tv['auth_ciphertext']
    auth_msg = responder.ecc.ecies_decrypt(auth_msg_cipher)
    assert auth_msg[65:] == tv['auth_plaintext'][65:]  # starts with non deterministic k

    responder.decode_authentication(auth_msg_cipher)
    auth_ack_msg = responder.create_auth_ack_message(responder_ephemeral_pubkey, nonce=tv['receiver_nonce'])
    assert auth_ack_msg == tv['authresp_plaintext']
    auth_ack_msg_cipher = responder.encrypt_auth_ack_message(auth_ack_msg, remote_pubkey=responder.remote_pubkey)

    # set auth ack msg cipher (needed later for mac calculation)
    responder.auth_ack = tv['authresp_ciphertext']

    responder.setup_cipher()
    assert responder.ecdhe_shared_secret == tv['ecdhe_shared_secret']
    assert len(responder.token) == len(tv['token'])
    assert responder.token == tv['token']
    assert responder.aes_secret == tv['aes_secret']
    assert responder.mac_secret == tv['mac_secret']

    assert responder.initiator_nonce == tv['initiator_nonce']
    assert responder.responder_nonce == tv['receiver_nonce']

    assert responder.auth_init == tv['auth_ciphertext']
    assert responder.auth_ack == tv['authresp_ciphertext']

    # test values are from initiator perspective?
    assert responder.ingress_mac.digest() == tv['initial_egress_MAC']
    assert responder.ingress_mac.digest() == tv['initial_egress_MAC']
    assert responder.egress_mac.digest() == tv['initial_ingress_MAC']
    assert responder.egress_mac.digest() == tv['initial_ingress_MAC']

    r = responder.decrypt(tv['initiator_hello_packet'])

    # unpack hello packet
    import struct
    import rlp
    import rlp.sedes as sedes
    from rlp.codec import consume_item

    header = r['header']
    frame_length = struct.unpack(b'>I', b'\x00' + header[:3])[0]

    header_sedes = sedes.List([sedes.big_endian_int, sedes.big_endian_int])
    header_data = rlp.decode(header[3:], strict=False, sedes=header_sedes)
    print('header', repr(header_data))

    # frame
    frame = r['frame']

    # normal: rlp(packet-type) [|| rlp(packet-data)] || padding
    packet_type, end = consume_item(frame, start=0)
    packet_type = rlp.decode(frame, sedes=sedes.big_endian_int, strict=False)
    print('packet_type', repr(packet_type))

    # decode hello body
    _sedes_capabilites_tuple = sedes.List([sedes.binary, sedes.big_endian_int])

    structure = [
        ('version', sedes.big_endian_int),
        ('client_version_string', sedes.big_endian_int),
        ('capabilities', sedes.CountableList(_sedes_capabilites_tuple)),
        ('listen_port', sedes.big_endian_int),
        ('remote_pubkey', sedes.binary)
    ]

    hello_sedes = sedes.List([x[1] for x in structure])
    frame_data = rlp.decode(frame[end:], sedes=hello_sedes)
    frame_data = dict((structure[i][0], x) for i, x in enumerate(frame_data))
    print('frame', frame_data)
