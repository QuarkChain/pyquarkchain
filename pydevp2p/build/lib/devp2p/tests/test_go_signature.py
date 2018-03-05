from devp2p.crypto import ecdsa_sign, mk_privkey, privtopub, ecdsa_recover, ECCx
from rlp.utils import decode_hex
import pyelliptic


def test_pyelliptic_sig():
    priv_seed = 'test'
    priv_key = mk_privkey(priv_seed)
    my_pubkey = privtopub(priv_key)
    e = ECCx(raw_privkey=priv_key)
    msg = 'a'
    s = pyelliptic.ECC.sign(e, msg)
    s2 = pyelliptic.ECC.sign(e, msg)
    assert s != s2  # non deterministic


def test_go_sig():
    """
    go client started with:

    ethereum -port="40404" -loglevel=5  -nodekeyhex="9c22ff5f21f0b81b113e63f7db6da94fedef11b2119b4088b89664fb9a3cb658" -bootnodes="enode://2da47499d52d9161a778e4c711e22e8651cb90350ec066452f9516d1d11eb465d1ec42bb27ec6cd4488b8b6a1a411cb5ef83c16cbb8bee194624bb65fef0f7fd@127.0.0.1:30303"
    """

    r_pubkey = decode_hex("ab16b8c7fc1febb74ceedf1349944ffd4a04d11802451d02e808f08cb3b0c1c1a9c4e1efb7d309a762baa4c9c8da08890b3b712d1666b5b630d6c6a09cbba171")
    d = {'signed_data': 'a061e5b799b5bb3a3a68a7eab6ee11207d90672e796510ac455e985bd206e240',
         'cmd': 'find_node',
         'body': '03f847b840ab16b8c7fc1febb74ceedf1349944ffd4a04d11802451d02e808f08cb3b0c1c1a9c4e1efb7d309a762baa4c9c8da08890b3b712d1666b5b630d6c6a09cbba1718454e869b1',
         'signature': '0de032c62e30f4a9f9f07f25ac5377c5a531116147617a6c08f946c97991f351577e53ae138210bdb7447bab53f3398d746d42c64a9ce67a6248e59353f1bc6e01'}

    priv_seed = 'test'
    priv_key = mk_privkey(priv_seed)
    assert priv_key == decode_hex("9c22ff5f21f0b81b113e63f7db6da94fedef11b2119b4088b89664fb9a3cb658")
    my_pubkey = privtopub(priv_key)
    assert my_pubkey == r_pubkey, (my_pubkey, r_pubkey)
    go_body = decode_hex(d['body'])  # cmd_id, rlp.encoded
    import rlp
    target_node_id, expiry = rlp.decode(go_body[1:])
    assert target_node_id == r_pubkey  # lookup for itself
    go_signed_data = decode_hex(d['signed_data'])
    go_signature = decode_hex(d['signature'])

    my_signature = ecdsa_sign(go_signed_data, priv_key)
    assert my_signature == ecdsa_sign(go_signed_data, priv_key)  # deterministic k

    assert len(go_signed_data) == 32  # sha3()
    assert len(go_signature) == 65
    assert len(my_signature) == 65  # length is okay

    try:
        assert my_signature == go_signature
        failed = False
    except:
        "expected fail, go signatures are not generated with deterministic k"
        failed = True
        pass
    assert failed

    # decoding works when we signed it
    assert my_pubkey == ecdsa_recover(go_signed_data, my_signature)

    # problem we can not decode the pubkey from the go signature
    # and go can not decode ours
    ecdsa_recover(go_signed_data, go_signature)


if __name__ == '__main__':
    test_go_sig()
