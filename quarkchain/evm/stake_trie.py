#!/usr/bin/python3
# trie.py from ethereum under MIT license
# use rlp to encode/decode a node as the original code
import rlp
from quarkchain import utils
from quarkchain.evm.fast_rlp import encode_optimized
from rlp.sedes import big_endian_int
from quarkchain.evm.utils import int_to_big_endian, big_endian_to_int
from quarkchain.evm.trie import (
    to_bytes,
    bin_to_nibbles,
    nibbles_to_bin,
    with_terminator,
    without_terminator,
    adapt_terminator,
    pack_nibbles,
    unpack_to_nibbles,
    starts_with,
    is_key_value_type,
)

rlp_encode = encode_optimized

bin_to_nibbles_cache = {}


hti = {}
for i, c in enumerate("0123456789abcdef"):
    hti[c] = i

itoh = {}
for i, c in enumerate("0123456789abcdef"):
    itoh[i] = c


def nibbles_to_address(nibbles):
    """convert nibbles to address (hex public key address starting with "0x")
    >>> nibbles_to_address([2, 4, 13, 13])
    "0x24dd"
    """
    if any(x > 15 or x < 0 for x in nibbles):
        raise Exception("nibbles can only be [0,..15]")
    res = "0x"
    for n in nibbles:
        res += itoh[n]
    return res


(NODE_TYPE_BLANK, NODE_TYPE_LEAF, NODE_TYPE_EXTENSION, NODE_TYPE_BRANCH) = tuple(
    range(4)
)


NIBBLE_TERMINATOR = 16
BLANK_NODE = b""
BLANK_ROOT = utils.sha3_256(rlp.encode(b""))


class Stake_Trie(object):
    def __init__(self, db, root_hash=BLANK_ROOT):
        """it also present a dictionary like interface

        :param db key value database
        :root: blank or stake trie node in form of [key, [value, token]] or [[v0, token],[v1, token]..[v15, token],[v, token]]
        :token: the total numbers of tokens rooted at that node (i.e., the number of tokens below it)
        All operations that modify the trie must adjust the token information
        """
        self.db = db  # Pass in a database object directly
        self.set_root_hash(root_hash)
        self.deletes = []

    # def __init__(self, dbfile, root_hash=BLANK_ROOT):
    #     """it also present a dictionary like interface

    #     :param dbfile: key value database
    #     :root: blank or trie node in form of [key, value] or [v0,v1..v15,v]
    #     """
    #     if isinstance(dbfile, str):
    #         dbfile = os.path.abspath(dbfile)
    #         self.db = DB(dbfile)
    #     else:
    # self.db = dbfile  # Pass in a database object directly
    #     self.set_root_hash(root_hash)

    @property
    def root_hash(self):
        """always empty or a 32 bytes
        """
        return self._root_hash

    def get_root_hash(self):
        return self._root_hash

    def _update_root_hash(self):
        val = rlp_encode(self.root_node)
        key = utils.sha3_256(val)
        self.db.put(key, val)
        self._root_hash = key

    @root_hash.setter
    def root_hash(self, value):
        self.set_root_hash(value)

    def set_root_hash(self, root_hash):
        assert isinstance(root_hash, bytes)
        assert len(root_hash) in [0, 32]
        if root_hash == BLANK_ROOT:
            self.root_node = BLANK_NODE
            self._root_hash = BLANK_ROOT
            return
        self.root_node = self._decode_to_node(root_hash)
        self._root_hash = root_hash

    def clear(self):
        """ clear all tree data
        """
        self._delete_child_storage(self.root_node)
        self._delete_node_storage(self.root_node)
        self.root_node = BLANK_NODE
        self._root_hash = BLANK_ROOT

    def _delete_child_storage(self, node):
        node_type = self._get_node_type(node)
        if node_type == NODE_TYPE_BRANCH:
            for item in node[:16]:
                self._delete_child_storage(self._decode_to_node(item))
        elif node_type == NODE_TYPE_EXTENSION:
            self._delete_child_storage(self._decode_to_node(node[1]))

    def _encode_node(self, node, put_in_db=True):
        """
        All the operations that modify the trie must adjust the stake information
        """
        if node == BLANK_NODE:
            return BLANK_NODE
        # assert isinstance(node, list)
        node_type = self._get_node_type(node)
        stake_sum = 0
        if is_key_value_type(node_type):
            stake_sum = big_endian_to_int(node[1][1])
        elif node_type == NODE_TYPE_BRANCH:
            for i in range(17):
                if node[i] != BLANK_NODE:
                    stake_sum += big_endian_to_int(node[i][1])

        rlpnode = rlp_encode(node)
        # may fix
        # if len(rlpnode) < 32:
        #    return node

        hashkey = utils.sha3_256(rlpnode)
        if put_in_db:
            self.db.put(hashkey, rlpnode)
        return [hashkey, int_to_big_endian(stake_sum)]

    def _decode_to_node(self, encoded):
        if encoded == BLANK_NODE:
            return BLANK_NODE

        encoded = encoded[0]

        if isinstance(encoded, list):
            return encoded
        o = rlp.decode(self.db[encoded])
        return o

    def _get_node_type(self, node):
        """ get node type and content

        :param node: node in form of list, or BLANK_NODE
        :return: node type
        """
        if node == BLANK_NODE:
            return NODE_TYPE_BLANK

        if len(node) == 2:
            nibbles = unpack_to_nibbles(node[0])
            has_terminator = nibbles and nibbles[-1] == NIBBLE_TERMINATOR
            return NODE_TYPE_LEAF if has_terminator else NODE_TYPE_EXTENSION
        if len(node) == 17:
            return NODE_TYPE_BRANCH

    def _get(self, node, key):
        """ get value inside a node

        :param node: node in form of list, or BLANK_NODE
        :param key: nibble list without terminator
        :return:
            BLANK_NODE if does not exist, otherwise value or hash
        """
        node_type = self._get_node_type(node)

        if node_type == NODE_TYPE_BLANK:
            return BLANK_NODE

        if node_type == NODE_TYPE_BRANCH:
            # already reach the expected node
            if not key:
                return node[-1][1]
            sub_node = self._decode_to_node(node[key[0]])
            return self._get(sub_node, key[1:])

        # key value node
        curr_key = without_terminator(unpack_to_nibbles(node[0]))
        if node_type == NODE_TYPE_LEAF:
            return node[1][1] if key == curr_key else BLANK_NODE

        if node_type == NODE_TYPE_EXTENSION:
            # traverse child nodes
            if starts_with(key, curr_key):
                sub_node = self._decode_to_node(node[1])
                return self._get(sub_node, key[len(curr_key) :])
            else:
                return BLANK_NODE

    def _update(self, node, key, value):
        """ update item inside a node

        :param node: node in form of list, or BLANK_NODE
        :param key: nibble list without terminator
            .. note:: key may be []
        :param value: value bytes
        :return: new node

        if this node is changed to a new node, it's parent will take the
        responsibility to *store* the new node storage, and delete the old
        node storage
        """
        node_type = self._get_node_type(node)

        if node_type == NODE_TYPE_BLANK:
            return [pack_nibbles(with_terminator(key)), [value, value]]

        elif node_type == NODE_TYPE_BRANCH:
            if not key:
                node[-1] = [value, value]
            else:
                new_node = self._update_and_delete_storage(
                    self._decode_to_node(node[key[0]]), key[1:], value
                )
                node[key[0]] = self._encode_node(new_node)
            return node

        elif is_key_value_type(node_type):
            return self._update_kv_node(node, key, value)

    def _update_and_delete_storage(self, node, key, value):
        old_node = node[:]
        new_node = self._update(node, key, value)
        if old_node != new_node:
            self._delete_node_storage(old_node)
        return new_node

    def _update_kv_node(self, node, key, value):
        node_type = self._get_node_type(node)
        curr_key = without_terminator(unpack_to_nibbles(node[0]))
        is_inner = node_type == NODE_TYPE_EXTENSION

        # find longest common prefix
        prefix_length = 0
        for i in range(min(len(curr_key), len(key))):
            if key[i] != curr_key[i]:
                break
            prefix_length = i + 1

        remain_key = key[prefix_length:]
        remain_curr_key = curr_key[prefix_length:]

        if remain_key == [] == remain_curr_key:
            if not is_inner:
                return [node[0], [value, value]]
            new_node = self._update_and_delete_storage(
                self._decode_to_node(node[1]), remain_key, value
            )

        elif remain_curr_key == []:
            if is_inner:
                new_node = self._update_and_delete_storage(
                    self._decode_to_node(node[1]), remain_key, value
                )
            else:
                new_node = [BLANK_NODE] * 17
                new_node[-1] = node[1]
                new_node[remain_key[0]] = self._encode_node(
                    [pack_nibbles(with_terminator(remain_key[1:])), [value, value]]
                )
        else:
            new_node = [BLANK_NODE] * 17
            if len(remain_curr_key) == 1 and is_inner:
                new_node[remain_curr_key[0]] = node[1]
            else:
                new_node[remain_curr_key[0]] = self._encode_node(
                    [
                        pack_nibbles(
                            adapt_terminator(remain_curr_key[1:], not is_inner)
                        ),
                        node[1],
                    ]
                )

            if remain_key == []:
                new_node[-1] = [value, value]
            else:
                new_node[remain_key[0]] = self._encode_node(
                    [pack_nibbles(with_terminator(remain_key[1:])), [value, value]]
                )

        if prefix_length:
            # create node for key prefix
            return [pack_nibbles(curr_key[:prefix_length]), self._encode_node(new_node)]
        else:
            return new_node

    def _getany(self, node, reverse=False, path=[]):
        # print('getany', node, 'reverse=', reverse, path)
        node_type = self._get_node_type(node)
        if node_type == NODE_TYPE_BLANK:
            return None
        if node_type == NODE_TYPE_BRANCH:
            if node[16] and not reverse:
                # print('found!', [16], path)
                return [16]
            scan_range = list(range(16))
            if reverse:
                scan_range.reverse()
            for i in scan_range:
                o = self._getany(
                    self._decode_to_node(node[i]), reverse=reverse, path=path + [i]
                )
                if o is not None:
                    # print('found@', [i] + o, path)
                    return [i] + o
            if node[16] and reverse:
                # print('found!', [16], path)
                return [16]
            return None
        curr_key = without_terminator(unpack_to_nibbles(node[0]))
        if node_type == NODE_TYPE_LEAF:
            # print('found#', curr_key, path)
            return curr_key

        if node_type == NODE_TYPE_EXTENSION:
            sub_node = self._decode_to_node(node[1])
            return curr_key + self._getany(
                sub_node, reverse=reverse, path=path + curr_key
            )

    def _iter(self, node, key, reverse=False, path=[]):
        # print('iter', node, key, 'reverse =', reverse, 'path =', path)
        node_type = self._get_node_type(node)

        if node_type == NODE_TYPE_BLANK:
            return None

        elif node_type == NODE_TYPE_BRANCH:
            # print('b')
            if len(key):
                sub_node = self._decode_to_node(node[key[0]])
                o = self._iter(sub_node, key[1:], reverse, path + [key[0]])
                if o is not None:
                    # print('returning', [key[0]] + o, path)
                    return [key[0]] + o
            if reverse:
                scan_range = reversed(list(range(key[0] if len(key) else 0)))
            else:
                scan_range = list(range(key[0] + 1 if len(key) else 0, 16))
            for i in scan_range:
                sub_node = self._decode_to_node(node[i])
                # print('prelim getany', path+[i])
                o = self._getany(sub_node, reverse, path + [i])
                if o is not None:
                    # print('returning', [i] + o, path)
                    return [i] + o
            if reverse and key and node[16]:
                # print('o')
                return [16]
            return None

        descend_key = without_terminator(unpack_to_nibbles(node[0]))
        if node_type == NODE_TYPE_LEAF:
            if reverse:
                # print('L', descend_key, key, descend_key if descend_key < key else None, path)
                return descend_key if descend_key < key else None
            else:
                # print('L', descend_key, key, descend_key if descend_key > key else None, path)
                return descend_key if descend_key > key else None

        if node_type == NODE_TYPE_EXTENSION:
            # traverse child nodes
            sub_node = self._decode_to_node(node[1])
            sub_key = key[len(descend_key) :]
            # print('amhere', key, descend_key, descend_key > key[:len(descend_key)])
            if starts_with(key, descend_key):
                o = self._iter(sub_node, sub_key, reverse, path + descend_key)
            elif descend_key > key[: len(descend_key)] and not reverse:
                # print(1)
                # print('prelim getany', path+descend_key)
                o = self._getany(sub_node, False, path + descend_key)
            elif descend_key < key[: len(descend_key)] and reverse:
                # print(2)
                # print('prelim getany', path+descend_key)
                o = self._getany(sub_node, True, path + descend_key)
            else:
                o = None
            # print('returning@', descend_key + o if o else None, path)
            return descend_key + o if o else None

    def next(self, key):
        # print('nextting')
        key = bin_to_nibbles(key)
        o = self._iter(self.root_node, key)
        # print('answer', o)
        return nibbles_to_bin(without_terminator(o)) if o else None

    def prev(self, key):
        # print('prevving')
        key = bin_to_nibbles(key)
        o = self._iter(self.root_node, key, reverse=True)
        # print('answer', o)
        return nibbles_to_bin(without_terminator(o)) if o else None

    def _delete_node_storage(self, node):
        """delete storage
        :param node: node in form of list, or BLANK_NODE
        """
        if node == BLANK_NODE:
            return
        # assert isinstance(node, list)
        encoded = self._encode_node(node, put_in_db=False)
        encoded = encoded[0]
        if len(encoded) < 32:
            return
        """
        ===== FIXME ====
        in the current trie implementation two nodes can share identical subtrees
        thus we can not safely delete nodes for now
        """
        self.deletes.append(encoded)
        # print('del', encoded, self.db.get_refcount(encoded))

    def _delete(self, node, key):
        """ update item inside a node

        :param node: node in form of list, or BLANK_NODE
        :param key: nibble list without terminator
            .. note:: key may be []
        :return: new node

        if this node is changed to a new node, it's parent will take the
        responsibility to *store* the new node storage, and delete the old
        node storage
        """
        node_type = self._get_node_type(node)
        if node_type == NODE_TYPE_BLANK:
            return BLANK_NODE

        if node_type == NODE_TYPE_BRANCH:
            return self._delete_branch_node(node, key)

        if is_key_value_type(node_type):
            return self._delete_kv_node(node, key)

    def _normalize_branch_node(self, node):
        """node should have only one item changed
        """
        not_blank_items_count = sum(1 for x in range(17) if node[x])
        assert not_blank_items_count >= 1

        if not_blank_items_count > 1:
            return node

        # now only one item is not blank
        not_blank_index = [i for i, item in enumerate(node) if item][0]

        # the value item is not blank
        if not_blank_index == 16:
            return [pack_nibbles(with_terminator([])), node[16]]

        # normal item is not blank
        sub_node = self._decode_to_node(node[not_blank_index])
        sub_node_type = self._get_node_type(sub_node)

        if is_key_value_type(sub_node_type):
            # collape subnode to this node, not this node will have same
            # terminator with the new sub node, and value does not change
            new_key = [not_blank_index] + unpack_to_nibbles(sub_node[0])
            return [pack_nibbles(new_key), sub_node[1]]
        if sub_node_type == NODE_TYPE_BRANCH:
            return [pack_nibbles([not_blank_index]), self._encode_node(sub_node)]
        assert False

    def _delete_and_delete_storage(self, node, key):
        old_node = node[:]
        new_node = self._delete(node, key)
        if old_node != new_node:
            self._delete_node_storage(old_node)
        return new_node

    def _delete_branch_node(self, node, key):
        # already reach the expected node
        if not key:
            node[-1] = BLANK_NODE
            return self._normalize_branch_node(node)

        encoded_new_sub_node = self._encode_node(
            self._delete_and_delete_storage(self._decode_to_node(node[key[0]]), key[1:])
        )

        if encoded_new_sub_node == node[key[0]]:
            return node

        node[key[0]] = encoded_new_sub_node
        if encoded_new_sub_node == BLANK_NODE:
            return self._normalize_branch_node(node)

        return node

    def _delete_kv_node(self, node, key):
        node_type = self._get_node_type(node)
        assert is_key_value_type(node_type)
        curr_key = without_terminator(unpack_to_nibbles(node[0]))

        if not starts_with(key, curr_key):
            # key not found
            return node

        if node_type == NODE_TYPE_LEAF:
            return BLANK_NODE if key == curr_key else node

        # for inner key value type
        new_sub_node = self._delete_and_delete_storage(
            self._decode_to_node(node[1]), key[len(curr_key) :]
        )

        if self._encode_node(new_sub_node) == node[1]:
            return node

        # new sub node is BLANK_NODE
        if new_sub_node == BLANK_NODE:
            return BLANK_NODE

        # assert isinstance(new_sub_node, list)

        # new sub node not blank, not value and has changed
        new_sub_node_type = self._get_node_type(new_sub_node)

        if is_key_value_type(new_sub_node_type):
            # collape subnode to this node, not this node will have same
            # terminator with the new sub node, and value does not change
            new_key = curr_key + unpack_to_nibbles(new_sub_node[0])
            return [pack_nibbles(new_key), new_sub_node[1]]

        if new_sub_node_type == NODE_TYPE_BRANCH:
            return [pack_nibbles(curr_key), self._encode_node(new_sub_node)]

        # should be no more cases
        assert False

    def delete(self, key):
        """
        :param key: a bytes with length of [0, 32]
        """
        if not isinstance(key, bytes):
            raise Exception("Key must be bytes")

        if len(key) > 32:
            raise Exception("Max key length is 32")

        self.root_node = self._delete_and_delete_storage(
            self.root_node, bin_to_nibbles(key)
        )
        self._update_root_hash()

    def _get_size(self, node):
        """Get counts of (key, value) stored in this and the descendant nodes

        :param node: node in form of list, or BLANK_NODE
        """
        if node == BLANK_NODE:
            return 0

        node_type = self._get_node_type(node)

        if is_key_value_type(node_type):
            value_is_node = node_type == NODE_TYPE_EXTENSION
            if value_is_node:
                return self._get_size(self._decode_to_node(node[1]))
            else:
                return 1
        elif node_type == NODE_TYPE_BRANCH:
            sizes = [self._get_size(self._decode_to_node(node[x])) for x in range(16)]
            sizes = sizes + [1 if node[-1] else 0]
            return sum(sizes)

    def _iter_branch(self, node):
        """yield (key, value) stored in this and the descendant nodes
        :param node: node in form of list, or BLANK_NODE

        .. note::
            Here key is in full form, rather than key of the individual node
        """
        if node == BLANK_NODE:
            raise StopIteration

        node_type = self._get_node_type(node)

        if is_key_value_type(node_type):
            nibbles = without_terminator(unpack_to_nibbles(node[0]))
            key = b"+".join([to_bytes(x) for x in nibbles])
            if node_type == NODE_TYPE_EXTENSION:
                sub_tree = self._iter_branch(self._decode_to_node(node[1]))
            else:
                sub_tree = [(to_bytes(NIBBLE_TERMINATOR), node[1][1])]

            # prepend key of this node to the keys of children
            for sub_key, sub_value in sub_tree:
                full_key = (key + b"+" + sub_key).strip(b"+")
                yield (full_key, sub_value)

        elif node_type == NODE_TYPE_BRANCH:
            for i in range(16):
                sub_tree = self._iter_branch(self._decode_to_node(node[i]))
                for sub_key, sub_value in sub_tree:
                    full_key = (bytes(str(i), "ascii") + b"+" + sub_key).strip(b"+")
                    yield (full_key, sub_value)
            if node[16]:
                yield (to_bytes(NIBBLE_TERMINATOR), node[-1][1])

    def iter_branch(self):
        for key_str, value in self._iter_branch(self.root_node):
            if key_str:
                nibbles = [int(x) for x in key_str.split(b"+")]
            else:
                nibbles = []
            key = nibbles_to_bin(without_terminator(nibbles))
            yield key, value

    def _to_dict(self, node):
        """convert (key, value) stored in this and the descendant nodes
        to dict items.

        :param node: node in form of list, or BLANK_NODE

        .. note::

            Here key is in full form, rather than key of the individual node
        """
        if node == BLANK_NODE:
            return {}

        node_type = self._get_node_type(node)

        if is_key_value_type(node_type):
            nibbles = without_terminator(unpack_to_nibbles(node[0]))
            key = b"+".join([to_bytes(x) for x in nibbles])
            if node_type == NODE_TYPE_EXTENSION:
                sub_dict = self._to_dict(self._decode_to_node(node[1]))
            else:
                sub_dict = {to_bytes(NIBBLE_TERMINATOR): node[1][1]}

            # prepend key of this node to the keys of children
            res = {}
            for sub_key, sub_value in sub_dict.items():
                full_key = (key + b"+" + sub_key).strip(b"+")
                res[full_key] = sub_value
            return res

        elif node_type == NODE_TYPE_BRANCH:
            res = {}
            for i in range(16):
                sub_dict = self._to_dict(self._decode_to_node(node[i]))

                for sub_key, sub_value in sub_dict.items():
                    full_key = (bytes(str(i), "ascii") + b"+" + sub_key).strip(b"+")
                    res[full_key] = sub_value

            if node[16]:
                res[to_bytes(NIBBLE_TERMINATOR)] = node[-1][1]
            return res

    def to_dict(self):
        d = self._to_dict(self.root_node)
        res = {}
        for key_str, value in d.items():
            if key_str:
                nibbles = [int(x) for x in key_str.split(b"+")]
            else:
                nibbles = []
            key = nibbles_to_bin(without_terminator(nibbles))
            res[key] = value
        return res

    def get(self, key):
        if not isinstance(key, bytes):
            raise Exception("Key must be bytes")
        return self._get(self.root_node, bin_to_nibbles(to_bytes(key)))

    def update(self, key, value):
        """
        :param key: a bytes
        :value: a bytes
        """
        if not isinstance(key, bytes):
            raise Exception("Key must be bytes")

        # if len(key) > 32:
        #     raise Exception("Max key length is 32")

        if not isinstance(value, bytes):
            raise Exception("Value must be bytes")

        # if value == '':
        #     return self.delete(key)
        self.root_node = self._update_and_delete_storage(
            self.root_node, bin_to_nibbles(key), value
        )
        self._update_root_hash()

    def root_hash_valid(self):
        if self.root_hash == BLANK_ROOT:
            return True
        return self.root_hash in self.db

    # New functions for POS (Photon)
    def _get_total_stake(self, node):
        """Get the total stake

        :param node: node in form of list, or BLANK_NODE
        """
        if node == BLANK_NODE:
            return 0

        node_type = self._get_node_type(node)

        if is_key_value_type(node_type):
            stake_is_node = node_type == NODE_TYPE_EXTENSION
            if stake_is_node:
                return self._get_total_stake(self._decode_to_node(node[1]))
            else:
                return big_endian_to_int(node[1][1])
        elif node_type == NODE_TYPE_BRANCH:
            tokens = [
                self._get_total_stake(self._decode_to_node(node[x])) for x in range(16)
            ]
            tokens = tokens + [big_endian_to_int(node[-1][1]) if node[-1] else 0]
            return sum(tokens)

    def _get_total_stake_from_root_node(self, node):
        """ Get the total stake directly from root node informaiton

        :param node: node in form of list, or BLANK_NODE
        """
        if node == BLANK_NODE:
            return 0

        node_type = self._get_node_type(node)

        if is_key_value_type(node_type):
            return big_endian_to_int(node[1][1])
        else:
            stake_sum = 0
            for i in range(17):
                if node[i] != BLANK_NODE:
                    stake_sum += big_endian_to_int(node[i][1])
            return stake_sum

    def _select_staker(self, node, value):
        """ Get the selected staker address given the pseudo-randomly value

        :param node: node in form of list, or BLANK_NODE
        :value: pseudo-randomly selected value
        """
        node_type = self._get_node_type(node)
        assert value >= 0

        if node_type == NODE_TYPE_BLANK:
            return None

        if node_type == NODE_TYPE_BRANCH:
            scan_range = list(range(17))
            for i in scan_range:
                if node[i] != BLANK_NODE:
                    if big_endian_to_int(node[i][1]) >= value:
                        sub_node = self._decode_to_node(node[i])
                        o = self._select_staker(sub_node, value)
                        return [i] + o if o is not None else None
                    else:
                        value = value - big_endian_to_int(node[i][1])
            return None

        if node_type == NODE_TYPE_LEAF:
            descend_key = without_terminator(unpack_to_nibbles(node[0]))
            return descend_key if value <= big_endian_to_int(node[1][1]) else None

        elif node_type == NODE_TYPE_EXTENSION:
            descend_key = without_terminator(unpack_to_nibbles(node[0]))
            if value <= big_endian_to_int(node[1][1]):
                sub_node = self._decode_to_node(node[1])
                o = self._select_staker(sub_node, value)
                return descend_key + o if o else None
            else:
                return None

        return None

    def _check_total_tokens(self):
        if self.root_node != BLANK_NODE:
            assert self._get_total_stake(
                self.root_node
            ) == self._get_total_stake_from_root_node(self.root_node)

    def get_total_stake(self):
        return self._get_total_stake_from_root_node(self.root_node)

    def select_staker(self, value):
        o = self._select_staker(self.root_node, value)
        return nibbles_to_address(o)

    def __len__(self):
        return self._get_size(self.root_node)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.update(key, value)

    def __delitem__(self, key):
        return self.delete(key)

    def __iter__(self):
        return iter(self.to_dict())

    def __contains__(self, key):
        return self.get(key) != BLANK_NODE


if __name__ == "__main__":
    import sys
    from quarkchain.db import PersistentDb

    _db = PersistentDb(path=sys.argv[2])

    def encode_node(nd):
        if isinstance(nd, bytes):
            return nd.hex()
        else:
            return rlp_encode(nd).hex()

    if len(sys.argv) >= 2:
        if sys.argv[1] == "insert":
            t = Stake_Trie(_db, bytes.fromhex(sys.argv[3]))
            t.update(bytes(sys.argv[4], "ascii"), bytes(sys.argv[5], "ascii"))
            print(encode_node(t.root_hash))
        elif sys.argv[1] == "get":
            t = Stake_Trie(_db, bytes.fromhex(sys.argv[3]))
            print(t.get(bytes(sys.argv[4], "ascii")))
