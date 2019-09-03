#include <cassert>
#include <cstdint>
#include <cstring>

#include <iostream>
#include <vector>

#include "util.h"

namespace org {
namespace quarkchain {

template <typename T>
class LLRB {

private:
    const uint32_t NULL_IDX = 0xFFFFFFFF;
    struct Node {
        T value;
        uint32_t left;
        uint32_t right;
        bool color;
        uint32_t size;      // number of elements on left
    };

public:
    static uint32_t getNodeSize() {
        return sizeof (Node);
    }

public:
    LLRB(uintptr_t arenaBase,
         uint64_t arenaSize)
         : arenaBase_(arenaBase),
           arenaSize_(arenaSize),
           capacity_(arenaSize_ / sizeof (Node)),
           root_(NULL_IDX) {
        for (int32_t i = arenaSize_ / sizeof (Node) - 1; i >= 0; i--) {
            freeList_.push_back(i);
        }
        resetRotationStats();
    }

    LLRB<T>& operator=(LLRB<T>&& other) {
        arenaBase_ = other.arenaBase_;
        arenaSize_ = other.arenaSize_;
        capacity_ = other.capacity_;
        root_ = other.root_;
        freeList_ = std::move(other.freeList_);
        rotationStats_ = std::move(other.rotationStats_);
        return *this;
    };
    LLRB<T>& operator=(const LLRB<T>& other)  = delete;
    LLRB<T>(LLRB<T>&& o) = default;

    void insert(T value) {
        Node* n = insert(getNode(root_), value);
        root_ = getIndex(n);
    }

    bool contain(T value) {
        Node* n = getNode(root_);
        while (n != nullptr) {
            if (n->value == value) {
                return true;
            } else if (value < n->value) {
                n = getNode(n->left);
            } else {
                n = getNode(n->right);
            }
        }
        return false;
    }

    void erase(T value) {
        CHECK(root_ != NULL_IDX);

        Node* root = getNode(root_);
        if (!isRed(root->left) && !isRed(root->right)) {
            root->color = RED;
        }

        Node* n = erase(root, value);
        root_ = getIndex(n);

        if (n != nullptr) {
            n->color = BLACK;
        }
    }

    T eraseByOrder(uint32_t p) {
        CHECK(p < size());

        Node* root = getNode(root_);
        if (!isRed(root->left) && !isRed(root->right)) {
            root->color = RED;
        }

        T value;
        Node* n = eraseAt(root, p, value);
        root_ = getIndex(n);

        if (n != nullptr) {
            n->color = BLACK;
        }
        return value;
    }

    void print() {
        print(root_);
    }

    LLRB<T> copy(uintptr_t arenaBase) {
        memcpy((void *)arenaBase, (void *)arenaBase_, arenaSize_);
        return LLRB<T>(arenaBase, arenaSize_, root_, freeList_, rotationStats_);
    }

    uint32_t size() {
        return capacity_ - freeList_.size();
    }

    /*
     * Obtain a quick hash of the tree.
     */
    uint64_t hash() {
        return hash(getNode(root_), FNV_OFFSET_BASE_64, 0);
    }

    /*
     * Sanity check.
     */
    bool check() {
        uint32_t sum;
        return isBST(getNode(root_), nullptr, nullptr) &&
               isSizeConsistent(getNode(root_), sum) &&
               sum == size() &&
               is23(getNode(root_));
    }

    uintptr_t getArenaBase() {
        return arenaBase_;
    }
  
    std::array<uint64_t, 4> getRotationStats() {
        return rotationStats_;
    }

    void resetRotationStats() {
        for (size_t i = 0; i < rotationStats_.size(); i++) {
            rotationStats_[i] = 0;
        }
    }
private:
    LLRB(uintptr_t arenaBase,
         uint64_t arenaSize,
         uint32_t root,
         std::vector<uint32_t> freeList,
         std::array<uint64_t, 4> rotationStats)
         : arenaBase_(arenaBase),
           arenaSize_(arenaSize),
           capacity_(arenaSize_ / sizeof (Node)),
           root_(root),
           freeList_(std::move(freeList)),
           rotationStats_(std::move(rotationStats)) { }

    uintptr_t arenaBase_;
    uint64_t arenaSize_;
    uint32_t capacity_;
    uint32_t root_;
    std::vector<uint32_t> freeList_;
    std::array<uint64_t, 4> rotationStats_;

    const bool RED = true;
    const bool BLACK = false;

    void print(uint32_t idx) {
        Node* n = getNode(idx);
        if (n == nullptr) {
            return;
        }

        print(n->left);
        std::cout << n->value << " " << std::endl;
        print(n->right);
    }

    uint64_t hash(Node* h, uint64_t hv, uint64_t idx) {
        if (h == nullptr) {
            return hv;
        }
        hv = fnv64(hv, idx);
        hv = fnv64(hv, h->value ^ (h->color ? 0 : 1));
        hv = hash(getNode(h->left), hv, (idx << 1) + 1);
        hv = hash(getNode(h->right), hv, (idx << 1) + 2);
        return hv;
    }

    Node* rotateLeft(Node* h) {
        Node* x = getNode(h->right);
        h->right = x->left;
        x->left = getIndex(h);
        x->color = h->color;
        h->color = RED;
        x->size += (h->size + 1);
        uint64_t c = 0;
        for (size_t i = 0; i < rotationStats_.size(); i++) {
            uint64_t nc = (rotationStats_[i] & (0x1ULL << 63)) == 0 ? 0 : 1;
            rotationStats_[i] = (rotationStats_[i] << 1) ^ c;
            c = nc;
        }
        rotationStats_[0] = rotationStats_[0] ^ c;
        return x;
    }

    uint32_t getNodeSize(Node *n) {
        if (n == nullptr) {
            return 0;
        }

        return n->size;
    }

    Node* rotateRight(Node* h) {
        Node* x = getNode(h->left);
        h->left = x->right;
        x->right = getIndex(h);
        x->color = h->color;
        h->color = RED;
        h->size -= (x->size + 1);
        uint64_t c = 1;
        for (size_t i = 0; i < rotationStats_.size(); i++) {
            uint64_t nc = (rotationStats_[i] & (0x1ULL << 63)) == 0 ? 0 : 1;
            rotationStats_[i] = (rotationStats_[i] << 1) ^ c;
            c = nc;
        }
        rotationStats_[0] = rotationStats_[0] ^ c;
        return x;
    }

    void flipColor(Node* h) {
        h->color = !h->color;
        Node* left = getNode(h->left);
        left->color = !left->color;
        Node* right = getNode(h->right);
        right->color = !right->color;
    }

    Node* getNode(uint32_t idx) {
        if (idx == NULL_IDX) {
            return nullptr;
        }
        return (Node *)(arenaBase_ + sizeof (Node) * idx);
    }

    uint32_t getIndex(Node *n) {
        if (n == nullptr) {
            return NULL_IDX;
        }

        return ((uintptr_t)n - arenaBase_) / sizeof (Node);
    }

    Node* newNode(uint64_t value) {
        CHECK(!freeList_.empty());

        uint32_t idx = freeList_.back();
        freeList_.pop_back();
        Node* node = getNode(idx);
        node->left = NULL_IDX;
        node->right = NULL_IDX;
        node->value = value;
        node->color = RED;
        node->size = 0;
        return node;
    }

    bool isRed(uint32_t idx) {
        if (idx == NULL_IDX) {
            return false;
        }

        return getNode(idx)->color;
    }

    Node* insert(Node* h, T value) {
        if (h == nullptr) {
            return newNode(value);
        }

        if (value == h->value) {
            return nullptr;
        } else if (value < h->value) {
            Node* left = insert(getNode(h->left), value);
            if (left == nullptr) {
                return nullptr;
            }
            h->left = getIndex(left);
            h->size++;
        } else {
            Node* right = insert(getNode(h->right), value);
            if (right == nullptr) {
                return nullptr;
            }
            h->right = getIndex(right);
        }

        if (isRed(h->right) && !isRed(h->left)) {
            h = rotateLeft(h);
        }

        if (isRed(h->left) && isRed(getNode(h->left)->left)) {
            h = rotateRight(h);
        }

        if (isRed(h->left) && isRed(h->right)) {
            flipColor(h);
        }
        return h;
    }

    Node* fixUp(Node* h) {
        if (isRed(h->right)) {
            h = rotateLeft(h);
        }
        if (isRed(h->left) && isRed(getNode(h->left)->left)) {
            h = rotateRight(h);
        }
        if (isRed(h->left) && isRed(h->right)) {
            flipColor(h);
        }
        return h;
    }

    Node* moveRedLeft(Node* h) {
        flipColor(h);
        Node* right = getNode(h->right);
        if (isRed(right->left)) {
            h->right = getIndex(rotateRight(right));
            h = rotateLeft(h);
            flipColor(h);
        }
        return h;
    }

    Node* moveRedRight(Node* h) {
        flipColor(h);
        if (isRed(getNode(h->left)->left)) {
            h = rotateRight(h);
            flipColor(h);
        }
        return h;
    }

    Node* getMinNode(Node* h) {
        while (h->left != NULL_IDX) {
            h = getNode(h->left);
        }
        return h;
    }

    Node* removeMin(Node* h) {
        if (h->left == NULL_IDX) {
            freeList_.push_back(getIndex(h));
            return nullptr;
        }

        if (!isRed(h->left) && !isRed(getNode(h->left)->left)) {
            h = moveRedLeft(h);
        }

        h->left = getIndex(removeMin(getNode(h->left)));
        h->size--;
        return fixUp(h);
    }

    Node* erase(Node* h, T value) {
        CHECK(h != nullptr);

        if (value < h->value) {
            if (!isRed(h->left) && !isRed(getNode(h->left)->left)) {
                h = moveRedLeft(h);
            }
            h->left = getIndex(erase(getNode(h->left), value));
            h->size--;
        } else {
            if (isRed(h->left)) {
                h = rotateRight(h);
            }
            if (h->value == value && h->right == NULL_IDX) {
                freeList_.push_back(getIndex(h));
                return nullptr;
            }
            if (!isRed(h->right) && !isRed(getNode(h->right)->left)) {
                h = moveRedRight(h);
            }
            if (h->value == value) {
                Node* minNode = getMinNode(getNode(h->right));
                h->value = minNode->value;
                h->right = getIndex(removeMin(getNode(h->right)));
            } else {
                h->right = getIndex(erase(getNode(h->right), value));
            }
        }

        return fixUp(h);
    }

    Node* eraseAt(Node* h, uint32_t p, T& value) {
        CHECK(h != nullptr);

        if (p < h->size) {
            if (!isRed(h->left) && !isRed(getNode(h->left)->left)) {
                h = moveRedLeft(h);
            }
            h->left = getIndex(eraseAt(getNode(h->left), p, value));
            h->size--;
        } else {
            if (isRed(h->left)) {
                h = rotateRight(h);
            }
            if (p == h->size && h->right == NULL_IDX) {
                value = h->value;
                freeList_.push_back(getIndex(h));
                return nullptr;
            }
            if (!isRed(h->right) && !isRed(getNode(h->right)->left)) {
                h = moveRedRight(h);
            }
            if (h->size == p) {
                value = h->value;
                Node* minNode = getMinNode(getNode(h->right));
                h->value = minNode->value;
                h->right = getIndex(removeMin(getNode(h->right)));
            } else {
                h->right = getIndex(eraseAt(getNode(h->right), p - h->size - 1, value));
            }
        }

        return fixUp(h);
    }

    bool isBST(Node* x, T* min, T* max) {
        if (x == nullptr) {
            return true;
        }

        if (min != nullptr && x->value <= *min) {
            return false;
        }

        if (max != nullptr && x->value >= *max) {
            return false;
        }

        return isBST(getNode(x->left), min, &x->value) &&
               isBST(getNode(x->right), &x->value, max);
    }

    bool isSizeConsistent(Node* x, uint32_t& sum) {
        if (x == nullptr) {
            sum = 0;
            return true;
        }

        uint32_t left_sum, right_sum;
        if (!isSizeConsistent(getNode(x->left), left_sum)) {
            return false;
        }

        if (x->size != left_sum) {
            return false;
        }

        if (!isSizeConsistent(getNode(x->right), right_sum)) {
            return false;
        }

        sum = left_sum + right_sum + 1;
        return true;
    }

    bool is23(Node* x) {
        if (x == nullptr) {
            return true;
        }

        if (isRed(x->right)) {
            return false;
        }

        if (x != getNode(root_) && x->color == RED && isRed(x->left)) {
            return false;
        }

        return is23(getNode(x->left)) && is23(getNode(x->right));
    }

    bool isBalanced() {
        uint32_t black = 0;
        Node* x = root_;
        while (x != nullptr) {
            if (!isRed(x)) black++;
            x = x.left;
        }

        return isBalanced(root_, black);
    }

    bool isBalanced(Node* x, int black) {
        if (x == nullptr) {
            return black == 0;
        }

        if (!isRed(x)) {
            black--;
        }

        return isBalanced(x->left, black) && isBalanced(x->right, black);
    }
};

} // quarkchain
} // org
