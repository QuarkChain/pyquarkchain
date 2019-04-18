package main

import (
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "log"
    "math/rand"
    "os"
    "strconv"

    "github.com/ethereum/go-ethereum/trie"
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func newEmptySecure() *trie.SecureTrie {
    trie, _ := trie.NewSecure(common.Hash{}, trie.NewDatabase(memorydb.New()))
    return trie
}

func toHex(data []byte) []byte {
    dst := make([]byte, hex.EncodedLen(len(data)))
    hex.Encode(dst, data)
    return dst
}

func generateTestCase(size int, startValue int, updateRate int, seed int64) {
    var maxKey int = 0
    var key int
    random := rand.New(rand.NewSource(seed))
    trie := newEmptySecure()

    for i :=0; i < size; i++ {
        keyData := make([]byte, 4)
        if maxKey != 0 && random.Intn(100) < updateRate {
            key = random.Intn(maxKey)
        } else {
            key = maxKey
            maxKey ++
        }
        value := make([]byte, 32)
        _, _ = random.Read(value)
        binary.LittleEndian.PutUint32(keyData, uint32(key + startValue))
        trie.Update(keyData, value)
        root, _ := trie.Commit(nil)
        fmt.Printf("%d %s %s\n", key + startValue, toHex(value), toHex(root.Bytes()))
    }
}

func main() {
    size, err := strconv.Atoi(os.Args[1])
    if err != nil {
        log.Fatal(err)
    }
    startValue, err := strconv.Atoi(os.Args[2])
    if err != nil {
        log.Fatal(err)
    }
    updateRate, err := strconv.Atoi(os.Args[3])
    if err != nil {
        log.Fatal(err)
    }
    seed, err := strconv.Atoi(os.Args[4])
    if err != nil {
        log.Fatal(err)
    }
    generateTestCase(size, startValue, updateRate, int64(seed))
}