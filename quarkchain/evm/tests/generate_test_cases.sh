#!/bin/bash

go run gen_trie_tests.go 10 0 30 0 > test_case_0
go run gen_trie_tests.go 20 50 10 1 > test_case_1
go run gen_trie_tests.go 30 100 0 2 > test_case_2
go run gen_trie_tests.go 100 1000 50 3 > test_case_3
go run gen_trie_tests.go 200 2000 25 4 > test_case_4
go run gen_trie_tests.go 500 3000 75 5 > test_case_5
