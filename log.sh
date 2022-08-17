Running main() from gmock_main.cc
[==========] Running 1 test from 1 test suite.
[----------] Global test environment set-up.
[----------] 1 test from BPlusTreeTests
[ RUN      ] BPlusTreeTests.SplitTest
/home/sean/cmu15445/bustub_2020/test/storage/gradescope_b_plus_tree_checkpoint_1_test.cpp:60: Failure
Expected equality of these values:
  false
  tree.Insert(index_key, rid, transaction)
    Which is: true
/home/sean/cmu15445/bustub_2020/test/storage/gradescope_b_plus_tree_checkpoint_1_test.cpp:60: Failure
Expected equality of these values:
  false
  tree.Insert(index_key, rid, transaction)
    Which is: true
/home/sean/cmu15445/bustub_2020/test/storage/gradescope_b_plus_tree_checkpoint_1_test.cpp:60: Failure
Expected equality of these values:
  false
  tree.Insert(index_key, rid, transaction)
    Which is: true
/home/sean/cmu15445/bustub_2020/test/storage/gradescope_b_plus_tree_checkpoint_1_test.cpp:60: Failure
Expected equality of these values:
  false
  tree.Insert(index_key, rid, transaction)
    Which is: true
/home/sean/cmu15445/bustub_2020/test/storage/gradescope_b_plus_tree_checkpoint_1_test.cpp:60: Failure
Expected equality of these values:
  false
  tree.Insert(index_key, rid, transaction)
    Which is: true
/home/sean/cmu15445/bustub_2020/test/storage/gradescope_b_plus_tree_checkpoint_1_test.cpp:65: Failure
Expected: (nullptr) != (leaf_node), actual: (nullptr) vs NULL
[  FAILED  ] BPlusTreeTests.SplitTest (1 ms)
[ DISABLED ] BPlusTreeTests.DISABLED_InsertTest1
[ DISABLED ] BPlusTreeTests.DISABLED_InsertTest2
[ DISABLED ] BPlusTreeTests.DISABLED_ScaleTest
[----------] 1 test from BPlusTreeTests (1 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (2 ms total)
[  PASSED  ] 0 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] BPlusTreeTests.SplitTest

 1 FAILED TEST
  YOU HAVE 3 DISABLED TESTS

