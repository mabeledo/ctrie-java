# ctrie-java
A Java implementation of the concurrent trie described by [Prokopec, Bronson, Bagwell, and Odersky](http://aleksandar-prokopec.com/resources/docs/ctries-snapshot.pdf).

According to Wikipedia:

> A concurrent hash-trie or Ctrie is a concurrent thread-safe lock-free implementation of a hash array mapped trie. It is used to implement the concurrent map abstraction. It has particularly scalable concurrent insert and remove operations and is memory-efficient. It is the first known concurrent data-structure that supports O(1), atomic, lock-free snapshots.

This is work in progress.

- ✅ Lookup
- ✅ Insertion
- ❌ Removal
- ❌ Snapshots (tests pending)
- ❌ Performance tests
- ❌ Concurrency tests
