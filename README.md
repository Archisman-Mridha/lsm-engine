## Learnings

- `bytes::Bytes` is similar to `Arc<[u8]>`.

- Use [`parking_lot::RwLock`](https://amanieu.github.io/parking_lot/parking_lot/struct.RwLock.html) instead of the RwLock implementation provided by the standard library, since it **considers fairness** and **automatically unlocks a poisoned lock**.

## REFERENCEs

- [7. Randomization: Skip Lists](https://www.youtube.com/watch?v=2g9OSRKJuzM)

- [11.1 Skip List | Complete Introduction | All Operations with Examples | Advanced Data Structures](https://www.youtube.com/watch?v=FMYKVdWywcg)

- [11.2 Skip List | Time and Space Complexity Computation | Part 2 | Probability Analysis](https://www.youtube.com/watch?v=RigE4QjdNks)

- [Lock-Free to Wait-Free Simulation in Rust](https://www.youtube.com/watch?v=Bw8-vvtA-E8)
