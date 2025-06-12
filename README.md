## Learnings

- `bytes::Bytes` is similar to `Arc<[u8]>`.

- Use [`parking_lot::RwLock`](https://amanieu.github.io/parking_lot/parking_lot/struct.RwLock.html) instead of the RwLock implementation provided by the standard library, since it **considers fairness** and **automatically unlocks a poisoned lock**.

## REFERENCEs

- [7. Randomization: Skip Lists](https://www.youtube.com/watch?v=2g9OSRKJuzM)

- [11.1 Skip List | Complete Introduction | All Operations with Examples | Advanced Data Structures](https://www.youtube.com/watch?v=FMYKVdWywcg)

- [11.2 Skip List | Time and Space Complexity Computation | Part 2 | Probability Analysis](https://www.youtube.com/watch?v=RigE4QjdNks)

- [Lock-Free to Wait-Free Simulation in Rust](https://www.youtube.com/watch?v=Bw8-vvtA-E8)

- [Subtyping and Variance](https://doc.rust-lang.org/nomicon/subtyping.html)

- [How to get &\[u8\] or Vec\<u8\> from bytes:Bytes?](https://www.reddit.com/r/rust/comments/1dndm2n/how_to_get_u8_or_vecu8_from_bytesbytes/)

- [What Is a Binary Heap?](https://www.youtube.com/watch?v=AE5I0xACpZs)

- [Why GATs?](https://rust-lang.github.io/generic-associated-types-initiative/explainer/motivation.html)
