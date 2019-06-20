DioDB: A persistent key-value store that's like... ok
-----------------------------------------------------
I just want to learn about building [LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree)-based key-value stores, [gRPC](https://grpc.io/), and [SPDK](https://spdk.io/). The project is called DioDB because I was listening to Holy Diver by Dio when I started it.

### Status/Roadmap
Currently, there's only a memtable and sstable implementation without the components being tied together to actually store keys and values in a persistent manner. The operations necessary for garbage collection and proper I/O are implemented, but there is no table manager scheduling the operations. There is also no write-ahead log yet.

- [x] Memtable
- [x] SSTable
- [x] Threadpool
- [ ] Background compaction
- [ ] End-to-end integration test

Upon completion of a full integration test, I'd like to begin benchmarking and ripping out bottlenecks. I'll almost certainly need bloom filters (or some other AMQ structure) in front of the SSTables. Beyond that, I'd like to experiment with SPDK and measure the impact on performance. I'd also like to implement some kind of correctness test, but I don't know what I don't know about that area quite yet.

So much to get done!

### Requirements
To build this, you'll want to have a modern version of [Bazel](https://docs.bazel.build/versions/master/install.html) running on some flavor of Linux.

### Building and Testing
To build the DioDB server binary, run:
```
bazel build //src:diodb
```
The binary should then show up in your generated `bazel-bin/src` directory.

Running unit tests is as simple as running:
```
bazel test //...
```

### Using DioDB
There are only silly testing endpoints implemented right now, so I'll probably make an `example` directory with a client that exercises the database as it's implemented. For now, it should suffice to have functioning unit tests while the base components of the database are under development.
