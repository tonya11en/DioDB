DiverDB: A persistent key-value store that's like... ok
-----------------------------------------------------
I just want to learn about building [LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree)-based key-value stores, [gRPC](https://grpc.io/), and [SPDK](https://spdk.io/). The project is called DiverDB because I was listening to Holy Diver by Dio when I started it.

### Requirements
To build this, you'll want to have a modern version of [Bazel](https://docs.bazel.build/versions/master/install.html) running on some flavor of Linux.

### Building and Testing
To build the DiverDB server binary, run:
```
bazel build //src:diodb
```
The binary should then show up in your generated `bazel-bin/src` directory.

Running unit tests is as simple as running:
```
bazel test //...
```

### Using DiverDB
There are only silly testing endpoints implemented right now, so I'll probably make an `example` directory with a client that exercises the database as it's implemented. For now, it should suffice to have functioning unit tests while the base components of the database are under development.
