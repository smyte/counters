# counters

A counts database written in C++ that speaks Redis protocol and replicates via Kafka.

## Building from source

* Ensure [Bazel](https://www.bazel.io/) is installed
* Check out the [smyte-db](https://github.com/smyte/smyte-db) repo
* Ensure your submodules are up-to-date: `git submodule update`
* Build the project: `bazel build -c opt counters`

## Running it

See `./bazel-bin/counters/counters --help` for help. More details are coming soon!
