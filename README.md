# PSync: Partial and Full Synchronization Library for NDN

![Latest version](https://img.shields.io/github/v/tag/named-data/PSync?label=Latest%20version)
![Language](https://img.shields.io/badge/C%2B%2B-17-blue)
[![CI](https://github.com/named-data/PSync/actions/workflows/ci.yml/badge.svg)](https://github.com/named-data/PSync/actions/workflows/ci.yml)
[![Docs](https://github.com/named-data/PSync/actions/workflows/docs.yml/badge.svg)](https://github.com/named-data/PSync/actions/workflows/docs.yml)

The PSync library implements the
[PSync protocol](https://named-data.net/wp-content/uploads/2017/05/scalable_name-based_data_synchronization.pdf).
It uses Invertible Bloom Lookup Table (IBLT), also known as Invertible Bloom Filter (IBF),
to represent the state of a producer in partial sync mode and the state of a node in full
sync mode. An IBF is a compact data structure where difference of two IBFs can be computed
efficiently. In partial sync, PSync uses a Bloom Filter to represent the subscription list
of the consumer.

PSync uses the [ndn-cxx](https://github.com/named-data/ndn-cxx) library.

## Installation

### Prerequisites

* [ndn-cxx and its dependencies](https://docs.named-data.net/ndn-cxx/current/INSTALL.html)

### Build

To build PSync from source:

```shell
./waf configure
./waf
sudo ./waf install
```

To build on memory constrained systems, please use `./waf -j1` instead of `./waf`. This
will disable parallel compilation.

If configured with tests (`./waf configure --with-tests`), the above commands will also
build a suite of unit tests that can be run with `./build/unit-tests`.

## Reporting bugs

Please submit any bug reports or feature requests to the
[PSync issue tracker](https://redmine.named-data.net/projects/psync/issues).

## Contributing

Contributions to PSync are greatly appreciated and can be made through our
[Gerrit code review site](https://gerrit.named-data.net/).
If you are new to the NDN software community, please read our [Contributor's Guide](
https://github.com/named-data/.github/blob/main/CONTRIBUTING.md) to get started.

## License

PSync is free software distributed under the GNU Lesser General Public License version 3.
See [`COPYING.md`](COPYING.md) and [`COPYING.lesser`](COPYING.lesser) for details.

PSync contains third-party software, licensed under the following licenses:

* The *C++ Bloom Filter Library* is licensed under the
  [MIT license](https://www.partow.net/programming/bloomfilter/index.html)
* *IBLT_Cplusplus* is licensed under the
  [MIT license](https://github.com/gavinandresen/IBLT_Cplusplus/blob/master/LICENSE)
* The *waf* build system is licensed under the [3-clause BSD license](waf)
