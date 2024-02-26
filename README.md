## DataFusion Federation

[![crates.io](https://img.shields.io/crates/v/datafusion-federation.svg)](https://crates.io/crates/datafusion-federation)
[![docs.rs](https://docs.rs/datafusion-federation/badge.svg)](https://docs.rs/datafusion-federation)

The goal of this repo is to allow [DataFusion](https://github.com/apache/arrow-datafusion) to resolve queries across remote query engines while pushing down as much compute as possible down.

Check out [the examples](./examples/) to get a feel for how it works.

Potential use-cases:

- Querying across SQLite, MySQL, PostgreSQL, ...
- Pushing down SQL or [Substrait](https://substrait.io/) plans.
- DataFusion -> Flight SQL -> DataFusion
- ..

#### Status

The project is in alpha status. Contributions welcome; land a PR = commit access.

- [Docs (release)](https://docs.rs/datafusion-federation)
- [Docs (main)](https://datafusion-contrib.github.io/datafusion-federation/)
