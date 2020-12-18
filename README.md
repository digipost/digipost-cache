# Digipost Cache

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/no.digipost/digipost-cache/badge.svg?style=flat-square)](https://maven-badges.herokuapp.com/maven-central/no.digipost/digipost-cache)
![](https://github.com/digipost/digipost-cache/workflows/Build%20and%20deploy/badge.svg)
[![License](https://img.shields.io/badge/license-Apache%202-blue)](https://github.com/digipost/digipost-cache/blob/main/LICENCE)

Library containing caching functionality used by Digipost projects.

## Feature overview

### In-memory cache

The entry point for creating in-memory caches can be found in the
`no.digipost.cache2.inmemory` package, which wraps the
[Caffeine](https://github.com/ben-manes/caffeine) cache implementation.
In addition to standard key-value cache, this library also offers a
specialized API for caching a shared single object
([```SingleCached<V>```](src/main/java/no/digipost/cache2/inmemory/SingleCached.java)).


### Fallback

Digipost Cache also supports a fallback-concept, currently implemented as
storing cache values on the file system. In the event of a cache value
[```Loader```](src/main/java/no/digipost/cache2/loader/Loader.java)
failing (e.g. if it resolves the value over the network or other error-prone I/O),
the last value it successfully loaded will instead be read from a disk-file.
Other ways to store and read values for fallback can be implemented by implementing
[```FallbackKeeper```](src/main/java/no/digipost/cache2/fallback/FallbackKeeper.java)
for storing a value for fallback, and the already mentioned ```Loader``` for
loading an already stored fallback value.
