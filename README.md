Digipost Cache
===================================

Library containing caching functionality used by Digipost projects.

## Feature overview

### In-memory cache

The entry point for creating in-memory caches can be found in the
`no.digipost.cache2.inmemory` package, which wraps the
[Caffeine](https://github.com/ben-manes/caffeine) cache implementation.
In addition to standard key-value cache, this library also offers a
specialized API for caching a shared single object
([```SingleCached<V>```](https://github.com/digipost/digipost-cache/blob/master/src/main/java/no/digipost/cache/inmemory/SingleCached.java)).


### Fallback

Digipost Cache also supports a fallback-concept, currently implemented as
storing cache values on the file system. In the event of a cache value
[```Loader```](https://github.com/digipost/digipost-cache/blob/master/src/main/java/no/digipost/cache/loader/Loader.java)
failing (e.g. if it resolves the value over the network or other error-prone I/O),
the last value it successfully loaded will instead be read from a disk-file.
Other ways to store and read values for fallback can be implemented by implementing
[```FallbackKeeper```](https://github.com/digipost/digipost-cache/blob/master/src/main/java/no/digipost/cache/fallback/FallbackKeeper.java)
for storing a value for fallback, and the already mentioned ```Loader``` for
loading an already stored fallback value.


## Maven dependency

Latest version: [![Digipost Cache in Maven Central Repository](https://maven-badges.herokuapp.com/maven-central/no.digipost/digipost-cache/badge.svg?style=flat-square)](https://maven-badges.herokuapp.com/maven-central/no.digipost/digipost-cache)

```xml
<dependency>
    <groupId>no.digipost</groupId>
    <artifactId>digipost-cache</artifactId>
    <version>2.0</version>
</dependency>
```
