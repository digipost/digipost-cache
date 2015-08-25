/**
 * Copyright (C) Posten Norge AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.digipost.cache.inmemory;

import no.digipost.cache.loader.Callables;
import no.digipost.cache.loader.Loader;

import java.util.UUID;
import java.util.concurrent.Callable;

import static java.util.Arrays.asList;
import static no.digipost.cache.inmemory.CacheConfig.initialCapacity;
import static no.digipost.cache.inmemory.CacheConfig.maximumSize;
import static no.motif.Iterate.on;

/**
 * Adapts the {@link Cache} to be more appropriate for single value caches.
 *
 * @param <V> The type of the single element contained in this cache.
 */
public final class SingleCached<V> {

	private final Cache<String, V> cache;
	private final Loader<? super String, V> resolver;

	private String key;

	public SingleCached(Callable<V> resolver, CacheConfig ... configs) {
		this(Callables.toLoader(resolver), configs);
	}

	public SingleCached(Loader<? super String, V> resolver, CacheConfig ... configs) {
		this(resolver, asList(configs));
	}

	public SingleCached(String name, Callable<V> resolver, CacheConfig ... configs) {
		this(name, Callables.toLoader(resolver), configs);
	}

	public SingleCached(String name, Loader<? super String, V> resolver, CacheConfig ... configs) {
		this(name, resolver, asList(configs));
	}

	public SingleCached(Callable<V> resolver, Iterable<CacheConfig> configs) {
		this(Callables.toLoader(resolver), configs);
	}

	public SingleCached(Loader<? super String, V> resolver, Iterable<CacheConfig> configs) {
		this("single-value-cache-" + UUID.randomUUID(), resolver, configs);
	}

	public SingleCached(String name, Callable<V> resolver, Iterable<CacheConfig> configs) {
		this(name, Callables.toLoader(resolver), configs);
	}

	public SingleCached(String name, Loader<? super String, V> resolver, Iterable<CacheConfig> configs) {
		this.resolver = resolver;
		this.cache = new Cache<String, V>(name, on(configs).append(initialCapacity(1)).append(maximumSize(1)));
		this.key = name + "-cachekey";
	}

	public V get() {
        return cache.get(key, resolver);
	}

	public void invalidate() {
		cache.invalidateAll();
	}

}
