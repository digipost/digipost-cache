/*
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
package no.digipost.cache2.inmemory;

import com.github.benmanes.caffeine.cache.Caffeine;
import no.digipost.cache2.loader.Callables;
import no.digipost.cache2.loader.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static java.util.Arrays.asList;
import static no.digipost.cache2.inmemory.CacheConfig.logRemoval;
import static no.digipost.cache2.inmemory.CacheConfig.systemClockTicker;

/**
 * Wrapper around {@link com.github.benmanes.caffeine.cache.Cache} from the
 * <a href="https://github.com/ben-manes/caffeine">Caffeine cache library</a>.
 */
public final class Cache<K, V> {

    public static <K, V> Cache<K, V> create(CacheConfig ... configurers) {
        return create(asList(configurers));
    }

    public static <K, V> Cache<K, V> create(String name, CacheConfig ... configurers) {
        return create(name, asList(configurers));
    }

    public static <K, V> Cache<K, V> create(List<CacheConfig> configurers) {
        return create("cache-" + UUID.randomUUID(), configurers);
    }

    public static <K, V> Cache<K, V> create(String name, List<CacheConfig> configurers) {
        List<CacheConfig> allConfigurers = new ArrayList<>();
        allConfigurers.add(systemClockTicker);
        allConfigurers.add(logRemoval);
        allConfigurers.addAll(configurers);
        return new Cache<>(name, allConfigurers);
    }

	static final Logger LOG = LoggerFactory.getLogger(Cache.class);

	private com.github.benmanes.caffeine.cache.Cache<K, V> caffeineCache;
	private String name;

	Cache(String name, List<CacheConfig> configurers) {
		LOG.info("Creating new cache: {}", name);
		Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
		configurers.forEach(configurer -> configurer.configure(cacheBuilder));

		this.caffeineCache = cacheBuilder.build();
		this.name = name;
	}


	/**
	 * Retrieve a possibly cached value from the cache, or use the provided
	 * {@code valueResolver} if the cache does not contain any value for the given
	 * key.
	 */
	public V get(final K key, final Callable<V> valueResolver) {
		return get(key, Callables.toLoader(valueResolver));
	}

	/**
	 * Retrieve a possibly cached value from the cache, or use the provided
	 * {@link Loader valueResolver} if the cache does not contain any value for the given
	 * key.
	 */
	public V get(final K key, final Loader<? super K, V> valueResolver) {
			return caffeineCache.get(key, k -> {
				LOG.debug("{} resolving value for key {}", name, k);
				V value;
				try {
					value = valueResolver.load(k);
				} catch (RuntimeException runtimeException) {
					throw runtimeException;
				} catch (Exception e) {
					throw new RuntimeException(getCauseDescription(e), e);
				}
				LOG.info("Loaded '{}' into '{}' cache for key '{}'", value, name, k);
				return value;
			});
	}

	private String getCauseDescription(final Throwable cause) {
		return cause.getClass().getSimpleName() + ": " + cause.getMessage();
	}

	public void invalidateAll() {
		LOG.debug("Invalidating all in {} cache", name);
		caffeineCache.invalidateAll();
	}

	@SafeVarargs
	public final void invalidate(K ... keys) {
		invalidate(asList(keys));
	}

	public void invalidate(Iterable<? extends K> keys) {
		LOG.debug("Invalidating specific keys in {} cache", name);
		caffeineCache.invalidateAll(keys);
	}

	public CacheStats getCacheStats() {
		return CacheStats.fromCaffeineStats(caffeineCache.stats());
	}

}
