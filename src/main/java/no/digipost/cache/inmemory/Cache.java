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

import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import no.digipost.cache.loader.Callables;
import no.digipost.cache.loader.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static no.digipost.cache.inmemory.CacheConfig.jodaTicker;
import static no.digipost.cache.inmemory.CacheConfig.logRemoval;
import static no.motif.Iterate.on;

/**
 * Wrapper around {@link com.google.common.cache.Cache} from the Guava
 * library.
 */
public final class Cache<K, V> {

	static final Logger LOG = LoggerFactory.getLogger(Cache.class);

	private com.google.common.cache.Cache<K, V> guavaCache;
	private String name;

	public Cache(CacheConfig ... configurers) {
		this(asList(configurers));
	}

	public Cache(String name, CacheConfig ... configurers) {
		this(name, asList(configurers));
	}

	public Cache(Iterable<CacheConfig> configurers) {
		this("cache-" + UUID.randomUUID(), configurers);
	}

	public Cache(String name, Iterable<CacheConfig> configurers) {
		LOG.info("Creating new cache: {}", name);
		this.guavaCache = on(configurers).append(jodaTicker).append(logRemoval).reduce(CacheBuilder.newBuilder(), ConfiguresGuavaCache.applyConfiguration).build();
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
		try {
			return guavaCache.get(key, new Callable<V>() {
				@Override
				public V call() throws Exception {
					LOG.debug("{} resolving value for key {}", name, key);
					V value = valueResolver.load(key);
					LOG.info("Loaded '{}' into '{}' cache for key '{}'", value, name, key);
					return value;
				}
			});
		} catch (ExecutionException | UncheckedExecutionException e) {
			final Throwable cause = e.getCause();
			if (cause instanceof RuntimeException) {
				throw (RuntimeException) cause;
			} else {
				throw new RuntimeException(getCauseDescription(cause), cause);
			}
		} catch (ExecutionError e) {
			final Throwable cause = e.getCause();
			throw new Error(getCauseDescription(cause), cause);
		}
	}

	private String getCauseDescription(final Throwable cause) {
		return cause.getClass() + ": " + cause.getMessage();
	}

	public void invalidateAll() {
		LOG.debug("Invalidating all in {} cache", name);
		guavaCache.invalidateAll();
	}

	@SafeVarargs
	public final void invalidate(K ... keys) {
		invalidate(asList(keys));
	}

	public void invalidate(Iterable<? extends K> keys) {
		LOG.debug("Invalidating specific keys in {} cache", name);
		guavaCache.invalidateAll(keys);
	}

	public CacheStats getCacheStats() {
		return CacheStats.fromGuava(guavaCache.stats());
	}

}
