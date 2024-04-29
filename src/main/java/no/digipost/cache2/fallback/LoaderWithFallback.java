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
package no.digipost.cache2.fallback;

import no.digipost.cache2.loader.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Loader} which wraps an extra loader which will be tried as fallback if the primary loader fails.
 * The fallback loader are usually paired with a {@link FallbackKeeper} which keeps successfully loaded
 * values for the fallback loader to retrieve in the event of a failing primary loader.
 * <p>
 * Should the write-operation start to fail, the fallback-value may become out-dated. Currently,
 * this content is never considered to be expired, and it will continue to return a "stale" copy of the cache
 * until the {@link FallbackKeeper#keep(Object, Object) keep}-operation succeeds (thus overwriting the stale data).
 *
 */
public class LoaderWithFallback<K, V> implements Loader<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(LoaderWithFallback.class);

	private final Loader<? super K, V> cacheLoader;
	private final Loader<? super K, V> fallbackLoader;
	private final FallbackKeeper<? super K, ? super V> fallbackKeeper;
	private final FallbackKeeperFailedHandler<? super K, ? super V> fallbackWriterFailedHandler;


	public LoaderWithFallback(
			Loader<? super K, V> cacheLoader,
			Loader<? super K, V> fallbackLoader,
			FallbackKeeperFailedHandler<? super K, ? super V> fallbackWriteFailedHandler) {
		this(cacheLoader, fallbackLoader, FallbackKeeper.NO_KEEPING, fallbackWriteFailedHandler);

	}

	public LoaderWithFallback(
			Loader<? super K, V> cacheLoader,
			Loader<? super K, V> fallbackLoader,
			FallbackKeeper<? super K, ? super V> fallbackKeeper,
			FallbackKeeperFailedHandler<? super K, ? super V> fallbackWriteFailedHandler) {

		this.cacheLoader = cacheLoader;
		this.fallbackLoader = fallbackLoader;
		this.fallbackKeeper = fallbackKeeper;
		this.fallbackWriterFailedHandler = fallbackWriteFailedHandler;
	}

	@Override
	public V load(K key) throws Exception {
		V newCacheContent;
		try {
			newCacheContent = cacheLoader.load(key);
		} catch (Exception loaderFailedException) {
			return tryRecoverFailingLoader(key, loaderFailedException);
		}

		try {
			fallbackKeeper.keep(key, newCacheContent);
		} catch (Exception e) {
			fallbackWriterFailedHandler.handle(key, newCacheContent, e);
		}
		return newCacheContent;
	}


	private V tryRecoverFailingLoader(K key, Exception loaderFailedException) throws Exception {
		LOG.warn("Failed to load cache-value from wrapped cache-loader because {}: '{}'. "
			      + "Attempting to load from disk as fallback mechanism. Enable debug-level to see stacktrace.",
			      loaderFailedException.getClass().getSimpleName(), loaderFailedException.getMessage());
		if (LOG.isDebugEnabled()) {
			LOG.debug("Stacktrace for failing cache loading:", loaderFailedException);
		}

		try {
			return fallbackLoader.load(key);
		} catch (Exception fallbackLoadException) {
			loaderFailedException.addSuppressed(fallbackLoadException);
			LOG.warn("Regular cache value loading failed because {}: '{}', and attempt to read " +
					 "fallback value from disk also failed because {}: '{}'",
					 loaderFailedException.getClass().getSimpleName(), loaderFailedException.getMessage(),
			         fallbackLoadException.getClass().getSimpleName(), fallbackLoadException.getMessage());
			throw loaderFailedException;
		}
	}

}
