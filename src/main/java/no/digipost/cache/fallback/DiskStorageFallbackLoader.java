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
package no.digipost.cache.fallback;

import no.digipost.cache.loader.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Cache-loader wrapper that persists the last cache-value to disk to be used as fallback-mechanism if
 * the primary cache-loader fails.
 *
 * Should the write-operation start to fail, the cache-value on disk may become out-dated. Currently,
 * this content is never considered to be expired, and it will continue to return a "stale" copy of the cache
 * until the write-opertation succeeds (thus overwriting the stale data).
 *
 */
public class DiskStorageFallbackLoader<K, V> implements Loader<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(DiskStorageFallbackLoader.class);

	private final FallbackFile.Resolver<K> cacheFileResolver;
	private final Marshaller<V> marshaller;
	private final Loader<? super K, V> cacheLoader;
	private final FallbackWriteFailedHandler<? super K, ? super V> fallbackWriterFailedHandler;


	DiskStorageFallbackLoader(
			FallbackFile.Resolver<K> cacheFileResolver, Loader<? super K, V> cacheLoader,
			Marshaller<V> marshaller, FallbackWriteFailedHandler<? super K, ? super V> fallbackWriteFailedHandler) {

		this.cacheFileResolver = cacheFileResolver;
		this.marshaller = marshaller;
		this.cacheLoader = cacheLoader;
		this.fallbackWriterFailedHandler = fallbackWriteFailedHandler;
	}

	@Override
	public V load(K key) throws Exception {
		FallbackFile fallbackFile = cacheFileResolver.resolveFor(key);
		V newCacheContent;
		try {
			newCacheContent = cacheLoader.load(key);
		} catch (Exception loaderFailedException) {
			return tryRecoverFailingLoader(fallbackFile, loaderFailedException);
		}

		try {
			tryWriteFallback(newCacheContent, fallbackFile);
		} catch (Exception e) {
			fallbackWriterFailedHandler.handle(key, newCacheContent, e);
		}
		return newCacheContent;
	}


	private V tryRecoverFailingLoader(FallbackFile fallbackFile, Exception loaderFailedException) throws Exception {
		LOG.warn("Failed to load cache-value from wrapped cache-loader because {}: '{}'. "
			      + "Attempting to load from disk as fallback mechanism. Enable debug-level to see stacktrace.",
			      loaderFailedException.getClass().getSimpleName(), loaderFailedException.getMessage());
		if (LOG.isDebugEnabled()) {
			LOG.debug("Stacktrace for failing cache loading:", loaderFailedException);
		}

		try (InputStream fallbackContent = fallbackFile.read()) {
			return marshaller.read(fallbackContent);
		} catch (IOException fallbackReadException) {
			loaderFailedException.addSuppressed(fallbackReadException);
			LOG.warn("Regular cache value loading failed because {}: '{}', and attempt to read " +
					 "fallback value from disk also failed because {}: '{}'",
					 loaderFailedException.getClass().getSimpleName(), loaderFailedException.getMessage(),
			         fallbackReadException.getClass().getSimpleName(), fallbackReadException.getMessage());
			throw loaderFailedException;
		}
	}



	private void tryWriteFallback(final V newFallbackContent, final FallbackFile fallbackFile) throws IOException {
		fallbackFile.lock.runIfLock(new ThrowingRunnable<IOException>() {
			@Override
			public void run() throws IOException {
				try (OutputStream out = fallbackFile.write()) {
					marshaller.write(newFallbackContent, out);
				}
			}
		});
	}

}
