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

	static final Logger LOG = LoggerFactory.getLogger(DiskStorageFallbackLoader.class);

	private final FallbackFile.Resolver<K> cacheFileResolver;
	private final Marshaller<V> marshaller;
	private final Loader<? super K, V> cacheLoader;


	DiskStorageFallbackLoader(FallbackFile.Resolver<K> cacheFileResolver, Loader<? super K, V> cacheLoader, Marshaller<V> marshaller) {
		this.cacheFileResolver = cacheFileResolver;
		this.marshaller = marshaller;
		this.cacheLoader = cacheLoader;
	}

	@Override
	public V load(K key) throws Exception {

		FallbackFile fallbackFile = cacheFileResolver.resolveFor(key);
		try {
			final V newCacheContent = cacheLoader.load(key);

			try {
				tryWriteToDiskIfLock(fallbackFile, newCacheContent);
			} catch (RuntimeException e) {
				LOG.error("Failed to write cache-value to disk (for use as fallback). "
						+ "Will still return value from wrapped cache-loader.", e);
			}

			return newCacheContent;

		} catch (RuntimeException originalException) {
			LOG.warn("Failed to load cache-value from wrapped cache-loader because {}: '{}'. "
				      + "Attempting to load from disk as fallback mechanism. Enable debug-level to see stacktrace.",
				      originalException.getClass().getSimpleName(), originalException.getMessage());
			if (LOG.isDebugEnabled()) {
				LOG.debug("Stacktrace for failing cache loading:", originalException);
			}

			try (InputStream fallbackContent = fallbackFile.read()) {
				return marshaller.read(fallbackContent);
			} catch (Exception exceptionWhileReadingFromDisk) {
				LOG.error("Failed to read cache-value from disk. " +
				          exceptionWhileReadingFromDisk.getClass().getSimpleName() + ": " +
						  exceptionWhileReadingFromDisk.getMessage(), exceptionWhileReadingFromDisk);
				originalException.addSuppressed(exceptionWhileReadingFromDisk);
				throw originalException;
			}
		}
	}

	private void tryWriteToDiskIfLock(final FallbackFile fallbackFile, final V newCacheContent) {

		fallbackFile.lock.runIfLock(new Runnable() {
			@Override
			public void run() {

				try (OutputStream out = fallbackFile.write()) {

					marshaller.write(newCacheContent, out);

				} catch (IOException e) {
					LOG.error("Failed to write cache-value to file {}. Will not attempt write "
	    					   + "until next time cache expires and loads from wrapped cache-loader.",
	    					   fallbackFile, e);
					return;
				}
			}
		});
	}


}
