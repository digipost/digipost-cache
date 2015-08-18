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
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.joda.time.Duration.standardMinutes;

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
	static final Duration LOCK_EXPIRES_AFTER = standardMinutes(10);

	private final Logger logger;
	private final FallbackFile.Resolver<K> cacheFileResolver;
	private final Marshaller<V> marshaller;
	private final Loader<K, V> cacheLoader;
	private final LockFiles lockFiles;

	DiskStorageFallbackLoader(FallbackFile.Resolver<K> cacheFileResolver, Loader<K, V> cacheLoader, Marshaller<V> marshaller) {
		this(LoggerFactory.getLogger(DiskStorageFallbackLoader.class), cacheFileResolver, cacheLoader, marshaller);
	}

	DiskStorageFallbackLoader(Logger logger, FallbackFile.Resolver<K> cacheFileResolver, Loader<K, V> cacheLoader, Marshaller<V> marshaller) {
		this.logger = logger;
		this.cacheFileResolver = cacheFileResolver;
		this.marshaller = marshaller;
		this.cacheLoader = cacheLoader;
		this.lockFiles = new LockFiles(LOCK_EXPIRES_AFTER, logger);
	}

	@Override
	public V load(K key) throws Exception {

		FallbackFile fallbackFile = cacheFileResolver.resolveFor(key);
		try {
			final V newCacheContent = cacheLoader.load(key);

			try {
				tryWriteToDiskIfLock(fallbackFile, newCacheContent);
			} catch (RuntimeException e) {
				logger.error("Failed to write cache-value to disk (for use as fallback). Will still return value from wrapped cache-loader.", e);
			}

			return newCacheContent;

		} catch (RuntimeException originalException) {
			logger.warn("Failed to load cache-value from wrapped cache-loader because {}: '{}'. Attempting to load from disk as fallback mechanism. Enable debug-level to see stacktrace.", originalException.getClass().getSimpleName(), originalException.getMessage());
			if (logger.isDebugEnabled()) {
				logger.debug("Stacktrace for failing cache loading:", originalException);
			}

			try (InputStream fallbackContent = fallbackFile.read()) {
				return marshaller.read(fallbackContent);
			} catch (Exception exceptionWhileReadingFromDisk) {
				logger.error("Failed to read cache-value from disk. " + exceptionWhileReadingFromDisk.getClass().getSimpleName() + ": " + exceptionWhileReadingFromDisk.getMessage(), exceptionWhileReadingFromDisk);
				originalException.addSuppressed(exceptionWhileReadingFromDisk);
				throw originalException;
			}
		}
	}

	private void tryWriteToDiskIfLock(final FallbackFile fallbackFile, final V newCacheContent) {

		lockFiles.runIfLock(fallbackFile.lock, new Runnable() {
			@Override
			public void run() {

				try (OutputStream out = fallbackFile.write()) {

					marshaller.write(newCacheContent, out);

				} catch (IOException e) {
					logger.error("Failed to write cache-value to file " + fallbackFile + ". Will not attempt write until next time cache expires and loads from wrapped cache-loader.", e);
					return;
				}

			}
		});
	}




}
