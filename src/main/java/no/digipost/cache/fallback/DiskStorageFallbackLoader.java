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
import java.nio.file.Files;

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
	private static final Logger LOG = LoggerFactory.getLogger(DiskStorageFallbackLoader.class);
	public static final Duration LOCK_EXPIRES_AFTER = standardMinutes(10);

	private final FallbackFile.Resolver<K> cacheFileResolver;
	private final Marshaller<V> marshaller;
	private final Loader<K, V> cacheLoader;


	DiskStorageFallbackLoader(FallbackFile.Resolver<K> cacheFileResolver, Loader<K, V> cacheLoader, Marshaller<V> marshaller) {
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
				LOG.warn("Failed to write cache-value to disk (for use as fallback). Will still return value from wrapped cache-loader.", e);
			}

			return newCacheContent;

		} catch (RuntimeException originalException) {
			LOG.warn("Failed to load cache-value from wrapped cache-loader because {}: '{}'. Attempting to load from disk as fallback mechanism. Enable debug-level to see stacktrace.", originalException.getClass().getSimpleName(), originalException.getMessage());
			if (LOG.isDebugEnabled()) {
				LOG.debug("Stacktrace for failing cache loading:", originalException);
			}

			try (InputStream fallbackContent = fallbackFile.read()) {
				return marshaller.read(fallbackContent);
			} catch (Exception exceptionWhileReadingFromDisk) {
				LOG.error("Failed to read cache-value from disk. " + exceptionWhileReadingFromDisk.getClass().getSimpleName() + ": " + exceptionWhileReadingFromDisk.getMessage(), exceptionWhileReadingFromDisk);
				originalException.addSuppressed(exceptionWhileReadingFromDisk);
				throw originalException;
			}
		}
	}

	private void tryWriteToDiskIfLock(FallbackFile fallbackFile, V newCacheContent) {

		// Check if lock is available. Removed expired locks.
		if (fallbackFile.isLocked()) {

			if (fallbackFile.lockedLongerAgoThan(LOCK_EXPIRES_AFTER)) {
				LOG.warn("Lock-file is considered to be expired since it is older than {}. Deleting it.", LOCK_EXPIRES_AFTER);
				if (!tryDeleteExpiredLockFile(fallbackFile)) {
					return;
				}
			} else {
				LOG.debug("Another process is updating the cache-value. Skipping write.");
				return;
			}
		}

		// Attempt to aquire lock
		try {
			LOG.trace("Creating lockfile");
			Files.createFile(fallbackFile.lock); // fails if lock-file exists
		} catch (IOException e) {
			LOG.debug("Failed to create lock-file. Likely means that another process created it. Skipping write.");
			// TODO: how do we detect continuous failing to create lock-file because of permission problems? will never warn currently..
			//       warn on outdated disk-fallback?
			return;
		}

		// Lock aquired. Update cache-value on disk
		try (OutputStream out = fallbackFile.write()) {

			marshaller.write(newCacheContent, out);

		} catch (IOException | RuntimeException e) {
			LOG.error("Failed to write cache-value to file " + fallbackFile + ". Will not attempt write until next time cache expires and loads from wrapped cache-loader.", e);
			return;
		} finally {
			fallbackFile.silentDeleteLockFile();
		}

	}


	private boolean tryDeleteExpiredLockFile(FallbackFile cacheFile) {
		try {
			LOG.trace("Deleting expired lockfile");
			final boolean wasDeleted = Files.deleteIfExists(cacheFile.lock);
			if (!wasDeleted) {
				LOG.info("Another process appears to have deleted the expired lock-file. Continuing.");
			}
			return true;

		} catch (IOException e) {
			LOG.error("Failed to delete lock-file. Lock-file will be deleted when the lock expires. In the meantime, no process will be able to aquire the lock.", e);
			return false;
		}
	}

}
