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

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Random;
import java.util.concurrent.Callable;

import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardMinutes;

/**
 * Cache-loader wrapper that persists the last cache-content to disk to be used as fallback-mechanism if
 * the primary cache-loader fails.
 *
 * Should the write-operation start to fail, the cache-content on disk may become out-dated. Currently,
 * this content is never considered to be expired, and it will continue to return a "stale" copy of the cache
 * until the write-opertation succeeds (thus overwriting the stale data).
 *
 */
public class DiskStorageFallback<T> implements Callable<T> {
	private static final Logger LOG = LoggerFactory.getLogger(DiskStorageFallback.class);
	public static final Duration LOCK_EXPIRES_AFTER = standardMinutes(10);

	final Path lockFile;
	private final Path cacheFile;
	private final Marshaller<T> marshaller;
	private final Callable<T> cacheLoader;
	private final Random random = new Random();


	public DiskStorageFallback(Path cacheFile, Callable<T> cacheLoader, Marshaller<T> marshaller) {
		this.cacheFile = cacheFile;
		this.marshaller = marshaller;
		this.lockFile = cacheFile.resolveSibling(cacheFile.getFileName() + ".lock");
		this.cacheLoader = cacheLoader;
	}

	@Override
	public T call() throws Exception {

		try {
			final T newCacheContent = cacheLoader.call();

			try {
				tryWriteToDiskIfLock(newCacheContent); // TODO: what todo if write fails? Possibly expiration limit of disk-copy aswell
			} catch (RuntimeException e) {
				LOG.warn("Failed to write cache-content to disk (for use as fallback).", e);
			}

			return newCacheContent;

		} catch (RuntimeException originalException) {
			LOG.warn("Cache-loading failed. Attempting to load from disk as fallback mechanism. Enable debug-level to see stacktrace.");
			LOG.debug("Stacktrace for failing cache loading:", originalException);

			try {
				return tryReadFromDisk();
			} catch (RuntimeException exceptionWhileReadingFromDisk) {
				LOG.error("Failed to read cache-content from disk:", exceptionWhileReadingFromDisk);
				originalException.addSuppressed(exceptionWhileReadingFromDisk);
				throw originalException;
			}
		}
	}

	private void tryWriteToDiskIfLock(T newCacheContent) {

		if (Files.exists(lockFile)) {

			if (isLockFileExpired()) {
				LOG.warn("Lock-file is considered to be expired since it is older than {}. Deleting it.", LOCK_EXPIRES_AFTER);
				if (!tryDeleteExpiredLockFile()) {
					return;
				}
			} else {
				LOG.debug("Another process is updating the cache. Will not write to disk");
				return;
			}
		}

		try {
			LOG.trace("Creating lockfile");
			Files.createFile(lockFile);
		} catch (IOException e) {
			LOG.debug("Failed to create lock-file. Likely means that another process created it. Skipping write.");
			return;
		}

		try {

			final Path tempFile = getTempfilePath(cacheFile);
			if (Files.exists(tempFile)) {
				LOG.error("Temp-file used for writing cache already exists. If this happens, the algorithm for generating temp-file path needs improving. Temp-file: " + tempFile);
				return;
			}

			// write to temp-file
			try (OutputStream out = Files.newOutputStream(tempFile)){
				marshaller.write(newCacheContent, out);

			} catch (IOException | RuntimeException e) {
				LOG.error("Failed to write cache-content to file " + cacheFile + ". Will not attempt write until next time cache expires and loads.", e);
				return;
			}

			// replace cache-file with temp-file
			try {
				Files.move(tempFile, cacheFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				LOG.error("Failed to replace current cache-file with new copy.", e);
				silentDelete(tempFile);
			}


		} finally {
			silentDeleteLockFile();
		}

	}

	private boolean isLockFileExpired() {
		try {
			final DateTime lastModifiedTime = new DateTime(Files.getLastModifiedTime(this.lockFile).toMillis());
			return lastModifiedTime.isBefore(now().minus(LOCK_EXPIRES_AFTER));

		} catch (IOException e) {
			LOG.warn("Failed to read last-modified time of lock-file. Will not check lock-file for expiration.");
			return false;
		}

	}


	private Path getTempfilePath(Path forPath) {
		return forPath.resolveSibling(forPath.getFileName() + "." + System.currentTimeMillis() + "." + randomString(10));
	}

	private T tryReadFromDisk() {
		if (!Files.exists(cacheFile)) {
			throw new RuntimeException("Cache-content not found on disk. Should only happen the first time using the disk-fallback loader.");
		}

		try (InputStream diskCacheContent = new FileInputStream(cacheFile.toFile())) {

			return marshaller.read(diskCacheContent);

		} catch (IOException e) {
			throw new RuntimeException("Failed to read cache-content from disk.", e);
		}
	}

	private boolean silentDeleteLockFile() {
		try {
			LOG.trace("Deleting lockfile");
			final boolean wasDeleted = Files.deleteIfExists(lockFile);
			if (!wasDeleted) {
				LOG.error("Failed to delete lock-file. This could indicate that the lock-file was deleted by another process. This should never happen if the other processes honor the lock-file.");
			}
			return wasDeleted;

		} catch (IOException e) {
			LOG.error("Failed to delete lock-file. Lock-file will be deleted when the lock expires. In the meantime, no process will be able to aquire the lock.", e);
			return false;
		}
	}

	private void silentDelete(Path file) {
		try {
			Files.deleteIfExists(file);
		} catch (IOException e) {
			LOG.warn("Failed to delete file: {}", file, e);
		}
	}

	private boolean tryDeleteExpiredLockFile() {
		try {
			LOG.trace("Deleting expired lockfile");
			final boolean wasDeleted = Files.deleteIfExists(lockFile);
			if (!wasDeleted) {
				LOG.info("Another process appears to have deleted the expired lock-file. Continuing.");
			}
			return true;

		} catch (IOException e) {
			LOG.error("Failed to delete lock-file. Lock-file will be deleted when the lock expires. In the meantime, no process will be able to aquire the lock.", e);
			return false;
		}
	}

	private String randomString(int length) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			sb.append((char)(random.nextInt(25) + 'a'));
		}
		return sb.toString();
	}
}
