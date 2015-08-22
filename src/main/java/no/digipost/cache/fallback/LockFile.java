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

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.joda.time.Duration.standardMinutes;

public class LockFile {

	/**
	 * The default maximum time a lock is allowed to be held before
	 * releasing it with {@link #releaseLock(Path)} is 10 minutes.
	 */
	static final Duration DEFAULT_EXPIRY_TIME = standardMinutes(10);

	static final Logger LOG = LoggerFactory.getLogger(LockFile.class);
	private static final String LOCK_FILE_POSTFIX = "." + LockFile.class.getCanonicalName() + ".lock";
	private final Path file;
	private final Duration maximumLockingDuration;


	public LockFile(Path forFile) {
		this(forFile, DEFAULT_EXPIRY_TIME);
	}

	public LockFile(Path forFile, Duration maximumLockingDuration) {
		this.file = forFile.resolveSibling(forFile.getFileName() + LOCK_FILE_POSTFIX);
		this.maximumLockingDuration = maximumLockingDuration;
	}

	/**
	 * Will run delegate if this thread is able to create given {@code lockfile}.
	 * The {@code lockfile} will be deleted again after delegate is run.
	 *
	 * @param delegate will be run if lock is acquired successfully
	 * @return
	 */
	public boolean runIfLock(Runnable delegate) {
		if (tryLock()) {
			try {
				delegate.run();
				return true;
			} finally {
				release();
			}
		} else {
			return false;
		}
	}


	/**
	 * Try to acquire a lock in order to safely write to the fallback file. This method is
	 * used for situations where one need to <em>attempt</em> writing, but it is ok to skip
	 * if other processes are currently writing to the file. It should always be invoked in
	 * the following fashion:
	 *
	 * <pre>{@code
	 * if (lockFile.tryLock()) {
	 *     try (OutputStream o = Files.newOutputStream(file)) {
	 *         // ...
	 *     } finally {
	 *         lockFile.release();
	 *     }
	 * }
	 * </pre>
	 *
	 *
	 * @return {@code true} if the lock was acquired, {@code false} false otherwise.
	 */
	public boolean tryLock() {
		// Check if lock is available. Removed expired locks.
		if (isLocked()) {
			if (isExpired()) {
				LOG.warn("Lock-file is considered to be expired since it is older than {}. Deleting it. " +
                            "There may be some problematic code which are unable to correctly release an aquired lock.",
                            maximumLockingDuration);
				if (!tryUnlockExpired()) {
					return false;
				}
			} else {
				LOG.debug("Another process is updating the cache-value. Skipping write.");
				return false;
			}
		}

		// Attempt to acquire lock
		try {
			LOG.trace("Creating lockfile");
			Files.createFile(file); // fails if lock-file exists
			LOG.trace("Acquired lock.");
			return true;
		} catch (FileAlreadyExistsException fileAlreadyExists) {
			LOG.debug("Failed to create lock-file. Means that another process created it. Will not run delegate.");
			return false;
		} catch (IOException e) {
			LOG.error("Unexpected error when creating lock-file. Will not run delegate. In the unlikely event that " +
					"the lock-file indeed was created, it will block the delegate-operation until this lock has expired (in {}).",
					maximumLockingDuration, e);
			return false;
		}
	}

	public boolean isLocked() {
		return Files.exists(file);
	}

	private boolean isExpired() {
		try {
			Instant lastModifiedTime = new Instant(Files.getLastModifiedTime(file).toMillis());
			return lastModifiedTime.isBefore(Instant.now().minus(maximumLockingDuration));
		} catch (IOException e) {
			LOG.warn("Failed to read last-modified time of lock-file. Will not check lock-file for expiration.");
			return false;
		}
	}

	private boolean tryUnlockExpired() {
		try {
			LOG.trace("Deleting expired lockfile");
			final boolean wasDeleted = Files.deleteIfExists(file);
			if (!wasDeleted) {
				LOG.info("Another process appears to have deleted the expired lock-file. Continuing.");
			}
			return true;

		} catch (IOException e) {
			LOG.error("Failed to delete lock-file. Lock-file will be deleted when the lock expires. In the meantime, no process will be able to acquire the lock.", e);
			return false;
		}
	}

	public boolean release() {
		try {
			LOG.trace("Deleting lockfile");
			final boolean wasDeleted = Files.deleteIfExists(file);
			if (!wasDeleted) {
				LOG.error("Failed to delete lock-file {}. "
						+ "This could indicate that the lock-file was deleted by another process. "
						+ "This should never happen if the other processes honor the lock-file.",
						file);
			}
			return wasDeleted;

		} catch (IOException e) {
			LOG.error("Failed to delete lock-file {} because {}: '{}'. Lock-file will be deleted "
					+ "when the lock expires. In the meantime, no process will be able to aquire the lock.",
					file, e.getClass().getSimpleName(), e.getMessage(), e);
			return false;
		}
	}


}
