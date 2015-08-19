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

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LockFiles {

	private final Duration lockfileExpiration;
	private final Logger logger;

	public LockFiles(Duration lockfileExpiration, Logger logger) {
		this.lockfileExpiration = lockfileExpiration;
		this.logger = logger;
	}

	/**
	 * Will run delegate if this thread is able to create lockfile. lockfile will be deleted again after delegate is run.
	 * @param lockfile the marker-file to use to signal a lock
	 * @param delegate will be run if lock is acquired successfully
	 * @return
	 */
	public boolean runIfLock(Path lockfile, Runnable delegate) {
		boolean acquiredLock = acquireLock(lockfile);
		if (!acquiredLock) {
			return false;
		}

		try {
			logger.trace("Acquired lock.");
			delegate.run();
			return true;
		} finally {
			releaseLock(lockfile);
		}
	}

	private boolean acquireLock(Path lockfile) {
		// Check if lock is available. Removed expired locks.
		if (isLocked(lockfile)) {

			if (lockedLongerAgoThan(lockfileExpiration, lockfile)) {
				logger.warn("Lock-file is considered to be expired since it is older than {}. Deleting it.", lockfileExpiration);
				if (!tryDeleteExpiredLockFile(lockfile)) {
					return false;
				}
			} else {
				logger.debug("Another process is updating the cache-value. Skipping write.");
				return false;
			}
		}

		// Attempt to acquire lock
		try {
			logger.trace("Creating lockfile");
			Files.createFile(lockfile); // fails if lock-file exists
			return true;
		} catch (FileAlreadyExistsException fileAlreadyExists) {
			logger.debug("Failed to create lock-file. Means that another process created it. Will not run delegate.");
			return false;
		} catch (IOException e) {
			logger.error("Unexpected error when creating lock-file. Will not run delegate. In the unlikely event that " +
					"the lock-file indeed was created, it will block the delegate-operation until this lock has expired (in {}).",
					lockfileExpiration, e);
			return false;
		}
	}

	private boolean isLocked(Path lockfile) {
		return Files.exists(lockfile);
	}

	private boolean lockedLongerAgoThan(Duration duration, Path lockfile) {
		try {
			Instant lastModifiedTime = new Instant(Files.getLastModifiedTime(lockfile).toMillis());
			return lastModifiedTime.isBefore(Instant.now().minus(duration));
		} catch (IOException e) {
			logger.warn("Failed to read last-modified time of lock-file. Will not check lock-file for expiration.");
			return false;
		}
	}

	private boolean tryDeleteExpiredLockFile(Path lockfile) {
		try {
			logger.trace("Deleting expired lockfile");
			final boolean wasDeleted = Files.deleteIfExists(lockfile);
			if (!wasDeleted) {
				logger.info("Another process appears to have deleted the expired lock-file. Continuing.");
			}
			return true;

		} catch (IOException e) {
			logger.error("Failed to delete lock-file. Lock-file will be deleted when the lock expires. In the meantime, no process will be able to acquire the lock.", e);
			return false;
		}
	}

	private boolean releaseLock(Path lockfile) {
		try {
			logger.trace("Deleting lockfile");
			final boolean wasDeleted = Files.deleteIfExists(lockfile);
			if (!wasDeleted) {
				logger.error("Failed to delete lock-file. This could indicate that the lock-file was deleted by another process. This should never happen if the other processes honor the lock-file.");
			}
			return wasDeleted;

		} catch (IOException e) {
			logger.error("Failed to delete lock-file. Lock-file will be deleted when the lock expires. In the meantime, no process will be able to acquire the lock.", e);
			return false;
		}
	}


}
