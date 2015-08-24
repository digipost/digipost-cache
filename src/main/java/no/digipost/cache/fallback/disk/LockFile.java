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
package no.digipost.cache.fallback.disk;

import no.digipost.cache.function.ThrowingRunnable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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


	LockFile(Path forFile) {
		this(forFile, DEFAULT_EXPIRY_TIME);
	}

	LockFile(Path forFile, Duration maximumLockingDuration) {
		this.file = forFile.resolveSibling(forFile.getFileName() + LOCK_FILE_POSTFIX);
		this.maximumLockingDuration = maximumLockingDuration;
	}

	/**
	 * Will run delegate if this thread is able to create given {@code lockfile}.
	 * The {@code lockfile} will be deleted again after delegate is run.
	 *
	 * @param operation will be run if lock is acquired successfully
	 * @return {@code true} if the operation was run, {@code false} if not because
	 *         the lock was not available.
	 */
	public <X extends Exception> boolean runIfLock(ThrowingRunnable<X> operation) throws X {
		if (tryLock()) {
			try {
				operation.run();
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
				releaseExpired();
			} else {
				LOG.debug("Another process is updating the cache-value. Not yielding lock.");
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
			throw new UnableToAcquireLock(e, maximumLockingDuration);
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
			LOG.warn("Failed to read last-modified time of lock-file. Treats is as not expired.");
			return false;
		}
	}

	private boolean releaseExpired() {
		try {
			LOG.trace("Deleting expired lockfile");
			if (!Files.deleteIfExists(file)) {
				LOG.info("Another process may have deleted the expired lock-file. This is expected behavior, " +
						 "and continuing normally.");
			}
			return true;

		} catch (IOException e) {
			throw new UnableToReleaseLock(e, maximumLockingDuration);
		}
	}

	public void release() {
		try {
			LOG.trace("Deleting lockfile");
			Files.delete(file);
		} catch (NoSuchFileException e) {
			throw new TryingToDeleteNonExistingLockFile(e);
		} catch (IOException e) {
			throw new UnableToReleaseLock(e, maximumLockingDuration);
		}
	}

	public static class UnableToAcquireLock extends RuntimeException {
		private UnableToAcquireLock(Exception cause, Duration lockExpiryDuration) {
			super("Got " + cause.getClass().getSimpleName() + ": '" + cause.getMessage() +
				  "' when trying to create lock-file, and thus the lock will not be yielded. " +
				  "In the unlikely event that the lock-file was created after all, it will prevent " +
				  "anyone from acquiring the lock until it has expired in " + lockExpiryDuration,
				  cause);
		}
	}

	public static class TryingToDeleteNonExistingLockFile extends RuntimeException {
		public TryingToDeleteNonExistingLockFile(NoSuchFileException cause) {
			super("Got " + cause.getClass().getSimpleName() + ": '" + cause.getMessage() +
				  "' when trying to delete lock-file. This could indicate that the lock-file " +
				  "was deleted by another process, and indicates a bug. Tt should never happen " +
				  "as long as other processes honor the lock-file.", cause);
		}
	}

	public static class UnableToReleaseLock extends RuntimeException {
		private UnableToReleaseLock(Exception cause, Duration lockExpiryDuration) {
			super("Unable to delete lock-file because " + cause.getClass().getSimpleName() + ": '" +
			      cause.getMessage() + "'. The lock may now not be acquired until it expires " +
	      		  "(expiry time: " + lockExpiryDuration + ").", cause);
		}
	}


}
