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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

public class FallbackFile {

	private static final Logger LOG = LoggerFactory.getLogger(FallbackFile.class);

	private final Random random = new SecureRandom();

	public final Path cacheValue;
	public final Path lock;
	private final AtomicBoolean written;

	FallbackFile(Path file) {
		this.cacheValue = file;
		this.lock = cacheValue.resolveSibling(cacheValue.getFileName() + ".lock");
		this.written = new AtomicBoolean(Files.exists(cacheValue));
	}

	public InputStream read() throws IOException {
		boolean fallbackFileExists = Files.exists(cacheValue);
		written.compareAndSet(false, fallbackFileExists);
		if (!written.get()) {
			throw new FileNotFoundException(
					"The fallback was not found on disk because it has not been written yet. This may happen in rare circumstances " +
					"if the Loader has never successfully produced any value.");
		}
		if (!fallbackFileExists) {
			LOG.error("Fallback-value has been written to disk, but the file for it, {}, does not exist.", cacheValue);
			throw new FileNotFoundException("File " + cacheValue + " not found on disk, even though the fallback file is supposed to have been written.");
		}
		return Files.newInputStream(cacheValue);
	}

	public OutputStream write() throws IOException {
		final Path tempfile = getTempfile();
		if (Files.exists(tempfile)) {
			throw new FileAlreadyExistsException(tempfile.toString(), null,
					"Temp-file used for writing cache already exists. " +
					"If this happens, the algorithm for generating temp-file path needs improving.");
		}
		final OutputStream stream = Files.newOutputStream(tempfile, CREATE_NEW, WRITE);
		return new OutputStream() {
			final AtomicBoolean closed = new AtomicBoolean(false);

			@Override
			public void write(int b) throws IOException {
				stream.write(b);
			}

			@Override
			public void close() throws IOException {
				if (closed.getAndSet(true)) {
					return;
				}
				try {
					stream.close();
					if (LOG.isDebugEnabled()) {
						LOG.debug("Done writing cachevalue to disk. Comitting by renaming {} to {} (directory: {})", tempfile.getFileName(), cacheValue.getFileName(), cacheValue.getParent());
					}
					Files.move(tempfile, cacheValue, ATOMIC_MOVE, REPLACE_EXISTING);
					written.set(true);
				} finally {
					Files.deleteIfExists(tempfile);
				}
			}
		};
	}


	public boolean isLocked() {
		return Files.exists(lock);
	}

	public boolean lockedLongerAgoThan(Duration duration) {
		try {
			Instant lastModifiedTime = new Instant(Files.getLastModifiedTime(lock).toMillis());
			return lastModifiedTime.isBefore(Instant.now().minus(duration));
		} catch (IOException e) {
			LOG.warn("Failed to read last-modified time of lock-file. Will not check lock-file for expiration.");
			return false;
		}

	}

	public boolean silentDeleteLockFile() {
		try {
			LOG.trace("Deleting lockfile");
			final boolean wasDeleted = Files.deleteIfExists(lock);
			if (!wasDeleted) {
				LOG.error("Failed to delete lock-file. This could indicate that the lock-file was deleted by another process. This should never happen if the other processes honor the lock-file.");
			}
			return wasDeleted;

		} catch (IOException e) {
			LOG.error("Failed to delete lock-file. Lock-file will be deleted when the lock expires. In the meantime, no process will be able to aquire the lock.", e);
			return false;
		}
	}

	@Override
	public String toString() {
		return "Fallback-file " + cacheValue;
	}

	private Path getTempfile() {
		return cacheValue.resolveSibling(cacheValue.getFileName() + "." + System.currentTimeMillis() + "." + randomString(10));
	}

	private String randomString(int length) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			sb.append((char)(random.nextInt(25) + 'a'));
		}
		return sb.toString();
	}


	public static class Resolver<K> {

		private final Path directory;
		private final FileNamingStrategy<? super K> fileNamingStrategy;

		public Resolver(Path directory, FileNamingStrategy<? super K> fileNamingStrategy) {
			this.directory = directory;
			this.fileNamingStrategy = fileNamingStrategy;
		}

		public FallbackFile resolveFor(K cacheKey) {
			return new FallbackFile(directory.resolve(fileNamingStrategy.toFilename(cacheKey)));
		}
	}
}
