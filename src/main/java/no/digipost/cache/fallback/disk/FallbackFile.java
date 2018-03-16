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
import java.time.Clock;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

public class FallbackFile {

	private static final Logger LOG = LoggerFactory.getLogger(FallbackFile.class);

	public final LockedFile lockedFile;

	private final Random random = new SecureRandom();
	private final AtomicBoolean written;

	FallbackFile(LockedFile file) {
		this.lockedFile = file;
		this.written = new AtomicBoolean(Files.exists(file.getPath()));
	}


	/**
	 * Read the contents of the fallback file.
	 *
	 * @return an {@link InputStream} for reading the contents of the fallback file.
	 */
	public InputStream read() throws IOException {
	    Path file = lockedFile.getPath();
        boolean fallbackFileExists = Files.exists(file);
		written.compareAndSet(false, fallbackFileExists);
		if (!written.get()) {
			throw new FallbackFileNotYetCreated(file);
		} else if (!fallbackFileExists) {
			throw new FileNotFoundException("File " + file + " not found, even though it is supposed to have been written.");
		}
		return Files.newInputStream(file);
	}



	/**
	 * Write contents to the fallback file.
	 *
	 * @return an {@link OutputStream} to write the fallback contents to.
	 */
	public OutputStream write() throws IOException {
		final Path tempfile = getTempfile();
		if (Files.exists(tempfile)) {
			throw new FileAlreadyExistsException(tempfile.toString(), null,
					"Temp-file used for writing cache already exists. " +
					"This is a bug. The algorithm for generating temp-file path needs improving.");
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
					Path file = lockedFile.getPath();
					if (LOG.isDebugEnabled()) {
						LOG.debug("Done writing cachevalue to disk. Comitting by renaming {} to {} (directory: {})",
								  tempfile.getFileName(), file.getFileName(), file.getParent());
					}
					Files.move(tempfile, file, ATOMIC_MOVE, REPLACE_EXISTING);
					written.set(true);
				} finally {
					Files.deleteIfExists(tempfile);
				}
			}
		};
	}


	@Override
	public String toString() {
		return "Fallback-file " + lockedFile.getPath();
	}

	private Path getTempfile() {
	    Path file = lockedFile.getPath();
	    return file.resolveSibling(file.getFileName() + "." + System.currentTimeMillis() + "." + randomString(10));
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
		private final FallbackFileNamingStrategy<? super K> fileNamingStrategy;
        private final Clock clock;

		public Resolver(Path directory, FallbackFileNamingStrategy<? super K> fileNamingStrategy, Clock clock) {
			this.directory = directory;
			this.fileNamingStrategy = fileNamingStrategy;
			this.clock = clock;
		}

		public FallbackFile resolveFor(K cacheKey) {
			return new FallbackFile(new LockedFile(directory.resolve(fileNamingStrategy.toFilename(cacheKey)), clock));
		}
	}

	public static class FallbackFileNotYetCreated extends IOException {
		private FallbackFileNotYetCreated(Path file) {
			super("The fallback file " + file + " has not been created yet. " +
				  "This may happen in rare circumstances " +
				  "if the Loader has never successfully produced any value.");
		}
	}
}
