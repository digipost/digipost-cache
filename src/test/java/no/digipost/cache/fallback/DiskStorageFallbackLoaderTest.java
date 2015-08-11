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

import no.digipost.util.FreezedTime;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;

import static no.digipost.cache.fallback.DiskStorageFallbackLoader.LOCK_EXPIRES_AFTER;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DiskStorageFallbackLoaderTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	@Rule
	public final FreezedTime time = new FreezedTime(DateTime.now().getMillis());

	public static final String FIRST_CONTENT = "first_content";
	public static final String SECOND_CONTENT = "second_content";
	public static final String THIRD_CONTENT = "third_content";
	private Path cache;
	private Path lockFile;
	private ExecutorService executor;

	@Before
	public void setUp() throws IOException {
		cache = temporaryFolder.getRoot().toPath().resolve("cache");
		lockFile = cache.resolveSibling(cache.getFileName() + ".lock");
		executor = Executors.newSingleThreadExecutor();
	}

	@Test
	public void should_load_cache_from_disk_as_fallback() throws Exception {
		assertThat(newDiskFallback(new OkCacheLoader(FIRST_CONTENT)).call(), is(FIRST_CONTENT));
		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(FIRST_CONTENT));
	}

	@Test
	public void should_fail_if_underlying_loader_fails_and_not_stored_on_disk() throws Exception {
		expectedException.expect(RuntimeException.class);
		expectedException.expectMessage("Failed!");
		newDiskFallback(new FailingCacheLoader()).call();
	}

	@Test
	public void should_primarily_return_value_from_underlying_loader() throws Exception {
		newDiskFallback(new OkCacheLoader(FIRST_CONTENT)).call();

		assertThat(newDiskFallback(new OkCacheLoader(SECOND_CONTENT)).call(), is(SECOND_CONTENT));
	}

	@Test
	public void should_not_allow_concurrent_updates_of_disk_fallback() throws Exception {
		newDiskFallback(new OkCacheLoader(FIRST_CONTENT)).call();

		final WaitOnWriteMarshaller waitingMarshaller = new WaitOnWriteMarshaller();
		// will take lock and wait
		final Future<String> waitingWrite = executor.submit(
						newDiskFallback(new OkCacheLoader(SECOND_CONTENT), waitingMarshaller));

		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(FIRST_CONTENT)); // second call will not have finished writing
		assertThat(newDiskFallback(new OkCacheLoader(THIRD_CONTENT)).call(), is(THIRD_CONTENT)); // will not be allowed to write to disk, lock is taken
		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(FIRST_CONTENT));

		waitingMarshaller.waiter.countDown(); //starts and lets writing finish
		waitingWrite.get();

		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(SECOND_CONTENT));
	}

	@Test
	public void should_allow_update_of_disk_fallback_if_lock_expired() throws Exception {
		final DiskStorageFallbackLoader<String> diskFallback = newDiskFallback(new OkCacheLoader(FIRST_CONTENT));
		diskFallback.call(); //initialize disk fallback

		// simulate lock
		Files.createFile(diskFallback.lockFile);

		assertThat(newDiskFallback(new OkCacheLoader(SECOND_CONTENT)).call(), is(SECOND_CONTENT)); // not allowed update
		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(FIRST_CONTENT));  // because second call was not allowed to update
		time.wait(LOCK_EXPIRES_AFTER.plus(Duration.standardSeconds(1)));

		assertThat(newDiskFallback(new OkCacheLoader(THIRD_CONTENT)).call(), is(THIRD_CONTENT)); // allowed update, lock expired
		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(THIRD_CONTENT));
	}


	private DiskStorageFallbackLoader<String> newDiskFallback(Callable<String> loader) {
		return newDiskFallback(loader, new SerializingMarshaller<String>());
	}

	private DiskStorageFallbackLoader<String> newDiskFallback(Callable<String> loader, Marshaller<String> marshaller) {
		return new DiskStorageFallbackLoader<>(cache, loader, marshaller);
	}

	public static class WaitOnWriteMarshaller extends SerializingMarshaller<String> {

		private CountDownLatch waiter = new CountDownLatch(1);

		@Override
		public String read(InputStream input) {
			return super.read(input);
		}

		@Override
		public void write(String toWrite, OutputStream output) {
			try {
				waiter.await();
			} catch (InterruptedException e) {
			}
			super.write(toWrite, output);
		}
	}

	public static class FailingCacheLoader implements Callable<String> {

		@Override
		public String call() throws Exception {
			throw new RuntimeException("Failed!");
		}
	}

	public static class OkCacheLoader implements Callable<String> {

		private final String content;

		OkCacheLoader(String content) {
			this.content = content;
		}
		@Override
		public String call() throws Exception {
			return content;
		}

	}
}