/*
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
package no.digipost.cache2.fallback.disk;

import no.digipost.cache2.fallback.FallbackKeeperFailedHandler;
import no.digipost.cache2.fallback.marshall.Marshaller;
import no.digipost.cache2.fallback.marshall.SerializingMarshaller;
import no.digipost.cache2.fallback.testharness.FailingCacheLoader;
import no.digipost.cache2.fallback.testharness.OkCacheLoader;
import no.digipost.cache2.fallback.testharness.SimulatedLoaderFailure;
import no.digipost.cache2.loader.Loader;
import no.digipost.cache2.loader.LoaderDecorator;
import no.digipost.time.ControllableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static no.digipost.cache2.fallback.disk.FallbackFileNamingStrategy.USE_KEY_TOSTRING_AS_FILENAME;
import static no.digipost.cache2.loader.Callables.toLoader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class DiskStorageFallbackTest {

	private final ControllableClock clock = ControllableClock.freezedAt(Instant.now());

	private static final String FIRST_CONTENT = "first_content";
	private static final String SECOND_CONTENT = "second_content";
	private static final String THIRD_CONTENT = "third_content";

	private final String key = getClass().getSimpleName() + "_cachekey";

	@TempDir
	private Path cache;
	private ExecutorService executor;

	@BeforeEach
	void setUp() {
		executor = Executors.newSingleThreadExecutor();
	}

	@Test
	void should_load_cache_from_disk_as_fallback() throws Exception {
		assertThat(newDiskFallback(new OkCacheLoader(FIRST_CONTENT)).call(), is(FIRST_CONTENT));
		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(FIRST_CONTENT));
	}

	@Test
	void should_fail_if_underlying_loader_fails_and_not_stored_on_disk() throws Exception {
		Callable<String> failingCacheLoader = newDiskFallback(new FailingCacheLoader());
		assertThrows(SimulatedLoaderFailure.class, failingCacheLoader::call);
	}

	@Test
	void should_primarily_return_value_from_underlying_loader() throws Exception {
		newDiskFallback(new OkCacheLoader(FIRST_CONTENT)).call();

		assertThat(newDiskFallback(new OkCacheLoader(SECOND_CONTENT)).call(), is(SECOND_CONTENT));
	}

	@Test
	void should_not_allow_concurrent_updates_of_disk_fallback() throws Exception {
		newDiskFallback(new OkCacheLoader(FIRST_CONTENT)).call();

		final WaitOnWriteMarshaller waitingMarshaller = new WaitOnWriteMarshaller();
		// will take lock and wait
		final Future<String> waitingWrite = executor.submit(
						newDiskFallback(new OkCacheLoader(SECOND_CONTENT), waitingMarshaller));
		waitingMarshaller.waitUntilWriteStarts();

		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(FIRST_CONTENT)); // second call will not have finished writing
		assertThat(newDiskFallback(new OkCacheLoader(THIRD_CONTENT)).call(), is(THIRD_CONTENT)); // will not be allowed to write to disk, lock is taken
		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(FIRST_CONTENT));

		waitingMarshaller.finishWriting(); //starts and lets writing finish
		waitingWrite.get();

		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(SECOND_CONTENT));
	}


	@Test
	void should_allow_update_of_disk_fallback_if_lock_expired() throws Exception {

		LoaderDecorator<String, String> diskFallbackDecorator = new LoaderWithDiskFallbackDecorator<>(cache, USE_KEY_TOSTRING_AS_FILENAME, new SerializingMarshaller<String>(), new FallbackKeeperFailedHandler.Rethrow(), clock);
		Loader<String, String> diskFallback = diskFallbackDecorator.decorate(toLoader(new OkCacheLoader(FIRST_CONTENT)));
		diskFallback.load(key); //initialize disk fallback
		// simulate lock
		assertTrue(new FallbackFile.Resolver<>(cache, USE_KEY_TOSTRING_AS_FILENAME, clock).resolveFor(key).lockedFile.tryLock());

		assertThat(newDiskFallback(new OkCacheLoader(SECOND_CONTENT)).call(), is(SECOND_CONTENT)); // not allowed update
		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(FIRST_CONTENT));  // because second call was not allowed to update
		clock.timePasses(LockedFile.DEFAULT_EXPIRY_TIME.plus(Duration.ofSeconds(1)));

		assertThat(newDiskFallback(new OkCacheLoader(THIRD_CONTENT)).call(), is(THIRD_CONTENT)); // allowed update, lock expired
		assertThat(newDiskFallback(new FailingCacheLoader()).call(), is(THIRD_CONTENT));
	}


	private Callable<String> newDiskFallback(Callable<String> loader) {
		return newDiskFallback(loader, new SerializingMarshaller<String>());
	}

	private Callable<String> newDiskFallback(Callable<String> loader, Marshaller<String> marshaller) {
		LoaderDecorator<String, String> fallbackLoaderFactory =	new LoaderWithDiskFallbackDecorator<String, String>(cache, USE_KEY_TOSTRING_AS_FILENAME, marshaller, new FallbackKeeperFailedHandler.Rethrow(), clock);
		return new Loader.AsCallable<>(fallbackLoaderFactory.decorate(toLoader(loader)), key);
	}


	private static class WaitOnWriteMarshaller extends SerializingMarshaller<String> {

		private CountDownLatch waitOnWrite = new CountDownLatch(1);
		private CountDownLatch waitBeforeStartWrite = new CountDownLatch(1);

		@Override
		public String read(InputStream input) throws Exception {
			return super.read(input);
		}

		@Override
		public void write(String toWrite, OutputStream output) throws IOException {
			waitBeforeStartWrite.countDown();
			try {
				waitOnWrite.await();
			} catch (InterruptedException e) {
			}
			super.write(toWrite, output);
		}

		public void waitUntilWriteStarts() {
			try {
				waitBeforeStartWrite.await();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		public void finishWriting() {
			waitOnWrite.countDown();
		}
	}
}