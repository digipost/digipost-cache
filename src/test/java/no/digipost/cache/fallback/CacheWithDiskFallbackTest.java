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

import no.digipost.cache.fallback.testharness.ExactKeyAsFilename;
import no.digipost.cache.fallback.testharness.FailingCacheLoader;
import no.digipost.cache.fallback.testharness.OkCacheLoader;
import no.digipost.cache.inmemory.Cache;
import no.digipost.cache.inmemory.SingleCached;
import no.digipost.cache.loader.Loader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CacheWithDiskFallbackTest {


	private static final String KEY1 = "key1";
	private static final String KEY2 = "key2";
	private static final String CONTENT1 = "content1";
	private static final String CONTENT2 = "content2";
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static final String FIRST_VALUE = "thevalue";

	@Test
	public void test_single_cached() throws IOException {
		final Path cacheDir = temporaryFolder.newFolder().toPath();
		final DiskStorageFallbackLoaderDecorator<String, String> cacheLoaderFactory = new DiskStorageFallbackLoaderDecorator<>(cacheDir, new ExactKeyAsFilename(), new SerializingMarshaller<String>());
		final FailSecondCacheLoader underlyingCacheLoader = new FailSecondCacheLoader(FIRST_VALUE);

		final Loader<String, String> cacheLoaderWithDiskFallback = cacheLoaderFactory.decorate(underlyingCacheLoader);

		final SingleCached<String> cache = new SingleCached<>(cacheLoaderWithDiskFallback);
		assertThat(cache.get(), is(FIRST_VALUE));
		cache.invalidate();
		assertThat(cache.get(), is(FIRST_VALUE));
	}

	@Test
	public void test_cache_with_multiple_keys() throws IOException {
		final Path cacheDir = temporaryFolder.newFolder().toPath();
		final Cache<String, String> cache = new Cache<>();
		final DiskStorageFallbackLoaderDecorator<String, String> diskFallbackFactory = new DiskStorageFallbackLoaderDecorator<>(cacheDir, new ExactKeyAsFilename(), new SerializingMarshaller<String>());

		// initialize cache
		cache.get(KEY1, diskFallbackFactory.decorate(new OkCacheLoader(CONTENT1)));
		cache.get(KEY2, diskFallbackFactory.decorate(new OkCacheLoader(CONTENT2)));

		cache.invalidateAll();
		assertThat(cache.get(KEY1, diskFallbackFactory.decorate(new FailingCacheLoader())), is(CONTENT1));
		assertThat(cache.get(KEY2, diskFallbackFactory.decorate(new FailingCacheLoader())), is(CONTENT2));
	}

	private static class FailSecondCacheLoader implements Callable<String> {

		private final String value;
		private AtomicLong counter = new AtomicLong(1);

		FailSecondCacheLoader(String value) {
			this.value = value;
		}

		@Override
		public String call() throws Exception {
			if (counter.getAndIncrement() % 2 == 0) {
				throw new RuntimeException("random error");
			}
			return value;
		}

	}



}
