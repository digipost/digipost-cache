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

import no.digipost.cache.inmemory.Cache;
import no.digipost.cache.inmemory.SingleCached;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CacheWithDiskFallbackTest {


	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static final String FIRST_VALUE = "thevalue";

	@Test
	public void test() throws IOException {
		final Path cacheFile = temporaryFolder.newFile().toPath();
		final FailSecondCacheLoader underlyingCacheLoader = new FailSecondCacheLoader(FIRST_VALUE);
		final DiskStorageFallbackLoader<String> cacheLoaderWithDiskFallback = new DiskStorageFallbackLoader<>(cacheFile, underlyingCacheLoader, new SerializingMarshaller<String>());

		final SingleCached<String> cache = new SingleCached<>(cacheLoaderWithDiskFallback);
		assertThat(cache.get(), is(FIRST_VALUE));
		cache.invalidate();
		assertThat(cache.get(), is(FIRST_VALUE));
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
