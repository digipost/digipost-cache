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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class DiskStorageConcurrencyTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	@Rule
	public final Timeout timeout = Timeout.seconds(60);
	private Path cache;


	@Before
	public void setUp() throws IOException {
		cache = temporaryFolder.newFile("cache").toPath();
	}

	@Test
	public void massive_concurrency() throws Exception {
		final DiskStorageFallback<String> fallbackLoader = new DiskStorageFallback<String>(cache, new RandomAnswerCacheLoader(), new SerializingMarshaller<String>());
		fallbackLoader.call(); // initialize disk-cache

		final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(30));

		List<ListenableFuture<String>> futures = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			futures.add(executorService.submit(fallbackLoader));
		}
		// should not throw an exception because either result is returned som underlying cache-loader or the disk-copy
		Futures.allAsList(futures).get();
	}


	private static class RandomAnswerCacheLoader implements Callable<String> {

		private AtomicLong counter = new AtomicLong(1);
		private Random random = new Random();

		@Override
		public String call() throws Exception {
			if (counter.getAndIncrement() % 2 == 0) {
				throw new RuntimeException("random error");
			}
			return randomString(10);
		}

		private String randomString(int length) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < length; i++) {
				sb.append((char) (random.nextInt(25) + 'a'));
			}
			return sb.toString();
		}
	}

}
