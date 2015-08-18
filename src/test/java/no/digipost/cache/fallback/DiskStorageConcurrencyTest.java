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
import no.digipost.cache.fallback.testharness.ExactKeyAsFilename;
import no.digipost.cache.fallback.testharness.RandomAnswerCacheLoader;
import no.digipost.cache.loader.Callables;
import no.digipost.cache.loader.Loader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.any;

@RunWith(MockitoJUnitRunner.class)
public class DiskStorageConcurrencyTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	@Rule
	public final Timeout timeout = Timeout.seconds(60);

	@Mock
	private Logger logger;
	private Path cache;


	@Before
	public void setUp() throws IOException {
		cache = temporaryFolder.getRoot().toPath().resolve("fallback");
		Files.createDirectories(cache);
	}

	@Test
	public void massive_concurrency() throws Exception {
		final Loader<String, String> underlyingCacheLoader = Callables.toLoader(new RandomAnswerCacheLoader());
		final DiskStorageFallbackLoader<String, String> fallbackLoader2 =
				new DiskStorageFallbackLoader<>(logger, new FallbackFile.Resolver<>(cache, new ExactKeyAsFilename()),
						underlyingCacheLoader, new SerializingMarshaller<String>());

		Callable<String> callableFallbackLoader = new Loader.AsCallable<>(fallbackLoader2, "key");

		callableFallbackLoader.call(); // initialize disk-fallback
		final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(30));

		List<ListenableFuture<String>> futures = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			futures.add(executorService.submit(callableFallbackLoader));
		}
		// should not throw an exception because either result is returned som underlying cache-loader or the disk-copy
		Futures.allAsList(futures).get();
		assertNoErrors(this.logger);
	}

	private void assertNoErrors(Logger logger) {
		Mockito.verify(logger, Mockito.times(0)).error(Matchers.anyString());
		Mockito.verify(logger, Mockito.times(0)).error(Matchers.anyString(), Matchers.anyObject());
		Mockito.verify(logger, Mockito.times(0)).error(Matchers.anyString(), Matchers.anyObject(), Matchers.anyObject());
		Mockito.verify(logger, Mockito.times(0)).error(Matchers.anyString(), any(Object[].class));
		Mockito.verify(logger, Mockito.times(0)).error(Matchers.anyString(), any(Throwable.class));
	}

}
