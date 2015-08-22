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
import no.digipost.cache.fallback.testharness.RandomAnswerCacheLoader;
import no.digipost.cache.loader.Loader;
import no.digipost.cache.loader.LoaderDecorator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static no.digipost.cache.fallback.FileNamingStrategy.USE_KEY_TOSTRING_AS_FILENAME;
import static no.digipost.cache.loader.Callables.toLoader;

public class DiskStorageConcurrencyTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final Timeout timeout = Timeout.seconds(60);

	@Rule
	public final MockitoRule mockito = MockitoJUnit.rule();

	private ListeningExecutorService executorService;

	@Before
	public void setUpExecutor() {
		executorService = listeningDecorator(Executors.newFixedThreadPool(30));
	}

	@Test
	public void massive_concurrency() throws Exception {
		String key = getClass().getSimpleName();
		LoaderDecorator<String, String> cacheLoaderFactory = new DiskStorageFallbackLoaderDecorator<>(temporaryFolder.getRoot().toPath(), USE_KEY_TOSTRING_AS_FILENAME, new SerializingMarshaller<String>());
		Callable<String> fallbackLoader = new Loader.AsCallable<>(cacheLoaderFactory.decorate(toLoader(new RandomAnswerCacheLoader())), key);
		fallbackLoader.call(); // initialize disk-fallback

		List<ListenableFuture<String>> futures = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			futures.add(executorService.submit(fallbackLoader));
		}
		// should not throw an exception because either result is returned som underlying cache-loader or the disk-copy
		Futures.allAsList(futures).get();
		LoggerFactory.getLogger(getClass()).error("TODO: implement verification that no error has occured.");
	}


	@After
	public void shutdownExecutor() {
		executorService.shutdownNow();
	}

}
