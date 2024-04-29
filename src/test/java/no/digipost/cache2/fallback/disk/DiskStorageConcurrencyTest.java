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
import no.digipost.cache2.fallback.marshall.SerializingMarshaller;
import no.digipost.cache2.fallback.testharness.RandomAnswerCacheLoader;
import no.digipost.cache2.loader.Loader;
import no.digipost.cache2.loader.LoaderDecorator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.stream.Collectors.toList;
import static no.digipost.DiggExceptions.mayThrow;
import static no.digipost.cache2.fallback.disk.FallbackFileNamingStrategy.USE_KEY_TOSTRING_AS_FILENAME;
import static no.digipost.cache2.loader.Callables.toLoader;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class DiskStorageConcurrencyTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final Timeout timeout = Timeout.seconds(60);

	@Rule
	public final MockitoRule mockito = MockitoJUnit.rule();

	private ExecutorService executorService;

	@Before
	public void setUpExecutor() {
		executorService = Executors.newFixedThreadPool(30);
	}

	@Test
	public void massive_concurrency() throws Exception {
		String key = getClass().getSimpleName();
		LoaderDecorator<String, String> cacheLoaderFactory = new LoaderWithDiskFallbackDecorator<>(
				temporaryFolder.getRoot().toPath(), USE_KEY_TOSTRING_AS_FILENAME, new SerializingMarshaller<String>(), new FallbackKeeperFailedHandler.Rethrow());
		Callable<String> fallbackLoader = new Loader.AsCallable<>(cacheLoaderFactory.decorate(toLoader(new RandomAnswerCacheLoader())), key);
		fallbackLoader.call(); // initialize disk-fallback

		List<CompletableFuture<String>> futures = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			futures.add(CompletableFuture.supplyAsync(mayThrow(fallbackLoader::call).asUnchecked(), executorService));
		}
		// should not throw an exception because either result is returned som underlying cache-loader or the disk-copy
		assertThat(futures.stream().map(CompletableFuture::join).collect(toList()), hasSize(10_000));
	}


	@After
	public void shutdownExecutor() {
		executorService.shutdownNow();
	}

}
