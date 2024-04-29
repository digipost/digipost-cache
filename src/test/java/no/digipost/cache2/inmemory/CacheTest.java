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
package no.digipost.cache2.inmemory;

import no.digipost.cache2.loader.Loader;
import no.digipost.time.ControllableClock;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.generate;
import static no.digipost.DiggExceptions.mayThrow;
import static no.digipost.cache2.inmemory.CacheConfig.clockTicker;
import static no.digipost.cache2.inmemory.CacheConfig.expireAfterAccess;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CacheTest {

	private final AtomicInteger num = new AtomicInteger(-1);
	private final Loader<String, Integer> incrementingValue = key -> num.incrementAndGet();

    private final ControllableClock clock = ControllableClock.freezedAt(ofEpochMilli(1000));

    private final SingleCached<Integer> value =
            new SingleCached<Integer>("CacheTest", incrementingValue, new Cache<String, Integer>("single-int", asList(expireAfterAccess(ofSeconds(1)), clockTicker(clock))));

	@Test
	public void resolvesValueOnFirstAccess() {
		assertThat(value.get(), is(0));
	}

	@Test
	public void reusesCachedValueWhileAccessingWithinExpiryTime() throws Exception {
		assertThat(asList(value.get(), value.get(), value.get()), everyItem(is(0)));
		clock.timePasses(ofMillis(900));
		assertThat(value.get(), is(0));
		clock.timePasses(ofMillis(20));
		assertThat(value.get(), is(0));
		clock.timePasses(ofSeconds(14));
		assertThat(asList(value.get(), value.get(), value.get()), contains(1, 1, 1));
	}

	@Test(timeout = 40000)
	public void threadSafety() throws InterruptedException {
		final int threadAmount = 300;
		ExecutorService threadpool = Executors.newFixedThreadPool(threadAmount);
		try {
			Callable<Integer> valueWhenIncreased = () -> {
			    while (value.get() == 0) {
			        Thread.sleep(5);
			    }
			    return value.get();
			};
			List<Future<Integer>> values = generate(() -> valueWhenIncreased).limit(threadAmount).map(threadpool::submit).collect(toList());
			Thread.sleep(2000);
			clock.timePasses(ofSeconds(3));
			assertThat(values.stream().map(mayThrow((Future<Integer> value) -> value.get()).asUnchecked()).collect(toList()), everyItem(is(1)));
		} finally {
			threadpool.shutdown();
			threadpool.awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	@Test
	public void invalidatingCache() {
		assertThat(value.get(), is(0));
		assertThat(value.get(), is(0));
		clock.timePasses(ofMinutes(1));
		assertThat(value.get(), is(1));
		assertThat(value.get(), is(1));
		value.invalidate();
		assertThat(value.get(), is(2));
	}



}
