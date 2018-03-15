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
package no.digipost.cache.inmemory;

import no.digipost.util.FreezedTime;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static co.unruly.matchers.StreamMatchers.allMatch;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.generate;
import static no.digipost.DiggExceptions.mayThrow;
import static no.digipost.cache.inmemory.CacheConfig.expireAfterAccess;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.joda.time.Duration.millis;
import static org.joda.time.Duration.standardMinutes;
import static org.joda.time.Duration.standardSeconds;
import static org.junit.Assert.assertThat;

public class CacheTest {

	@Rule
	public final FreezedTime time = new FreezedTime(1000);

	private final Callable<Integer> incrementingValue = new Callable<Integer>() {
		final AtomicInteger num = new AtomicInteger(0);
		@Override public Integer call() {
			return num.getAndIncrement();
        }};


    private final SingleCached<Integer> value = new SingleCached<>("CacheTest", incrementingValue, expireAfterAccess(standardSeconds(1)));

	@Test
	public void resolvesValueOnFirstAccess() {
		assertThat(value.get(), is(0));
	}

	@Test
	public void reusesCachedValueWhileAccessingWithinExpiryTime() throws Exception {
		assertThat(asList(value.get(), value.get(), value.get()), everyItem(is(0)));
		time.wait(millis(900));
		assertThat(value.get(), is(0));
		time.wait(millis(20));
		assertThat(value.get(), is(0));
		time.wait(standardSeconds(14));
		assertThat(asList(value.get(), value.get(), value.get()), contains(1, 1, 1));
	}

	@Test(timeout = 40000)
	public void threadSafety() throws InterruptedException {
		final int threadAmount = 300;
		ExecutorService threadpool = Executors.newFixedThreadPool(threadAmount);
		try {
			Callable<Integer> valueWhenIncreased = new Callable<Integer>() {
				@Override
                public Integer call() throws Exception {
					while (value.get() == 0) {
						Thread.sleep(5);
					}
					return value.get();
                }
			};
			List<Future<Integer>> values = generate(() -> valueWhenIncreased).limit(threadAmount).map(threadpool::submit).collect(toList());
			Thread.sleep(2000);
			time.wait(standardSeconds(3));
			assertThat(values.stream().map(mayThrow((Future<Integer> value) -> value.get()).asUnchecked()), allMatch(is(1)));
		} finally {
			threadpool.shutdown();
			threadpool.awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	@Test
	public void invalidatingCache() {
		assertThat(value.get(), is(0));
		assertThat(value.get(), is(0));
		time.wait(standardMinutes(1));
		assertThat(value.get(), is(1));
		assertThat(value.get(), is(1));
		value.invalidate();
		assertThat(value.get(), is(2));
	}



}
