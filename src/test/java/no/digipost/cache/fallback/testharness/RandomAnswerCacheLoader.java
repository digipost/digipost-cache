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
package no.digipost.cache.fallback.testharness;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

public class RandomAnswerCacheLoader implements Callable<String> {

	private final AtomicLong counter = new AtomicLong(0);
	private final Random random = new Random();

	@Override
	public String call() throws Exception {
		if (counter.incrementAndGet() % 2 == 0) {
			throw new SimulatedLoaderFailure();
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