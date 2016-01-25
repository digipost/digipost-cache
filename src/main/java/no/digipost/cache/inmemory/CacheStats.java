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

public class CacheStats {

	private final long requestCount;
	private final long hitCount;
	private final double hitRate;
	private final long missCount;
	private final double missRate;

	public CacheStats(long requestCount, long hitCount, double hitRate, long missCount, double missRate) {

		this.requestCount = requestCount;
		this.hitCount = hitCount;
		this.hitRate = hitRate;
		this.missCount = missCount;
		this.missRate = missRate;
	}

	public static CacheStats fromGuava(com.google.common.cache.CacheStats guavaStats) {
		return new CacheStats(
				guavaStats.requestCount(),
				guavaStats.hitCount(),
				guavaStats.hitRate(),
				guavaStats.missCount(),
				guavaStats.missRate());
	}

	public long getRequestCount() {
		return requestCount;
	}

	public long getHitCount() {
		return hitCount;
	}

	public double getHitRate() {
		return hitRate;
	}

	public long getMissCount() {
		return missCount;
	}

	public double getMissRate() {
		return missRate;
	}
}
