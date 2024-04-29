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
package no.digipost.cache2.fallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a failing {@link FallbackKeeper#keep(Object, Object)}.
 * As writing a fallback value should in general not interfere with
 * yielding the already successfully loaded value, the most appropriate
 * action is to {@link LogAsError log errors}, and investigate if this starts
 * happening regularily.
 */
public interface FallbackKeeperFailedHandler<K, V> {
	void handle(K cacheKey, V value, Exception cause);


	class LogAsError implements FallbackKeeperFailedHandler<Object, Object> {

		private static final Logger LOG = LoggerFactory.getLogger(FallbackKeeperFailedHandler.LogAsError.class);

		@Override
		public void handle(Object cacheKey, Object value, Exception cause) {
			LOG.error("Failed to write cache-value for key '{}' to disk for use as fallback because {}: '{}', "
					+ "but will still return value from underlying cache-loader. "
					+ "Writing this fallback value will not be re-attempted until next time cache expires and "
					+ "successfully loads from underlying cache-loader.",
					cacheKey, cause.getClass().getSimpleName(), cause.getMessage(), cause);
		}
	}

	class Rethrow implements FallbackKeeperFailedHandler<Object, Object> {
		@Override
		public void handle(Object cacheKey, Object value, Exception cause) {
			throw new FallbackWriteFailed(cause, cacheKey, value);
		}
	}

	class FallbackWriteFailed extends RuntimeException {
		private FallbackWriteFailed(Exception cause, Object cacheKey, Object value) {
			super("Writing fall-back value failed, and '" + value + "', which was successfully retrieved " +
		          "from the underlying loader, will not be loaded into the cache.", cause);
		}
	}
}
