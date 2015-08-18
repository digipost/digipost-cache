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
package no.digipost.cache.loader;

import java.util.concurrent.Callable;

/**
 * A cache value loader which depends on the cache key. If resolving the
 * value does not depend on the key, consider using a {@link Callable}
 * instead.
 *
 * @param <K> The key's type.
 * @param <V> The cached value's type.
 */
public interface Loader<K, V> {
	V load(K key) throws Exception;

	class AsCallable<K, V> implements Callable<V> {

		private final Loader<K, V> loader;
		private final K key;

		public AsCallable(Loader<K, V> loader, K key) {
			this.loader = loader;
			this.key = key;
		}

		@Override
		public V call() throws Exception {
			return loader.load(key);
		}
	}
}
