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

import no.digipost.cache.loader.Loader;

/**
 * Defines how to keep a fallback of a successfully loaded cache value. In
 * case a {@link Loader} fails, a {@link LoaderWithFallback} are able to use
 * the value that may have been kept using a {@code FallbackKeeper}
 *
 * @see LoaderWithFallback
 */
public interface FallbackKeeper<K, V> {

	/**
	 * Not do anything in order to keep a cache value. This is typically appropriate for cases
	 * where a {@link Loader fallback loader} tries to resolve the value using alternative means
	 * instead of relying on keeping previous succesfully loaded values.
	 */
	public static final FallbackKeeper<Object, Object> NO_KEEPING = new FallbackKeeper<Object, Object>() {
		@Override public void keep(Object key, Object value) { }};


	/**
	 * Keep the value (with it's associated key) successfully retrieved from a {@link Loader}.
	 */
	public void keep(K key, V value) throws Exception;

}
