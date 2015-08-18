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

import java.util.function.Function;

public class LoaderTransformer<K, V, T> implements Loader<K, T> {
	private final Loader<K, V> underlyingLoader;
	private final Function<V, T> valueMapper;

	public LoaderTransformer(Loader<K, V> underlyingLoader, Function<V, T> valueMapper) {
		this.underlyingLoader = underlyingLoader;
		this.valueMapper = valueMapper;
	}

	public static <K, V, T> LoaderTransformer transform(Loader<K, V> underlyingLoader, Function<V, T> valueMapper) {
		return new LoaderTransformer(underlyingLoader, valueMapper);
	}

	@Override
	public T load(K key) throws Exception {
		return valueMapper.apply(underlyingLoader.load(key));
	}
}
