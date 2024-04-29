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
package no.digipost.cache2.loader;

import java.util.concurrent.Callable;

/**
 * A component able to extend a {@link Loader}'s behavior using
 * the decorator pattern.
 */
public interface LoaderDecorator<K, V> {

	/**
	 * Decorate a {@link Loader}.
	 * <p>
	 * To decorate a {@link Callable}, supply it using {@link Callables#toLoader(Callable)}.
	 *
	 *
	 * @param underlyingLoader the {@code Loader} to decorate.
	 *
	 * @return a new {@code Loader} decorating the given {@code underlyingLoader}.
	 */
	Loader<K, V> decorate(Loader<? super K, V> underlyingLoader);

}
