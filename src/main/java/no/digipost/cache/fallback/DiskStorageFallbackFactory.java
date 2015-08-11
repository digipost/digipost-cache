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

import java.nio.file.Path;
import java.util.concurrent.Callable;

public class DiskStorageFallbackFactory<K, V> {
	private final Path cacheDirectory;
	private final CacheKeyNamingStrategy cacheKeyNamingStrategy;
	private final Marshaller<V> marshaller;

	public DiskStorageFallbackFactory(Path cacheDirectory, CacheKeyNamingStrategy<K> cacheKeyNamingStrategy, Marshaller<V> marshaller) {
		this.cacheDirectory = cacheDirectory;
		this.cacheKeyNamingStrategy = cacheKeyNamingStrategy;
		this.marshaller = marshaller;
	}

	public DiskStorageFallback fallbackPerKey(K key, Callable<V> loader) {
		return new DiskStorageFallback(cacheDirectory.resolve(cacheKeyNamingStrategy.keyAsFilename(key)), loader, marshaller);
	}

}
