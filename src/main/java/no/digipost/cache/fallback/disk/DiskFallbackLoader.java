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
package no.digipost.cache.fallback.disk;

import no.digipost.cache.fallback.marshall.Marshaller;
import no.digipost.cache.loader.Loader;

import java.io.InputStream;

public class DiskFallbackLoader<K, V> implements Loader<K, V> {

	private final FallbackFile.Resolver<K> fileResolver;
	private final Marshaller<V> marshaller;

	DiskFallbackLoader(FallbackFile.Resolver<K> fileResolver, Marshaller<V> marshaller) {
		this.fileResolver = fileResolver;
		this.marshaller = marshaller;
	}

	@Override
	public V load(K key) throws Exception {
		try (InputStream fallbackContent = fileResolver.resolveFor(key).read()) {
			return marshaller.read(fallbackContent);
		}
	}

}
