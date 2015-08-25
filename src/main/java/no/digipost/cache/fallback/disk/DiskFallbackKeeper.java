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

import no.digipost.cache.fallback.FallbackKeeper;
import no.digipost.cache.fallback.disk.FallbackFile.Resolver;
import no.digipost.cache.fallback.marshall.Marshaller;
import no.digipost.cache.function.ThrowingRunnable;

import java.io.IOException;
import java.io.OutputStream;

class DiskFallbackKeeper<K, V> implements FallbackKeeper<K, V> {

	private Resolver<K> fileResolver;
	private Marshaller<V> marshaller;

	public DiskFallbackKeeper(FallbackFile.Resolver<K> fileResolver, Marshaller<V> marshaller) {
		this.fileResolver = fileResolver;
		this.marshaller = marshaller;
	}

	@Override
	public void keep(K key, final V value) throws Exception {
		final FallbackFile fallbackFile = fileResolver.resolveFor(key);
		fileResolver.resolveFor(key).lock.runIfLock(new ThrowingRunnable<IOException>() {
			@Override
			public void run() throws IOException {
				try (OutputStream out = fallbackFile.write()) {
					marshaller.write(value, out);
				}
			}
		});
	}
}
