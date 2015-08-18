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

import no.digipost.cache.fallback.FallbackFile.Resolver;
import no.digipost.cache.loader.Callables;
import no.digipost.cache.loader.Loader;
import no.digipost.cache.loader.LoaderDecorator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

public class DiskStorageFallbackLoaderDecorator<K, V> implements LoaderDecorator<K, V> {
	private final Path fallbackDirectory;
	private final FileNamingStrategy<? super K> fallbackFileNamingStrategy;
	private final Marshaller<V> marshaller;

	public DiskStorageFallbackLoaderDecorator(Path fallbackDirectory, FileNamingStrategy<? super K> fallbackFileNamingStrategy, Marshaller<V> marshaller) {
		this.fallbackDirectory = fallbackDirectory;
		this.fallbackFileNamingStrategy = fallbackFileNamingStrategy;
		this.marshaller = marshaller;
	}

	public Loader<K, V> decorate(Callable<V> loader) {
		return decorate(Callables.<K, V>toLoader(loader));
	}

	@Override
	public Loader<K, V> decorate(Loader<K, V> loader) {
		Resolver<K> resolver = new FallbackFile.Resolver<>(fallbackDirectory, fallbackFileNamingStrategy);
		if (Files.isRegularFile(fallbackDirectory)) {
			throw new IllegalStateException(fallbackDirectory + " should either be non-existing or a directory, but refers to an existing file.");
		}
		try {
			Files.createDirectories(fallbackDirectory);
		} catch (IOException e) {
			throw new RuntimeException("Unable to prepare the directory to store cache values for fallback: "
					+ e.getClass().getSimpleName() + " '" + e.getMessage() + "'", e);
		}
		return new DiskStorageFallbackLoader<K, V>(resolver, loader, marshaller);
	}

}
