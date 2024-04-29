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
package no.digipost.cache2.fallback.disk;

import no.digipost.cache2.fallback.FallbackKeeperFailedHandler;
import no.digipost.cache2.fallback.LoaderWithFallback;
import no.digipost.cache2.fallback.marshall.Marshaller;
import no.digipost.cache2.loader.Loader;
import no.digipost.cache2.loader.LoaderDecorator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;

public class LoaderWithDiskFallbackDecorator<K, V> implements LoaderDecorator<K, V> {

	private final Path fallbackDirectory;
	private final FallbackFileNamingStrategy<? super K> fallbackFileNamingStrategy;
	private final Marshaller<V> marshaller;
	private final FallbackKeeperFailedHandler<? super K, ? super V> fallbackWriteFailedHandler;
    private final Clock clock;


	public LoaderWithDiskFallbackDecorator(
			Path fallbackDirectory, FallbackFileNamingStrategy<? super K> fallbackFileNamingStrategy, Marshaller<V> marshaller) {

		this(fallbackDirectory, fallbackFileNamingStrategy, marshaller, new FallbackKeeperFailedHandler.LogAsError());
	}

	public LoaderWithDiskFallbackDecorator(
	        Path fallbackDirectory, FallbackFileNamingStrategy<? super K> fallbackFileNamingStrategy, Marshaller<V> marshaller,
	        FallbackKeeperFailedHandler<? super K, ? super V> fallbackWriteFailedHandler) {

	    this(fallbackDirectory, fallbackFileNamingStrategy, marshaller, fallbackWriteFailedHandler, Clock.systemDefaultZone());
	}

	public LoaderWithDiskFallbackDecorator(
			Path fallbackDirectory, FallbackFileNamingStrategy<? super K> fallbackFileNamingStrategy, Marshaller<V> marshaller,
			FallbackKeeperFailedHandler<? super K, ? super V> fallbackWriteFailedHandler, Clock clock) {

		this.fallbackDirectory = fallbackDirectory;
		this.fallbackFileNamingStrategy = fallbackFileNamingStrategy;
		this.marshaller = marshaller;
		this.fallbackWriteFailedHandler = fallbackWriteFailedHandler;
        this.clock = clock;
	}

	@Override
	public Loader<K, V> decorate(Loader<? super K, V> loader) {
		FallbackFile.Resolver<K> resolver = new FallbackFile.Resolver<>(fallbackDirectory, fallbackFileNamingStrategy, clock);
		if (Files.isRegularFile(fallbackDirectory)) {
			throw new IllegalStateException(fallbackDirectory + " should either be non-existing or a directory, but refers to an existing file.");
		}
		try {
			Files.createDirectories(fallbackDirectory);
		} catch (IOException e) {
			throw new RuntimeException("Unable to prepare the directory to store cache values for fallback: "
					+ e.getClass().getSimpleName() + " '" + e.getMessage() + "'", e);
		}
		return new LoaderWithFallback<K, V>(loader, new DiskFallbackLoader<>(resolver, marshaller), new DiskFallbackKeeper<>(resolver, marshaller), fallbackWriteFailedHandler);
	}

}
