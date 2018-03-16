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
package no.digipost.cache2.inmemory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public abstract class CacheConfig implements ConfiguresCaffeine {

	private static final Logger LOG = LoggerFactory.getLogger(CacheConfig.class);


    public static final CacheConfig useSoftValues = onCacheBuilder(builder -> {
		LOG.info("Using soft references for caching. See http://docs.oracle.com/javase/1.5.0/docs/api/java/lang/ref/SoftReference.html");
		return builder.softValues();
	});

	public static CacheConfig expireAfterAccess(final Duration expiryTime) {
		return onCacheBuilder(builder -> {
			LOG.info("Expires values {} ms after last access", expiryTime.toMillis());
			return builder.expireAfterAccess(expiryTime.toMillis(), TimeUnit.MILLISECONDS);
        });
	}

	public static CacheConfig expireAfterWrite(final Duration expiryTime) {
		return onCacheBuilder(builder -> {
			LOG.info("Expire values {} ms after they are written to the cache", expiryTime.toMillis());
			return builder.expireAfterWrite(expiryTime.toMillis(), TimeUnit.MILLISECONDS);
        });
	}

	public static CacheConfig initialCapacity(final int initCapacity) {
		return onCacheBuilder(builder -> {
			LOG.info("Initial capacity = {}" , initCapacity);
			return builder.initialCapacity(initCapacity);
	    });
	}

	public static CacheConfig maximumSize(final long size) {
		return onCacheBuilder(builder -> {
			LOG.info("Maximum size = {}", size);
			return builder.maximumSize(size);
		});
	}

	public static CacheConfig recordStats() {
		return onCacheBuilder(builder -> {
			LOG.info("Recording stats");
			return builder.recordStats();
		});
	}

	static CacheConfig clockTicker(Clock clock) {
	    return onCacheBuilder(builder -> {
	        LOG.info("Using a {} as the clock source", clock.getClass().getName());
	        return builder.ticker(new Ticker() {
	            @Override
	            public long read() {
	                return clock.millis() * 1000000;
	            }
	        });
	    });
	}

	static final CacheConfig systemClockTicker = clockTicker(Clock.systemDefaultZone());

	static final CacheConfig logRemoval = onCacheBuilder(builder -> builder.removalListener(
	        (key, value, reason) -> Cache.LOG.info("Removing '{}' from cache (key={}). Cause: {}.", value, key, reason)));


	private static CacheConfig onCacheBuilder(ConfiguresCaffeine configurer) {
	    return new CacheConfig() {
            @Override
            public Caffeine<Object, Object> configure(Caffeine<Object, Object> builder) {
                return configurer.configure(builder);
            }
	    };
	}

    protected CacheConfig() {
	}
}
