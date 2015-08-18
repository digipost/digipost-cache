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

public interface FileNamingStrategy<K> {

	/**
	 * Generates a filename for the given key. The filename MUST be unique for all keys stored in the cache.
	 *
	 * The filename returned should not contain any special characters. Ideally matching pattern [a-z0-9]+ .
	 *
	 * @param key
	 * @return
	 */
	String toFilename(K key);

}
