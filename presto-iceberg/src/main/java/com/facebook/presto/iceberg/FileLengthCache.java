/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.google.common.cache.Cache;
import com.google.common.cache.ForwardingCache;

import static java.util.Objects.requireNonNull;

public class FileLengthCache
        extends ForwardingCache.SimpleForwardingCache<FileLengthCache.FileLengthCacheKey, Long>
{
    public FileLengthCache(Cache<FileLengthCacheKey, Long> delegate)
    {
        super(delegate);
    }

    // class for cache key using file path
    public static class FileLengthCacheKey
    {
        private final String filePath;

        public FileLengthCacheKey(String filePath)
        {
            this.filePath = requireNonNull(filePath, "filePath is null");
        }

        @Override
        public int hashCode()
        {
            return filePath.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FileLengthCacheKey other = (FileLengthCacheKey) obj;
            return filePath.equals(other.filePath);
        }
    }
}
