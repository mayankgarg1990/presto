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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HiveEncryptionMetadataProvider
{
    private final Set<EncryptionMetadataSource> sources;

    @Inject
    public HiveEncryptionMetadataProvider(Set<EncryptionMetadataSource> sources)
    {
        this.sources = ImmutableSet.copyOf(requireNonNull(sources, "sources is null"));
    }

    public Optional<Map<String, EncryptionMetadata>> getEncryptionMetadata(Table table, Map<String, Partition> partitions)
    {
        for (EncryptionMetadataSource source : sources) {
            Optional<Map<String, EncryptionMetadata>> result = source.getEncryptionMetadata(table, partitions);
            if (result.isPresent()) {
                return result.map(ImmutableMap::copyOf);
            }
        }

        return Optional.empty();
    }

    public Optional<EncryptionMetadata> getEncryptionMetadata(Table table)
    {
        for (EncryptionMetadataSource source : sources) {
            Optional<EncryptionMetadata> result = source.getEncryptionMetadata(table);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }
}
