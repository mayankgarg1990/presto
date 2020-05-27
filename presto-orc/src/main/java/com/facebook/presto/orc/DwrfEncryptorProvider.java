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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.DwrfEncryption;
import com.facebook.presto.orc.metadata.EncryptionGroup;
import com.facebook.presto.orc.metadata.KeyProvider;
import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DwrfEncryptorProvider
{
    private final Map<Integer, Integer> nodeToGroupMap;
    private final EncryptionLibrary encryptionLibrary;

    private DwrfEncryptorProvider(EncryptionLibrary encryptionLibrary, Map<Integer, Integer> nodeToGroupMap)
    {
        this.encryptionLibrary = requireNonNull(encryptionLibrary, "encryption is null");
        this.nodeToGroupMap = requireNonNull(nodeToGroupMap, "nodeToGroupMap is null");
    }

    public static DwrfEncryptorProvider createDwrfEncryptorProvider(DwrfEncryption dwrfEncryption, List<OrcType> types)
    {
        return new DwrfEncryptorProvider(
                getDecryptionLibrary(dwrfEncryption.getKeyProvider()),
                createNodeToGroupMap(
                        dwrfEncryption.getEncryptionGroups().stream()
                                .map(EncryptionGroup::getNodes)
                                .collect(toImmutableList()),
                        types));
    }

    public static DwrfEncryptorProvider createDwrfEncryptorProvider(DwrfWriterEncryption dwrfEncryption, List<OrcType> types)
    {
        return new DwrfEncryptorProvider(
                getDecryptionLibrary(dwrfEncryption.getKeyProvider()),
                createNodeToGroupMap(
                        dwrfEncryption.getWriterEncryptionGroups().stream()
                                .map(WriterEncryptionGroup::getNodes)
                                .collect(toImmutableList()),
                        types));
    }

    public static Map<Integer, Integer> createNodeToGroupMap(List<List<Integer>> encryptionGroups, List<OrcType> types)
    {
        // We don't use an immutableMap builder so that we can check what's already been added
        Map nodeToGroupMapBuilder = new HashMap();
        for (int groupId = 0; groupId < encryptionGroups.size(); groupId++) {
            for (Integer nodeId : encryptionGroups.get(groupId)) {
                createNodeToGroupMap(groupId, nodeId, types, nodeToGroupMapBuilder);
            }
        }
        return ImmutableMap.copyOf(nodeToGroupMapBuilder);
    }

    private static Map<Integer, Integer> createNodeToGroupMap(int groupId, int nodeId, List<OrcType> types, Map<Integer, Integer> nodeToGroupMapBuilder)
    {
        if (nodeToGroupMapBuilder.containsKey(nodeId) && nodeToGroupMapBuilder.get(nodeId) != groupId) {
            throw new VerifyException(format("Column or sub-column %s belongs to more than one encryption group: %s and %s", nodeId, nodeToGroupMapBuilder.get(nodeId), groupId));
        }
        nodeToGroupMapBuilder.put(nodeId, groupId);
        OrcType type = types.get(nodeId);
        for (int childId = nodeId + 1; childId < nodeId + type.getFieldCount() + 1; childId++) {
            createNodeToGroupMap(groupId, childId, types, nodeToGroupMapBuilder);
        }
        return nodeToGroupMapBuilder;
    }

    private static EncryptionLibrary getDecryptionLibrary(KeyProvider keyProvider)
    {
        switch (keyProvider) {
            case CRYPTO_SERVICE:
                return getCryptoServiceDecryptor();
            default:
                return getUnknownDecryptor();
        }
    }

    private static EncryptionLibrary getCryptoServiceDecryptor()
    {
        // TODO: configure and pass through
        return new TestingEncryptionLibrary();
    }

    private static EncryptionLibrary getUnknownDecryptor()
    {
        // TODO: configure default decryptor and pass through
        return new TestingEncryptionLibrary();
    }

    public DwrfEncryptor createEncryptor(Slice keyMetadata)
    {
        return new DwrfEncryptor(keyMetadata, encryptionLibrary);
    }

    public Map<Integer, Integer> getNodeToGroupMap()
    {
        return nodeToGroupMap;
    }
}
