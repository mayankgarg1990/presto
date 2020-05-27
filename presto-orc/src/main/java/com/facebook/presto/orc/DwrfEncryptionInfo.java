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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DwrfEncryptionInfo
{
    public static final DwrfEncryptionInfo UNENCRYPTED = new DwrfEncryptionInfo(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of());
    private final List<DwrfEncryptor> dwrfEncryptors;
    private final List<Slice> keyMetadatas;
    private final Map<Integer, Integer> nodeToGroupMap;

    public DwrfEncryptionInfo(List<DwrfEncryptor> dwrfEncryptors, List<Slice> keyMetadatas, Map<Integer, Integer> nodeToGroupMap)
    {
        this.dwrfEncryptors = ImmutableList.copyOf(requireNonNull(dwrfEncryptors, "dwrfDecryptors is null"));
        this.keyMetadatas = ImmutableList.copyOf(requireNonNull(keyMetadatas, "keyMetadatas is null"));
        this.nodeToGroupMap = ImmutableMap.copyOf(requireNonNull(nodeToGroupMap, "nodeToGroupMap is null"));
    }

    public static DwrfEncryptionInfo createDwrfEncryptionInfo(DwrfDecryptorProvider decryptorProvider, List<Slice> dataKeyMetadatas, List<Slice> intermediateKeyMetadatas)
    {
        ImmutableList.Builder<DwrfEncryptor> decryptorsBuilder = ImmutableList.builder();
        verify(dataKeyMetadatas.size() == intermediateKeyMetadatas.size(), "length of keyMetadata lists do not match");
        for (int i = 0; i < dataKeyMetadatas.size(); i++) {
            DwrfEncryptor keyDecryptor = decryptorProvider.createDecryptor(intermediateKeyMetadatas.get(i));
            Slice encryptedDataKey = dataKeyMetadatas.get(i);
            verify(encryptedDataKey.hasByteArray(), "key not backed by byte array");
            Slice decryptedKeyMetadata = keyDecryptor.decrypt(encryptedDataKey.getBytes(), encryptedDataKey.byteArrayOffset(), encryptedDataKey.length());
            decryptorsBuilder.add(decryptorProvider.createDecryptor(decryptedKeyMetadata));
        }

        return new DwrfEncryptionInfo(decryptorsBuilder.build(), dataKeyMetadatas, decryptorProvider.getNodeToGroupMap());
    }

    public DwrfEncryptor getEncryptorByGroupId(int groupId)
    {
        verify(groupId < dwrfEncryptors.size(), "groupId exceeds the size of dwrfDecryptors");
        return dwrfEncryptors.get(groupId);
    }

    public Optional<DwrfEncryptor> getEncryptorByNodeId(int nodeId)
    {
        if (!nodeToGroupMap.containsKey(nodeId)) {
            return Optional.empty();
        }
        return Optional.of(getEncryptorByGroupId(nodeToGroupMap.get(nodeId)));
    }

    public Optional<Integer> getGroupByNodeId(int nodeId)
    {
        return Optional.ofNullable(nodeToGroupMap.get(nodeId));
    }

    public List<Slice> getKeyMetadatas()
    {
        return keyMetadatas;
    }
}
