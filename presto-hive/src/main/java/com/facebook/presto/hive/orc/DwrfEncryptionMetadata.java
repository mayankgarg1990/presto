package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.EncryptionMetadata;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DwrfEncryptionMetadata
        implements EncryptionMetadata
{
    private final Map<String, String> columnIdToKeyIdentifier;
    private final Map<String, byte[]> keyIdentifierToKeyInformation;
    private final String encryptionAlgorithm;
    private final String partitionReferenceKey;

    public DwrfEncryptionMetadata(
            Map<String, String> columnIdToKeyIdentifier,
            Map<String, byte[]> keyIdentifierToKeyInformation,
            String encryptionAlgorithm,
            String partitionReferenceKey)
    {
        this.columnIdToKeyIdentifier = ImmutableMap.copyOf(requireNonNull(columnIdToKeyIdentifier, "columnIdToKeyIdentifier is null"));
        this.keyIdentifierToKeyInformation = ImmutableMap.copyOf(requireNonNull(keyIdentifierToKeyInformation, "keyIdentifierToKeyInformation is null"));
        this.encryptionAlgorithm = requireNonNull(encryptionAlgorithm, "encryptionAlgorithm is null");
        this.partitionReferenceKey = requireNonNull(partitionReferenceKey, "partitionReferenceKey is null");
    }

    public Map<String, String> getColumnIdToKeyIdentifier()
    {
        return columnIdToKeyIdentifier;
    }

    public Map<String, byte[]> getKeyIdentifierToKeyInformation()
    {
        return keyIdentifierToKeyInformation;
    }

    public String getEncryptionAlgorithm()
    {
        return encryptionAlgorithm;
    }

    public String getPartitionReferenceKey()
    {
        return partitionReferenceKey;
    }

    @Override
    public String toString()
    {
        return toStringHelper(DwrfEncryptionMetadata.class)
                .add("columnIdToKeyIdentifier", columnIdToKeyIdentifier)
                .add("encryptionAlgorithm", encryptionAlgorithm)
                .add("partitionKeyReference", partitionReferenceKey)
                // never print keyIdentifierToKeyInformation since it may contain sensitive information
                .toString();
    }
}
