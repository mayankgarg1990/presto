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

import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

public class DwrfEncryptor
{
    private final Slice keyMetadata;
    private final EncryptionLibrary encryptionLibrary;

    public DwrfEncryptor(Slice keyMetadata, EncryptionLibrary encryptionLibrary)
    {
        this.keyMetadata = requireNonNull(keyMetadata, "keyMetadata is null");
        this.encryptionLibrary = requireNonNull(encryptionLibrary, "encryptionLibrary is null");
    }

    public Slice decrypt(byte[] input, int offset, int length)
    {
        return encryptionLibrary.decrypt(keyMetadata, input, offset, length);
    }

    public Slice encrypt(byte[] input, int offset, int length)
    {
        return encryptionLibrary.encrypt(keyMetadata, input, offset, length);
    }

    public int maxEncryptedLength(int unencryptedLength)
    {
        return encryptionLibrary.getMaxEncryptedLength(keyMetadata, unencryptedLength);
    }
}
