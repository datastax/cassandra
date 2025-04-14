/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.crypto;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import static java.util.Objects.requireNonNull;

//TODO: to decide whether we need key backups for now
/**
 * A backup of an encryption key used to encrypt/decrypt data. It's a generic
 * container so it can accommodate different types of keys and its metadata, allowing
 * to restore them.
 *
 * This class implements {@link Comparable} as keys might have dependencies between them
 * and the restoring order is important in those cases.
 */
public final class EncryptionKeyBackup implements Comparable<EncryptionKeyBackup>
{
    public enum KeyType
    {
        SYSTEM(0), // System keys must be restored first as they might be a dependency of some other keys
        REGULAR(1);

        private final int priority;

        KeyType(int priority)
        {
            this.priority = priority;
        }

        private static KeyType fromString(String keyTypeString)
        {
            for (KeyType keyType : values())
            {
                if (keyType.toString().equalsIgnoreCase(keyTypeString))
                    return keyType;
            }

            throw new IllegalArgumentException("Unknown encryption key type " + keyTypeString);
        }
    }

    /**
     * Class name of the class used to restore this key, it must be present on the classpath during restore.
     */
    public final String keyRestorerClassName;

    public final String cipher;

    public final int strength;

    /**
     * String representation of the encryption key, some key providers are external
     * services so it's possible that we don't need to store that key, hence the {@code Nullable}
     * annotation.
     */
    @Nullable public final String key;

    /**
     * A generic metadata store to include extra information.
     */
    public final Map<String, String> metadata;

    /**
     * The key type, this defines the ordering of instances of this type.
     */
    public final KeyType keyType;

    /**
     * Whether or not this key is global at cluster level, i.e. is used by all nodes in the cluster.
     *
     * Some examples are:
     * LocalFileSystemKeyProvider keys aren't global
     * ReplicatedKeyProvider keys are global
     */
    public final boolean isGlobal;

    private EncryptionKeyBackup(Builder builder)
    {
        if (builder.keyType == KeyType.SYSTEM && !builder.isGlobal)
            throw new IllegalArgumentException("System keys are always global");

        this.keyRestorerClassName = requireNonNull(builder.keyRestorerClassName);
        this.cipher = requireNonNull(builder.cipher);
        this.strength = builder.strength;
        this.key = builder.key;
        this.metadata = ImmutableMap.copyOf(requireNonNull(builder.metadata));
        this.keyType = requireNonNull(builder.keyType);
        this.isGlobal = builder.isGlobal;
    }

    public static Builder builder(String keyProviderClassName)
    {
        return new Builder(keyProviderClassName);
    }

    public static Builder builder(Class<?> keyProviderClassName)
    {
        return new Builder(keyProviderClassName.getName());
    }

    /**
     * Checks if this key is global, meaning that can be restored across different nodes, i.e. if we're doing a different
     * topology restore a key might be needed in multiple nodes.
     *
     * @return {@code true} if this key is global, {@code false} otherwise.
     */
    public boolean isGlobal()
    {
        return isGlobal;
    }

    @Nullable
    public String getMetadataProperty(String propertyName)
    {
        return metadata.get(propertyName);
    }

    /**
     * Checks if this encryption key backup metadata contains all the properties specified in {@param propertyNames},
     * throwing an {@code IllegalStateException} if there is a missing property.
     *
     * @param propertyNames the property names to check.
     * @throws NoSuchElementException if a specified property doesn't exist in the metadata store.
     */
    public void checkContainsProperties(String... propertyNames)
    {
        for (String propertyName : propertyNames)
        {
            if (!metadata.containsKey(propertyName))
                throw new NoSuchElementException("Missing encryption key property " + propertyName);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EncryptionKeyBackup that = (EncryptionKeyBackup) o;
        return strength == that.strength &&
                Objects.equals(keyRestorerClassName, that.keyRestorerClassName) &&
                Objects.equals(cipher, that.cipher) &&
                Objects.equals(key, that.key) &&
                Objects.equals(metadata, that.metadata) &&
                keyType == that.keyType &&
                isGlobal == that.isGlobal;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyRestorerClassName, cipher, strength, key, metadata, keyType, isGlobal);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("keyRestorerClassName", keyRestorerClassName)
                .add("cipher", cipher)
                .add("strength", strength)
                .add("key", "<redacted>")
                .add("metadata", metadata)
                .add("keyType", keyType)
                .add("isGlobal", isGlobal)
                .toString();
    }

    @Override
    public int compareTo(EncryptionKeyBackup o)
    {
        int cmp = Integer.compare(keyType.priority, o.keyType.priority);
        if (cmp != 0)
            return cmp;

        // If the key type is the same but this key is local,
        // we should restore the local key before.
        return (this.isGlobal == o.isGlobal) ? 0 : (this.isGlobal ? 1 : -1);
    }

    public static class Builder
    {
        private final String keyRestorerClassName;

        private String cipher;
        private int strength;
        private String key;
        private Map<String, String> metadata = new HashMap<>();
        private KeyType keyType = KeyType.REGULAR;
        private boolean isGlobal = true;

        public Builder(String keyRestorerClassName)
        {
            this.keyRestorerClassName = keyRestorerClassName;
        }

        public Builder withCipher(String cipher)
        {
            this.cipher = cipher;
            return this;
        }

        public Builder withStrength(int strength)
        {
            this.strength = strength;
            return this;
        }

        public Builder withKey(String key)
        {
            this.key = key;
            return this;
        }

        public Builder withMetadataProperty(String key, String value)
        {
            if (metadata.put(key, value) != null)
            {
                throw new IllegalArgumentException(String.format("Key %s was already present on EncryptionKeyBackup metadata", key));
            }

            return this;
        }

        public Builder withKeyType(String keyType)
        {
            return withKeyType(KeyType.fromString(keyType));
        }

        public Builder withKeyType(KeyType keyType)
        {
            this.keyType = keyType;
            return this;
        }

        public Builder withSystemType()
        {
            withKeyType(KeyType.SYSTEM);
            // System keys are global
            return isGlobal(true);
        }

        public Builder isGlobal(boolean isGlobal)
        {
            this.isGlobal = isGlobal;
            return this;
        }

        public EncryptionKeyBackup build()
        {
            return new EncryptionKeyBackup(this);
        }
    }
}
