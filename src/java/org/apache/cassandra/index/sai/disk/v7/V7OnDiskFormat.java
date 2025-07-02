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

package org.apache.cassandra.index.sai.disk.v7;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v6.V6OnDiskFormat;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.store.IndexInput;

public class V7OnDiskFormat extends V6OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final V7OnDiskFormat instance = new V7OnDiskFormat();

    private static final Set<IndexComponentType> LITERAL_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                 IndexComponentType.META,
                                                                                 IndexComponentType.TERMS_DATA,
                                                                                 IndexComponentType.POSTING_LISTS,
                                                                                 IndexComponentType.DOC_LENGTHS);

    @Override
    public Set<IndexComponentType> perIndexComponentTypes(AbstractType<?> validator)
    {
        // Vector types are technically "literal" (frozen) but should not have DOC_LENGTHS
        // which is only for text search BM25 functionality
        if (validator.isVector())
            return super.perIndexComponentTypes(validator);
        if (TypeUtil.isLiteral(validator))
            return LITERAL_COMPONENTS;
        return super.perIndexComponentTypes(validator);
    }

    @Override
    public int jvectorFileFormatVersion()
    {
        // Before version EC, we write JVector format 2. Version EB introduced the ability for jvector to read format 4,
        // so we can safely start writing it for versions EC (V7) and later while maintaining proper backward
        // compatibility.
        return 4;
    }

    @Override
    public void validateIndexComponent(IndexComponent.ForRead component, boolean checksum)
    {
        if (component.isCompletionMarker())
            return;

        IndexContext context = component.parent().context();
        if (context != null && context.isVector() && component.parent().version().onOrAfter(Version.EC))
        {
            try (IndexInput input = component.openInput())
            {
                // We can't validate TERMS_DATA with checksum because the checksum was computed incorrectly through
                // V7. See https://github.com/riptano/cndb/issues/14656. We can still call the basic validate method
                // which does not check the checksum. (The issue is in the way the checksum was computed. It didn't
                // include the header/footer bytes, and for multi-segment builds, it didn't include the bytes from
                // all previous segments, which is the design for all index components to date.)
                if (!checksum || component.componentType() == IndexComponentType.TERMS_DATA)
                    SAICodecUtils.validate(input, getExpectedEarliestVersion(context, component.componentType()));
                else
                    SAICodecUtils.validateChecksum(input, getExpectedEarliestVersion(context, component.componentType()));
            }
            catch (Throwable e)
            {
                logger.warn(component.parent().logMessage("{} failed for index component {} on SSTable {}"),
                            (checksum ? "Checksum validation" : "Validation"),
                            component,
                            component.parent().descriptor(),
                            e);
                if (e instanceof IOException)
                    throw new UncheckedIOException((IOException) e);
                if (e.getCause() instanceof IOException)
                    throw new UncheckedIOException((IOException) e.getCause());
                throw Throwables.unchecked(e);
            }
        }
        else
        {
            super.validateIndexComponent(component, checksum);
        }
    }
}
