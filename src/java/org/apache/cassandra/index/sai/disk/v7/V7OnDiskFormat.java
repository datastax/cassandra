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

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v6.V6OnDiskFormat;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
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
    public boolean validateIndexComponent(IndexComponent.ForRead component, boolean checksum)
    {
        if (component.isCompletionMarker())
            return true;

        IndexContext context = component.parent().context();
        if (context != null && context.isVector())
        {
            try (IndexInput input = component.openInput())
            {
                if (!checksum)
                    SAICodecUtils.validate(input, getExpectedEarliestVersion(context, component.componentType()));
                else if (component.componentType() == IndexComponentType.TERMS_DATA)
                    SAICodecUtils.validateChecksumSkippingHeaderAndFooter(input);
                else
                    SAICodecUtils.validateChecksum(input);
                return true;
            }
            catch (Throwable e)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(component.parent().logMessage("{} failed for index component {} on SSTable {}"),
                                 (checksum ? "Checksum validation" : "Validation"),
                                 component,
                                 component.parent().descriptor(),
                                 e);
                }
                return false;
            }
        }
        else
        {
            return super.validateIndexComponent(component, checksum);
        }
    }
}
