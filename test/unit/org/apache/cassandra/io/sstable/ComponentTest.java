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

package org.apache.cassandra.io.sstable;

import java.util.HashSet;
import java.util.function.Function;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component.Type;
import org.apache.cassandra.io.sstable.SSTableFormatTest.Format1;
import org.apache.cassandra.io.sstable.SSTableFormatTest.Format2;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.mockito.Mockito;

import static org.apache.cassandra.io.sstable.SSTableFormatTest.factory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class ComponentTest
{
    private static final String SECOND = "second";
    private static final String FIRST = "first";

    static
    {
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            SSTableFormatTest.configure(new Config.SSTableConfig(), new BtiFormat.BtiFormatFactory(), factory("first", Format1.class), factory("second", Format2.class));
            return config;
        });
    }

    @Test
    public void testTypes()
    {
        Function<Type, Component> componentFactory = Mockito.mock(Function.class);

        // do not allow to define a type with the same name or repr as the existing type for this or parent format
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> Type.createSingleton(Components.Types.TOC.name, Components.Types.TOC.repr + "x", true, Format1.class));
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> Type.createSingleton(Components.Types.TOC.name + "x", Components.Types.TOC.repr, true, Format2.class));

        // allow to define a format with other name and repr
        Type t1 = Type.createSingleton("ONE", "One.db", true, Format1.class);

        // allow to define a format with the same name and repr for two different formats
        Type t2f1 = Type.createSingleton("TWO", "Two.db", true, Format1.class);
        Type t2f2 = Type.createSingleton("TWO", "Two.db", true, Format2.class);
        assertThat(t2f1).isNotEqualTo(t2f2);

        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> Type.createSingleton(null, "-Three.db", true, Format1.class));

        assertThat(Type.fromRepresentation("should be custom", BtiFormat.getInstance())).isSameAs(Components.Types.CUSTOM);
        assertThat(Type.fromRepresentation(Components.Types.TOC.repr, BtiFormat.getInstance())).isSameAs(Components.Types.TOC);
        assertThat(Type.fromRepresentation(t1.repr, DatabaseDescriptor.getSSTableFormats().get(FIRST))).isSameAs(t1);
        assertThat(Type.fromRepresentation(t2f1.repr, DatabaseDescriptor.getSSTableFormats().get(FIRST))).isSameAs(t2f1);
        assertThat(Type.fromRepresentation(t2f2.repr, DatabaseDescriptor.getSSTableFormats().get(SECOND))).isSameAs(t2f2);
    }

    @Test
    public void testComponents()
    {
        Type t3f1 = Type.createSingleton("THREE", "Three.db", true, Format1.class);
        Type t3f2 = Type.createSingleton("THREE", "Three.db", true, Format2.class);
        Type t4f1 = Type.create("FOUR", ".*-Four.db", true, Format1.class);
        Type t4f2 = Type.create("FOUR", ".*-Four.db", true, Format2.class);

        Component c1 = t3f1.getSingleton();
        Component c2 = t3f2.getSingleton();

        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> t3f1.createComponent(t3f1.repr));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> t3f2.createComponent(t3f2.repr));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> t4f1.getSingleton());
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> t4f2.getSingleton());

        assertThat(Component.parse(t3f1.repr, DatabaseDescriptor.getSSTableFormats().get(FIRST))).isSameAs(c1);
        assertThat(Component.parse(t3f2.repr, DatabaseDescriptor.getSSTableFormats().get("second"))).isSameAs(c2);
        assertThat(c1).isNotEqualTo(c2);
        assertThat(c1.type).isSameAs(t3f1);
        assertThat(c2.type).isSameAs(t3f2);

        Component c3 = Component.parse("abc-Four.db", DatabaseDescriptor.getSSTableFormats().get(FIRST));
        Component c4 = Component.parse("abc-Four.db", DatabaseDescriptor.getSSTableFormats().get("second"));
        assertThat(c3.type).isSameAs(t4f1);
        assertThat(c4.type).isSameAs(t4f2);
        assertThat(c3.name).isEqualTo("abc-Four.db");
        assertThat(c4.name).isEqualTo("abc-Four.db");
        assertThat(c3).isNotEqualTo(c4);
        assertThat(c3).isNotEqualTo(c1);
        assertThat(c4).isNotEqualTo(c2);

        Component c5 = Component.parse("abc-Five.db", DatabaseDescriptor.getSSTableFormats().get(FIRST));
        assertThat(c5.type).isSameAs(Components.Types.CUSTOM);
        assertThat(c5.name).isEqualTo("abc-Five.db");

        Component c6 = Component.parse("Data.db", DatabaseDescriptor.getSSTableFormats().get("second"));
        assertThat(c6.type).isSameAs(Components.Types.DATA);
        assertThat(c6).isSameAs(Components.DATA);

        HashSet<Component> s1 = Sets.newHashSet(Component.getSingletonsFor(Format1.class));
        HashSet<Component> s2 = Sets.newHashSet(Component.getSingletonsFor(Format2.class));
        assertThat(s1).contains(c1, Components.DATA, Components.STATS, Components.COMPRESSION_INFO);
        assertThat(s2).contains(c2, Components.DATA, Components.STATS, Components.COMPRESSION_INFO);
        assertThat(s1).doesNotContain(c2);
        assertThat(s2).doesNotContain(c1);

        assertThat(Sets.newHashSet(Component.getSingletonsFor(DatabaseDescriptor.getSSTableFormats().get(FIRST)))).isEqualTo(s1);
        assertThat(Sets.newHashSet(Component.getSingletonsFor(DatabaseDescriptor.getSSTableFormats().get("second")))).isEqualTo(s2);
    }

    @Test
    public void testFromRepresentationSame()
    {
        // Test exact string matches for standard components
        assertSame(SSTableFormat.Components.Types.DATA, Component.Type.fromRepresentation("Data.db", BtiFormat.getInstance()));
        assertSame(SSTableFormat.Components.Types.DATA, Component.Type.fromRepresentation("Data.db", null));
        assertSame(BtiFormat.Components.Types.PARTITION_INDEX, Component.Type.fromRepresentation("Partitions.db", BtiFormat.getInstance()));
        assertSame(BtiFormat.Components.Types.ROW_INDEX, Component.Type.fromRepresentation("Rows.db", BtiFormat.getInstance()));
        assertSame(SSTableFormat.Components.Types.FILTER, Component.Type.fromRepresentation("Filter.db", BtiFormat.getInstance()));
        assertSame(SSTableFormat.Components.Types.FILTER, Component.Type.fromRepresentation("Filter.db", null));
        assertSame(SSTableFormat.Components.Types.COMPRESSION_INFO, Component.Type.fromRepresentation("CompressionInfo.db", BtiFormat.getInstance()));
        assertSame(SSTableFormat.Components.Types.COMPRESSION_INFO, Component.Type.fromRepresentation("CompressionInfo.db", null));
        assertSame(SSTableFormat.Components.Types.STATS, Component.Type.fromRepresentation("Statistics.db", BtiFormat.getInstance()));
        assertSame(SSTableFormat.Components.Types.STATS, Component.Type.fromRepresentation("Statistics.db", null));
        assertSame(SSTableFormat.Components.Types.DIGEST, Component.Type.fromRepresentation("Digest.crc32", BtiFormat.getInstance()));
        assertSame(SSTableFormat.Components.Types.DIGEST, Component.Type.fromRepresentation("Digest.crc32", null));
        assertSame(SSTableFormat.Components.Types.CRC, Component.Type.fromRepresentation("CRC.db", BtiFormat.getInstance()));
        assertSame(SSTableFormat.Components.Types.CRC, Component.Type.fromRepresentation("CRC.db", null));
        assertSame(SSTableFormat.Components.Types.TOC, Component.Type.fromRepresentation("TOC.txt", BtiFormat.getInstance()));
        assertSame(SSTableFormat.Components.Types.TOC, Component.Type.fromRepresentation("TOC.txt", null));
    }

    @Test
    public void testFromRepresentationSecondaryIndexPattern()
    {
        // Test regex pattern matching for SECONDARY_INDEX
        assertEquals(BtiFormat.Components.Types.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_.db", BtiFormat.getInstance()));
        assertEquals(BtiFormat.Components.Types.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_something.db", BtiFormat.getInstance()));
        assertEquals(BtiFormat.Components.Types.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_idx_name.db", BtiFormat.getInstance()));
        assertEquals(BtiFormat.Components.Types.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_a1b2c3.db", BtiFormat.getInstance()));
        assertEquals(BtiFormat.Components.Types.SECONDARY_INDEX, Component.Type.fromRepresentation("SI_test_index_123.db", BtiFormat.getInstance()));
    }

    @Test
    public void testFromRepresentationCustomComponent()
    {
        // Test that unknown components return CUSTOM
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("Unknown.db", BtiFormat.getInstance()));
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("CustomComponent.db", BtiFormat.getInstance()));
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("SomethingElse.txt", BtiFormat.getInstance()));
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("Random-File.dat", BtiFormat.getInstance()));
    }

    @Test
    public void testFromRepresentationEmptyString()
    {
        // Test that empty string returns CUSTOM
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("", BtiFormat.getInstance()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromRepresentationNullInput()
    {
        // Test that null input throws IllegalArgumentException
        Component.Type.fromRepresentation(null, BtiFormat.getInstance());
    }

    @Test
    public void testFromRepresentationNonMatches()
    {
        // Test that similar but not exact matches return CUSTOM
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("data.db", BtiFormat.getInstance()));  // lowercase
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("Data.DB", BtiFormat.getInstance()));  // different case
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("DataExtra.db", BtiFormat.getInstance()));  // extra chars
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("SI.db", BtiFormat.getInstance()));  // missing underscore
        assertEquals(BtiFormat.Components.Types.CUSTOM, Component.Type.fromRepresentation("SI_", BtiFormat.getInstance()));  // missing .db
    }

    @Test
    public void testComponentParse()
    {
        // Test the parse method which uses fromRepresentation internally
        Component data = Component.parse("Data.db", BtiFormat.getInstance());
        assertEquals(BtiFormat.Components.Types.DATA, data.type);
        assertEquals("Data.db", data.name);

        Component secondaryIndex = Component.parse("SI_myindex.db", BtiFormat.getInstance());
        assertEquals(BtiFormat.Components.Types.SECONDARY_INDEX, secondaryIndex.type);
        assertEquals("SI_myindex.db", secondaryIndex.name);

        Component custom = Component.parse("CustomFile.db", BtiFormat.getInstance());
        assertEquals(BtiFormat.Components.Types.CUSTOM, custom.type);
        assertEquals("CustomFile.db", custom.name);
    }

    @Test
    public void testComponentEquality()
    {
        Component comp1 = new Component(SSTableFormat.Components.Types.DATA, "test");
        Component comp2 = new Component(SSTableFormat.Components.Types.DATA, "test");

        assertEquals(comp1, comp2);
        assertEquals(comp1.hashCode(), comp2.hashCode());

        // Test singletons
        assertSame(SSTableFormat.Components.Types.DATA, Component.parse("Data.db", null).type);
        assertSame(SSTableFormat.Components.Types.FILTER, Component.parse("Filter.db", null).type);
    }

    @Test
    public void testComponentName()
    {
        assertEquals("DATA", BtiFormat.Components.Types.DATA.name);
        assertEquals("FILTER", BtiFormat.Components.Types.FILTER.name);
        assertEquals("STATS", BtiFormat.Components.Types.STATS.name);
    }

    @Test
    public void testSecondaryIndexNotSingleton()
    {
        // Secondary index components should not be singletons
        Component si1 = Component.parse("SI_index1.db", BtiFormat.getInstance());
        Component si2 = Component.parse("SI_index2.db", BtiFormat.getInstance());

        assertEquals(BtiFormat.Components.Types.SECONDARY_INDEX, si1.type);
        assertEquals(BtiFormat.Components.Types.SECONDARY_INDEX, si2.type);
        assertNotEquals(si1, si2);  // Different names
        assertNotSame(si1, si2);  // Different instances
    }

    @Test
    public void testFromRepresentationConsistency()
    {
        String[] inputs = {"Data.db", "SI_test.db", "CustomFile.db"};

        for (String input : inputs) {
            Component.Type type1 = Component.Type.fromRepresentation(input, BtiFormat.getInstance());
            Component.Type type2 = Component.Type.fromRepresentation(input, BtiFormat.getInstance());
            assertSame("Same input should return same enum instance", type1, type2);
        }
    }
}
