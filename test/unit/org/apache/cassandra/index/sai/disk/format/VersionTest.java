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
package org.apache.cassandra.index.sai.disk.format;

import java.util.Optional;
import javax.annotation.Nullable;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.in;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionTest
{
    @BeforeClass
    public static void initialise() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testVersionsSorted()
    {
        Version previous = null;
        for (Version version : Version.ALL)
        {
            if (previous != null)
            {
                assertTrue(previous.onOrAfter(version));
                assertTrue(previous.after(version));
                assertFalse(version.onOrAfter(previous));
                assertFalse(version.after(previous));
            }
            previous = version;
        }
    }

    @Test
    public void supportedVersionsWillParse()
    {
        assertEquals(Version.AA, Version.parse("aa"));
        assertEquals(Version.BA, Version.parse("ba"));
        assertEquals(Version.CA, Version.parse("ca"));
    }

    @Test
    public void unsupportedOrInvalidVersionsDoNotParse()
    {
        assertThatThrownBy(() -> Version.parse(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("ab")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("abc")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAfterMethod()
    {
        // Do some basic checks, doesn't need to be updated for each new format.
        assertTrue(Version.ED.after(Version.EC));
        assertTrue(Version.EC.after(Version.EB));
        assertTrue(Version.EB.after(Version.DC));
        assertTrue(Version.DC.after(Version.DB));
        assertTrue(Version.DB.after(Version.CA));
        assertTrue(Version.CA.after(Version.BA));
        assertTrue(Version.BA.after(Version.AA));

        assertFalse(Version.AA.after(Version.BA));
        assertFalse(Version.AA.after(Version.AA));
    }

    @Test
    public void testOnOrAfterMethod()
    {
        // Do some basic checks, doesn't need to be updated for each new format.
        assertTrue(Version.ED.onOrAfter(Version.ED));
        assertTrue(Version.ED.onOrAfter(Version.EC));
        assertTrue(Version.CA.onOrAfter(Version.BA));
        assertTrue(Version.BA.onOrAfter(Version.AA));

        assertFalse(Version.AA.onOrAfter(Version.BA));
        assertFalse(Version.BA.onOrAfter(Version.CA));
    }

    // Specific test for AA because it doesn't use the same file formatter than other versions.
    @Test
    public void testAAFileFormatting()
    {
        assertFileFormatting("filename-SAI_GroupMeta.db", Version.AA, 0, IndexComponentType.GROUP_META, null);
        assertFileFormatting("filename-SAI_TokenValues_1.db", Version.AA, 1, IndexComponentType.TOKEN_VALUES, null);

        assertFileFormatting("filename-SAI_myIndex_Vector.db", Version.AA, 0, IndexComponentType.VECTOR, "myIndex");
        assertFileFormatting("filename-SAI_saiIndex_TermsData_42.db", Version.AA, 42, IndexComponentType.TERMS_DATA, "saiIndex");
    }

    @Test
    public void testFileFormatting()
    {
        for (Version version : Version.ALL)
        {
            // AA is tested separately above.
            if (version == Version.AA)
                continue;

            assertFileFormatting("filename-SAI+" + version + "+GroupMeta.db", version, 0, IndexComponentType.GROUP_META, null);
            assertFileFormatting("filename-SAI+" + version + "+1+TokenValues.db", version, 1, IndexComponentType.TOKEN_VALUES, null);

            assertFileFormatting("filename-SAI+" + version + "+myIndex+Vector.db", version, 0, IndexComponentType.VECTOR, "myIndex");
            assertFileFormatting("filename-SAI+" + version + "+42+saiIndex+TermsData.db", version, 42, IndexComponentType.TERMS_DATA, "saiIndex");
        }
    }

    @Test
    public void testInvalidFileFormatting()
    {
        assertThat(Version.tryParseFileName("")).isEmpty();
        assertThat(Version.tryParseFileName("filename-SAI_")).isEmpty();
        assertThat(Version.tryParseFileName("filemame-SAI_.db")).isEmpty();
        assertThat(Version.tryParseFileName("filemame-SAI.db")).isEmpty();
        assertThat(Version.tryParseFileName("filemame-SAI+foo.db")).isEmpty();
    }

    private void assertFileFormatting(String componentStr, Version version, int generation, IndexComponentType componentType, @Nullable String indexName)
    {
        Optional<Version.ParsedFileName> opt = Version.tryParseFileName(componentStr);
        assertThat(opt).isPresent();

        Version.ParsedFileName parsed = opt.get();
        assertEquals(version, parsed.buildId.version());
        assertEquals(generation, parsed.buildId.generation());
        assertEquals(componentType, parsed.component);
        assertEquals(indexName, parsed.indexName);

        assertThat(componentStr).endsWith(version.fileNameFormatter().format(componentType, indexName, generation));
    }
}
