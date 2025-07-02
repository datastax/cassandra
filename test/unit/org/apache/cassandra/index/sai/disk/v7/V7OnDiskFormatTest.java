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

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class V7OnDiskFormatTest extends SAITester
{
    private final V7OnDiskFormat format = V7OnDiskFormat.instance;

    @Test
    public void testPerIndexComponentTypesForVectorType()
    {
        // Create a vector type with float elements
        VectorType<Float> vectorType = VectorType.getInstance(FloatType.instance, 3);
        
        // Get component types for vector
        Set<IndexComponentType> vectorComponents = format.perIndexComponentTypes(vectorType);
        
        // Vector types should not include DOC_LENGTHS
        assertFalse("Vector types should not have DOC_LENGTHS component", 
                   vectorComponents.contains(IndexComponentType.DOC_LENGTHS));
        
        // But should have other components
        assertTrue(vectorComponents.contains(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(vectorComponents.contains(IndexComponentType.META));
        
        // Should call parent perIndexComponentTypes for vector types
        // Verify it has the expected components from parent (VECTOR_COMPONENTS_V3)
        assertTrue(vectorComponents.contains(IndexComponentType.PQ));
        assertTrue(vectorComponents.contains(IndexComponentType.TERMS_DATA));
        assertTrue(vectorComponents.contains(IndexComponentType.POSTING_LISTS));
    }

    @Test
    public void testPerIndexComponentTypesForLiteralType()
    {
        // Test with a literal type (UTF8Type)
        Set<IndexComponentType> literalComponents = format.perIndexComponentTypes(UTF8Type.instance);
        
        // Literal types should include DOC_LENGTHS for BM25 functionality
        assertTrue("Literal types should have DOC_LENGTHS component", 
                  literalComponents.contains(IndexComponentType.DOC_LENGTHS));
        assertTrue(literalComponents.contains(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(literalComponents.contains(IndexComponentType.META));
        assertTrue(literalComponents.contains(IndexComponentType.TERMS_DATA));
        assertTrue(literalComponents.contains(IndexComponentType.POSTING_LISTS));
        
        // Should have exactly these 5 components for literal types
        assertEquals(5, literalComponents.size());
    }

    @Test
    public void testPerIndexComponentTypesForNonLiteralType()
    {
        // Test with a non-literal type (Int32Type)
        Set<IndexComponentType> nonLiteralComponents = format.perIndexComponentTypes(Int32Type.instance);
        
        // Non-literal types should not have DOC_LENGTHS
        assertFalse("Non-literal types should not have DOC_LENGTHS component", 
                   nonLiteralComponents.contains(IndexComponentType.DOC_LENGTHS));
        
        // Should have other components from parent
        assertTrue(nonLiteralComponents.contains(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(nonLiteralComponents.contains(IndexComponentType.META));
        assertTrue(nonLiteralComponents.contains(IndexComponentType.KD_TREE));
        assertTrue(nonLiteralComponents.contains(IndexComponentType.KD_TREE_POSTING_LISTS));
    }

    @Test
    public void testJvectorFileFormatVersion()
    {
        // V7 should return format version 4
        assertEquals("V7OnDiskFormat should use JVector format version 4", 4, format.jvectorFileFormatVersion());
    }

    @Test
    public void testPerIndexComponentTypesForVectorTypeMultipleDimensions()
    {
        // Test with different vector dimensions
        VectorType<Float> smallVector = VectorType.getInstance(FloatType.instance, 2);
        VectorType<Float> largeVector = VectorType.getInstance(FloatType.instance, 128);
        
        Set<IndexComponentType> smallComponents = format.perIndexComponentTypes(smallVector);
        Set<IndexComponentType> largeComponents = format.perIndexComponentTypes(largeVector);
        
        // Both should not have DOC_LENGTHS
        assertFalse(smallComponents.contains(IndexComponentType.DOC_LENGTHS));
        assertFalse(largeComponents.contains(IndexComponentType.DOC_LENGTHS));
        
        // Both should have the same set of components
        assertEquals(smallComponents, largeComponents);
    }

    @Test
    public void testPerIndexComponentTypesConsistency()
    {
        // Test that calling perIndexComponentTypes multiple times returns consistent results
        VectorType<Float> vectorType = VectorType.getInstance(FloatType.instance, 5);
        
        Set<IndexComponentType> firstCall = format.perIndexComponentTypes(vectorType);
        Set<IndexComponentType> secondCall = format.perIndexComponentTypes(vectorType);
        
        // Results should be equal
        assertEquals(firstCall, secondCall);
        
        // Also test with literal type
        Set<IndexComponentType> firstLiteral = format.perIndexComponentTypes(UTF8Type.instance);
        Set<IndexComponentType> secondLiteral = format.perIndexComponentTypes(UTF8Type.instance);
        
        assertEquals(firstLiteral, secondLiteral);
    }

    @Test
    public void testPerIndexComponentTypesForFrozenType()
    {
        // Test with a frozen type that is not vector (should behave like non-literal)
        // Frozen types might have different behavior
        Set<IndexComponentType> frozenComponents = format.perIndexComponentTypes(Int32Type.instance.freeze());
        
        // Frozen non-literal types should not have DOC_LENGTHS
        assertFalse("Frozen non-literal types should not have DOC_LENGTHS component", 
                   frozenComponents.contains(IndexComponentType.DOC_LENGTHS));
    }

    @Test
    public void testVectorComponentsDoNotIncludeDocLengths()
    {
        // vector types should not have DOC_LENGTHS even though they are technically "literal" (frozen) types
        VectorType<Float> vectorType = VectorType.getInstance(FloatType.instance, 10);
        
        Set<IndexComponentType> components = format.perIndexComponentTypes(vectorType);
        
        assertFalse("Vector types should not include DOC_LENGTHS component",
                   components.contains(IndexComponentType.DOC_LENGTHS));
        
        Set<IndexComponentType> textComponents = format.perIndexComponentTypes(UTF8Type.instance);
        assertTrue("Text types should include DOC_LENGTHS component",
                  textComponents.contains(IndexComponentType.DOC_LENGTHS));
    }
}