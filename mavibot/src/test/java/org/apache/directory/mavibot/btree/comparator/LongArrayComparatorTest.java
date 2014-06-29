/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.directory.mavibot.btree.comparator;


import static org.junit.Assert.assertEquals;

import org.junit.Test;


/**
 * Test the LongArrayComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class LongArrayComparatorTest
{
    @Test
    public void testLongArrayComparator()
    {
        LongArrayComparator comparator = LongArrayComparator.INSTANCE;

        // Check equality
        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( new long[]
            {}, new long[]
            {} ) );
        assertEquals( 0, comparator.compare( new long[]
            { 1L, 2L }, new long[]
            { 1L, 2L } ) );

        // The first long[] is > the second
        assertEquals( 1, comparator.compare( new long[]
            {}, null ) );
        assertEquals( 1, comparator.compare( new long[]
            { 1L }, null ) );
        assertEquals( 1, comparator.compare( new long[]
            { 1L, 2L }, new long[]
            { 1L, 1L } ) );
        assertEquals( 1, comparator.compare( new long[]
            { 1L, 2L, 1L }, new long[]
            { 1L, 2L } ) );
        assertEquals( 1, comparator.compare( new long[]
            { 1L, 2L }, new long[]
            { 1L, 1L, 2L } ) );

        // The first long[] is < the second
        assertEquals( -1, comparator.compare( null, new long[]
            {} ) );
        assertEquals( -1, comparator.compare( null, new long[]
            { 1L, 2L } ) );
        assertEquals( -1, comparator.compare( null, new long[]
            { -1L, 2L } ) );
        assertEquals( -1, comparator.compare( new long[]
            {}, new long[]
            { 1L, 2L } ) );
        assertEquals( -1, comparator.compare( new long[]
            {}, new long[]
            { -1L, 2L } ) );
        assertEquals( -1, comparator.compare( new long[]
            { -1L, 1L }, new long[]
            { 1L, 1L, 2L } ) );
        long[] array = new long[3];
        array[0] = 1L;
        array[1] = 2L;
        assertEquals( -1, comparator.compare( new long[]
            { 1L, 2L }, array ) );
    }
}
