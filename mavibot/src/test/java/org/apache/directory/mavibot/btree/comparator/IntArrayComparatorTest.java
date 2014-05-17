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
 * Test the IntArrayComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class IntArrayComparatorTest
{
    @Test
    public void testIntArrayComparator()
    {
        IntArrayComparator comparator = IntArrayComparator.INSTANCE;

        // Check equality
        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( new int[]
            {}, new int[]
            {} ) );
        assertEquals( 0, comparator.compare( new int[]
            { 1, 2 }, new int[]
            { 1, 2 } ) );

        // The first int[] is > the second
        assertEquals( 1, comparator.compare( new int[]
            {}, null ) );
        assertEquals( 1, comparator.compare( new int[]
            { 1 }, null ) );
        assertEquals( 1, comparator.compare( new int[]
            { 1, 2 }, new int[]
            { 1, 1 } ) );
        assertEquals( 1, comparator.compare( new int[]
            { 1, 2, 1 }, new int[]
            { 1, 2 } ) );
        assertEquals( 1, comparator.compare( new int[]
            { 1, 2 }, new int[]
            { 1, 1, 2 } ) );

        // The first int[] is < the second
        assertEquals( -1, comparator.compare( null, new int[]
            {} ) );
        assertEquals( -1, comparator.compare( null, new int[]
            { 1, 2 } ) );
        assertEquals( -1, comparator.compare( null, new int[]
            { -1, 2 } ) );
        assertEquals( -1, comparator.compare( new int[]
            {}, new int[]
            { 1, 2 } ) );
        assertEquals( -1, comparator.compare( new int[]
            {}, new int[]
            { -1, 2 } ) );
        assertEquals( -1, comparator.compare( new int[]
            { -1, 1 }, new int[]
            { 1, 1, 2 } ) );
        int[] array = new int[3];
        array[0] = 1;
        array[1] = 2;
        assertEquals( -1, comparator.compare( new int[]
            { 1, 2 }, array ) );
    }
}
