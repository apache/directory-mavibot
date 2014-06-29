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
 * Test the BooleanArrayComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BooleanArrayComparatorTest
{
    @Test
    public void testBooleanArrayComparator()
    {
        BooleanArrayComparator comparator = BooleanArrayComparator.INSTANCE;

        assertEquals( 0, comparator.compare( null, null ) );

        boolean[] b1 = new boolean[]
            { true, true, true };
        boolean[] b2 = new boolean[]
            { true, true, false };
        boolean[] b3 = new boolean[]
            { true, false, true };
        boolean[] b4 = new boolean[]
            { false, true, true };
        boolean[] b5 = new boolean[]
            { true, true };

        // 0
        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( new boolean[]
            {}, new boolean[]
            {} ) );
        assertEquals( 0, comparator.compare( b1, b1 ) );

        // -1
        assertEquals( -1, comparator.compare( null, new boolean[]
            {} ) );
        assertEquals( -1, comparator.compare( null, b1 ) );
        assertEquals( -1, comparator.compare( new boolean[]
            {}, b1 ) );
        assertEquals( -1, comparator.compare( new boolean[]
            {}, b4 ) );
        assertEquals( -1, comparator.compare( b5, b1 ) );
        assertEquals( -1, comparator.compare( b5, b3 ) );

        // 1
        assertEquals( 1, comparator.compare( new boolean[]
            {}, null ) );
        assertEquals( 1, comparator.compare( b1, null ) );
        assertEquals( 1, comparator.compare( b1, new boolean[]
            {} ) );
        assertEquals( 1, comparator.compare( b1, b2 ) );
        assertEquals( 1, comparator.compare( b1, b3 ) );
        assertEquals( 1, comparator.compare( b1, b4 ) );
        assertEquals( 1, comparator.compare( b1, b5 ) );
    }
}
