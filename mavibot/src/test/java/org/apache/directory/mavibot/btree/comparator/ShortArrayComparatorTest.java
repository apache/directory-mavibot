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
 * Test the ShortArrayComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ShortArrayComparatorTest
{
    @Test
    public void testShortArrayComparator()
    {
        ShortArrayComparator comparator = ShortArrayComparator.INSTANCE;

        // Check equality
        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( new short[]
            {}, new short[]
            {} ) );
        assertEquals( 0, comparator.compare( new short[]
            { ( short ) 1, ( short ) 2 }, new short[]
            { ( short ) 1, ( short ) 2 } ) );

        // The first short[] is > the second
        assertEquals( 1, comparator.compare( new short[]
            {}, null ) );
        assertEquals( 1, comparator.compare( new short[]
            { ( short ) 1 }, null ) );
        assertEquals( 1, comparator.compare( new short[]
            { ( short ) 1, ( short ) 2 }, new short[]
            { ( short ) 1, ( short ) 1 } ) );
        assertEquals( 1, comparator.compare( new short[]
            { ( short ) 1, ( short ) 2, ( short ) 1 }, new short[]
            { ( short ) 1, ( short ) 2 } ) );
        assertEquals( 1, comparator.compare( new short[]
            { ( short ) 1, ( short ) 2 }, new short[]
            { ( short ) 1, ( short ) 1, ( short ) 2 } ) );

        // The first short[] is < the second
        assertEquals( -1, comparator.compare( null, new short[]
            {} ) );
        assertEquals( -1, comparator.compare( null, new short[]
            { ( short ) 1, ( short ) 2 } ) );
        assertEquals( -1, comparator.compare( null, new short[]
            { ( short ) -1, ( short ) 2 } ) );
        assertEquals( -1, comparator.compare( new short[]
            {}, new short[]
            { ( short ) 1, ( short ) 2 } ) );
        assertEquals( -1, comparator.compare( new short[]
            {}, new short[]
            { ( short ) -1, ( short ) 2 } ) );
        assertEquals( -1, comparator.compare( new short[]
            { ( short ) -1, ( short ) 1 }, new short[]
            { ( short ) 1, ( short ) 1, ( short ) 2 } ) );
        short[] array = new short[3];
        array[0] = ( short ) 1;
        array[1] = ( short ) 2;
        assertEquals( -1, comparator.compare( new short[]
            { ( short ) 1, ( short ) 2 }, array ) );
    }
}
