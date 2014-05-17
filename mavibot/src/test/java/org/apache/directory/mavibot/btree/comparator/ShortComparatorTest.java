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
 * Test the ShortComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ShortComparatorTest
{
    @Test
    public void testShortComparator()
    {
        ShortComparator comparator = ShortComparator.INSTANCE;

        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( ( short ) 1, ( short ) 1 ) );
        assertEquals( 0, comparator.compare( ( short ) -1, ( short ) -1 ) );
        assertEquals( 1, comparator.compare( ( short ) 1, null ) );
        assertEquals( 1, comparator.compare( ( short ) 2, ( short ) 1 ) );
        assertEquals( 1, comparator.compare( ( short ) 3, ( short ) 1 ) );
        assertEquals( 1, comparator.compare( ( short ) 1, ( short ) -1 ) );
        assertEquals( -1, comparator.compare( null, ( short ) 1 ) );
        assertEquals( -1, comparator.compare( ( short ) 1, ( short ) 2 ) );
        assertEquals( -1, comparator.compare( ( short ) -1, ( short ) 1 ) );
    }
}
