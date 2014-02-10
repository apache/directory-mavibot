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
 * Test the StringComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class StringComparatorTest
{
    @Test
    public void testStringComparator()
    {
        StringComparator comparator = StringComparator.INSTANCE;

        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( "", "" ) );
        assertEquals( 0, comparator.compare( "abc", "abc" ) );
        assertEquals( 1, comparator.compare( "", null ) );
        assertEquals( 1, comparator.compare( "abc", "" ) );
        assertEquals( 1, comparator.compare( "ac", "ab" ) );
        assertEquals( 1, comparator.compare( "abc", "ab" ) );
        assertEquals( -1, comparator.compare( null, "" ) );
        assertEquals( -1, comparator.compare( "ab", "abc" ) );
        assertEquals( -1, comparator.compare( "ab", "abc" ) );
    }
}
