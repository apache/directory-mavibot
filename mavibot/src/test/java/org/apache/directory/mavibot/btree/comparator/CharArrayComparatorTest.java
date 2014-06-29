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
 * Test the CharArrayComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class CharArrayComparatorTest
{
    @Test
    public void testCharArrayComparator()
    {
        CharArrayComparator comparator = CharArrayComparator.INSTANCE;

        // Check equality
        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( new char[]
            {}, new char[]
            {} ) );
        assertEquals( 0, comparator.compare( new char[]
            { 'a', 'b' }, new char[]
            { 'a', 'b' } ) );

        // The first char[] is > the second
        assertEquals( 1, comparator.compare( new char[]
            {}, null ) );
        assertEquals( 1, comparator.compare( new char[]
            { 'a' }, null ) );
        assertEquals( 1, comparator.compare( new char[]
            { 'a', 'b' }, new char[]
            { 'a', 'a' } ) );
        assertEquals( 1, comparator.compare( new char[]
            { 'a', 'b', 'a' }, new char[]
            { 'a', 'b' } ) );
        assertEquals( 1, comparator.compare( new char[]
            { 'a', 'b' }, new char[]
            { 'a', 'a', 'b' } ) );

        // The first char[] is < the second
        assertEquals( -1, comparator.compare( null, new char[]
            {} ) );
        assertEquals( -1, comparator.compare( null, new char[]
            { 'a', 'b' } ) );
        assertEquals( -1, comparator.compare( null, new char[]
            { '\uffff', 'b' } ) );
        assertEquals( -1, comparator.compare( new char[]
            {}, new char[]
            { 'a', 'b' } ) );
        assertEquals( -1, comparator.compare( new char[]
            {}, new char[]
            { '\uffff', 'b' } ) );
        assertEquals( -1, comparator.compare( new char[]
            { '0', 'a' }, new char[]
            { 'a', 'a', 'b' } ) );
        char[] array = new char[3];
        array[0] = 'a';
        array[1] = 'b';
        assertEquals( -1, comparator.compare( new char[]
            { 'a', 'b' }, array ) );
    }
}
