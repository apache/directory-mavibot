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
 * Test the ByteArrayComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ByteArrayComparatorTest
{
    @Test
    public void testByteArrayComparator()
    {
        ByteArrayComparator comparator = ByteArrayComparator.INSTANCE;

        // Check equality
        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( new byte[]
            {}, new byte[]
            {} ) );
        assertEquals( 0, comparator.compare( new byte[]
            { 0x01, 0x02 }, new byte[]
            { 0x01, 0x02 } ) );

        // The first byte[] is > the second
        assertEquals( 1, comparator.compare( new byte[]
            {}, null ) );
        assertEquals( 1, comparator.compare( new byte[]
            { 0x01 }, null ) );
        assertEquals( 1, comparator.compare( new byte[]
            { 0x01, 0x02 }, new byte[]
            { 0x01, 0x01 } ) );
        assertEquals( 1, comparator.compare( new byte[]
            { 0x01, 0x02, 0x01 }, new byte[]
            { 0x01, 0x02 } ) );
        assertEquals( 1, comparator.compare( new byte[]
            { 0x01, 0x02 }, new byte[]
            { 0x01, 0x01, 0x02 } ) );

        // The first byte[] is < the second
        assertEquals( -1, comparator.compare( null, new byte[]
            {} ) );
        assertEquals( -1, comparator.compare( null, new byte[]
            { 0x01, 0x02 } ) );
        assertEquals( -1, comparator.compare( null, new byte[]
            { ( byte ) 0xFF, 0x02 } ) );
        assertEquals( -1, comparator.compare( new byte[]
            {}, new byte[]
            { 0x01, 0x02 } ) );
        assertEquals( -1, comparator.compare( new byte[]
            {}, new byte[]
            { ( byte ) 0xFF, 0x02 } ) );
        assertEquals( -1, comparator.compare( new byte[]
            { ( byte ) 0xFF, 0x01 }, new byte[]
            { 0x01, 0x01, 0x02 } ) );
        byte[] array = new byte[3];
        array[0] = 0x01;
        array[1] = 0x02;
        assertEquals( -1, comparator.compare( new byte[]
            { 0x01, 0x02 }, array ) );

    }
}
