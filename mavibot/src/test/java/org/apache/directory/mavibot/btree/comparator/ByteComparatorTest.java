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
 * Test the ByteComparator class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ByteComparatorTest
{
    @Test
    public void testByteComparator()
    {
        ByteComparator comparator = ByteComparator.INSTANCE;

        assertEquals( 0, comparator.compare( null, null ) );
        assertEquals( 0, comparator.compare( ( byte ) 0x00, ( byte ) 0x00 ) );
        assertEquals( 0, comparator.compare( ( byte ) 0xFE, ( byte ) 0xFE ) );
        assertEquals( 1, comparator.compare( ( byte ) 0x01, null ) );
        assertEquals( 1, comparator.compare( ( byte ) 0x01, ( byte ) 0x00 ) );
        assertEquals( 1, comparator.compare( ( byte ) 0x00, ( byte ) 0xFF ) );
        assertEquals( 1, comparator.compare( ( byte ) 0x7F, ( byte ) 0x01 ) );
        assertEquals( -1, comparator.compare( null, ( byte ) 0x00 ) );
        assertEquals( -1, comparator.compare( null, ( byte ) 0xFF ) );
        assertEquals( -1, comparator.compare( ( byte ) 0x00, ( byte ) 0x01 ) );
        assertEquals( -1, comparator.compare( ( byte ) 0xF0, ( byte ) 0xFF ) );
        assertEquals( -1, comparator.compare( ( byte ) 0xFF, ( byte ) 0x01 ) );
    }
}
