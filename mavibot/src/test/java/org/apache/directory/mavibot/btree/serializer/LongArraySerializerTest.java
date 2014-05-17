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
package org.apache.directory.mavibot.btree.serializer;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;


/**
 * Test the LongArraySerializer class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class LongArraySerializerTest
{
    LongArraySerializer longArraySerializer = LongArraySerializer.INSTANCE;
    
    @Test
    @Ignore
    public void testLongArraySerializer() throws IOException
    {
        long[] value = null;
        byte[] result = longArraySerializer.serialize( value );
        int pos = 0;

        assertEquals( 4, result.length );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );

        assertEquals( value, longArraySerializer.deserialize( new BufferHandler( result ) ) );

        // ------------------------------------------------------------------
        value = new long[]{};
        result = longArraySerializer.serialize( value );
        pos = 0;

        assertEquals( 4, result.length );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );

        assertTrue( Arrays.equals( value, longArraySerializer.deserialize( new BufferHandler( result ) ) ) );

        // ------------------------------------------------------------------
        value = new long[]{ 1L };
        result = longArraySerializer.serialize( value );
        pos = 0;

        assertEquals( 12, result.length );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x01, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x01, result[pos++] );

        assertTrue( Arrays.equals( value, longArraySerializer.deserialize( new BufferHandler( result ) ) ) );
        
        // ------------------------------------------------------------------
        value = new long[]{ 1L, 0x00000000FFFFFFFFL, 0xFFFFFFFFFFFFFFFFL };
        result = longArraySerializer.serialize( value );
        pos = 0;

        assertEquals( 28, result.length );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x03, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x01, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0x00, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );
        assertEquals( ( byte ) 0xFF, result[pos++] );

        assertTrue( Arrays.equals( value, longArraySerializer.deserialize( new BufferHandler( result ) ) ) );
    }
}
