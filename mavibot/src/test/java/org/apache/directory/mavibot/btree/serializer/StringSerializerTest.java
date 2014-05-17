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

import java.io.IOException;

import org.junit.Test;


/**
 * Test the StringSerializer class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class StringSerializerTest
{
    private static StringSerializer serializer = StringSerializer.INSTANCE;


    @Test
    public void testStringSerializer() throws IOException
    {
        String value = null;
        byte[] result = serializer.serialize( value );

        assertEquals( 4, result.length );
        assertEquals( ( byte ) 0xFF, result[0] );
        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0xFF, result[2] );
        assertEquals( ( byte ) 0xFF, result[3] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ) );

        // ------------------------------------------------------------------
        value = "";
        result = serializer.serialize( value );

        assertEquals( 4, result.length );
        assertEquals( ( byte ) 0x00, result[0] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[3] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ) );

        // ------------------------------------------------------------------
        value = "test";
        result = serializer.serialize( value );

        assertEquals( 8, result.length );
        assertEquals( ( byte ) 0x00, result[0] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x04, result[3] );
        assertEquals( 't', result[4] );
        assertEquals( 'e', result[5] );
        assertEquals( 's', result[6] );
        assertEquals( 't', result[7] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ) );

        // ------------------------------------------------------------------
        value = "L\u00E9charny";
        result = serializer.serialize( value );

        assertEquals( 13, result.length );
        assertEquals( ( byte ) 0x00, result[0] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x09, result[3] );
        assertEquals( 'L', result[4] );
        assertEquals( ( byte ) 0xC3, result[5] );
        assertEquals( ( byte ) 0xA9, result[6] );
        assertEquals( 'c', result[7] );
        assertEquals( 'h', result[8] );
        assertEquals( 'a', result[9] );
        assertEquals( 'r', result[10] );
        assertEquals( 'n', result[11] );
        assertEquals( 'y', result[12] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ) );
    }
}
