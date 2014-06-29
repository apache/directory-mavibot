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
 * Test the ByteSerializer class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ByteSerializerTest
{
    private static ByteSerializer serializer = ByteSerializer.INSTANCE;


    @Test
    public void testByteSerializer() throws IOException
    {
        byte value = 0x00;
        byte[] result = ByteSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).byteValue() );

        // ------------------------------------------------------------------
        value = 0x01;
        result = ByteSerializer.serialize( value );

        assertEquals( ( byte ) 0x01, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).byteValue() );

        // ------------------------------------------------------------------
        value = 0x7F;
        result = ByteSerializer.serialize( value );

        assertEquals( ( byte ) 0x7F, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).byteValue() );

        // ------------------------------------------------------------------
        value = ( byte ) 0x80;
        result = ByteSerializer.serialize( value );

        assertEquals( ( byte ) 0x80, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).byteValue() );

        // ------------------------------------------------------------------
        value = ( byte ) 0xFF;
        result = ByteSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).byteValue() );
    }
}
