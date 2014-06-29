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
 * Test the CharSerializer class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class CharSerializerTest
{
    private static CharSerializer serializer = CharSerializer.INSTANCE;


    @Test
    public void testCharSerializer() throws IOException
    {
        char value = 0x0000;
        byte[] result = CharSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).charValue() );

        // ------------------------------------------------------------------
        value = 0x0001;
        result = CharSerializer.serialize( value );

        assertEquals( ( byte ) 0x01, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).charValue() );

        // ------------------------------------------------------------------
        value = 0x00FF;
        result = CharSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).charValue() );

        // ------------------------------------------------------------------
        value = 0x0100;
        result = CharSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x01, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).charValue() );

        // ------------------------------------------------------------------
        value = 0x7FFF;
        result = CharSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0x7F, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).charValue() );

        // ------------------------------------------------------------------
        value = 0x8000;
        result = CharSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x80, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).charValue() );

        // ------------------------------------------------------------------
        value = 0xFFFF;
        result = CharSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0xFF, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).charValue() );
    }
}
