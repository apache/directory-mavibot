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
package org.apache.mavibot.btree.serializer;


import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;


/**
 * Test the IntSerializer class
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class IntSerializerTest
{
    private static IntSerializer serializer = new IntSerializer();


    @Test
    public void testIntSerializer() throws IOException
    {
        int value = 0x00000000;
        byte[] result = serializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x00000001;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0x01, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x000000FF;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x00000100;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x01, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x0000FFFF;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0xFF, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x00010000;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x01, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x00FFFFFF;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0xFF, result[2] );
        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x01000000;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x01, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x7FFFFFFF;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0x00FF, result[3] );
        assertEquals( ( byte ) 0x00FF, result[2] );
        assertEquals( ( byte ) 0x00FF, result[1] );
        assertEquals( ( byte ) 0x7F, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0x80000000;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x80, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );

        // ------------------------------------------------------------------
        value = 0xFFFFFFFF;
        result = serializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0xFF, result[2] );
        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0xFF, result[0] );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ).intValue() );
    }
}
