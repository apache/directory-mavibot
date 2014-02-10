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
 * Test the LongSerializer class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class LongSerializerTest
{
    @Test
    public void testLongSerializer() throws IOException
    {
        long value = 0x0000000000000000L;
        byte[] result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000000000000001L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x01, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x00000000000000FFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000000000000100L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x01, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x000000000000FFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000000000010000L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x01, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000000000FFFFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0xFF, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000000001000000L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x01, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x000000007FFFFFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0xFF, result[5] );
        assertEquals( ( byte ) 0x7F, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000000080000000L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x80, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x00000000FFFFFFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0xFF, result[5] );
        assertEquals( ( byte ) 0xFF, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000000100000000L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x01, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x000000FFFFFFFFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0xFF, result[5] );
        assertEquals( ( byte ) 0xFF, result[4] );
        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000010000000000L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x01, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0000FFFFFFFFFFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0xFF, result[5] );
        assertEquals( ( byte ) 0xFF, result[4] );
        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0xFF, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0001000000000000L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x01, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x00FFFFFFFFFFFFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0xFF, result[5] );
        assertEquals( ( byte ) 0xFF, result[4] );
        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0xFF, result[2] );
        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0x00, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x0100000000000000L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x01, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x7FFFFFFFFFFFFFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0xFF, result[5] );
        assertEquals( ( byte ) 0xFF, result[4] );
        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0xFF, result[2] );
        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0x7F, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0x8000000000000000L;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0x00, result[7] );
        assertEquals( ( byte ) 0x00, result[6] );
        assertEquals( ( byte ) 0x00, result[5] );
        assertEquals( ( byte ) 0x00, result[4] );
        assertEquals( ( byte ) 0x00, result[3] );
        assertEquals( ( byte ) 0x00, result[2] );
        assertEquals( ( byte ) 0x00, result[1] );
        assertEquals( ( byte ) 0x80, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );

        // ------------------------------------------------------------------
        value = 0xFFFFFFFFFFFFFFFFL;
        result = LongSerializer.serialize( value );

        assertEquals( ( byte ) 0xFF, result[7] );
        assertEquals( ( byte ) 0xFF, result[6] );
        assertEquals( ( byte ) 0xFF, result[5] );
        assertEquals( ( byte ) 0xFF, result[4] );
        assertEquals( ( byte ) 0xFF, result[3] );
        assertEquals( ( byte ) 0xFF, result[2] );
        assertEquals( ( byte ) 0xFF, result[1] );
        assertEquals( ( byte ) 0xFF, result[0] );

        assertEquals( value, LongSerializer.INSTANCE.deserialize( new BufferHandler( result ) ).longValue() );
    }
}
