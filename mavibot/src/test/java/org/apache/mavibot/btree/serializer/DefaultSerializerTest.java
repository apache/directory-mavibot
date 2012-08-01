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

import org.junit.Test;


/**
 * Test the DefaultSerializer class
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class DefaultSerializerTest
{
    @Test
    public void testDefaultSerializerIntegerString()
    {
        DefaultSerializer<Integer, String> serializer = new DefaultSerializer<Integer, String>( Integer.class,
            String.class );

        byte[] keyBytes = serializer.serializeKey( 25 );

        assertEquals( 0x00, keyBytes[0] );
        assertEquals( 0x00, keyBytes[1] );
        assertEquals( 0x00, keyBytes[2] );
        assertEquals( 0x19, keyBytes[3] );

        byte[] valueBytes = serializer.serializeValue( "test" );

        assertEquals( 0x00, valueBytes[0] );
        assertEquals( 0x00, valueBytes[1] );
        assertEquals( 0x00, valueBytes[2] );
        assertEquals( 0x04, valueBytes[3] );
        assertEquals( 't', valueBytes[4] );
        assertEquals( 'e', valueBytes[5] );
        assertEquals( 's', valueBytes[6] );
        assertEquals( 't', valueBytes[7] );

        int key = serializer.deserializeKey( keyBytes );

        assertEquals( 25, key );

        String value = serializer.deserializeValue( valueBytes );

        assertEquals( "test", value );
    }
}
