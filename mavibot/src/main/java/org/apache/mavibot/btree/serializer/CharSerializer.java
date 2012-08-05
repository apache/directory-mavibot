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


import java.io.IOException;


/**
 * The Character serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class CharSerializer implements ElementSerializer<Character>
{
    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Character element )
    {
        byte[] bytes = new byte[2];
        char value = element.charValue();

        bytes[0] = ( byte ) ( value >>> 8 );
        bytes[1] = ( byte ) ( value );

        return bytes;
    }


    /**
     * A static method used to deserialize a Character from a byte array.
     * @param in The byte array containing the Character
     * @return A Character
     */
    public static Character deserialize( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 8 ) )
        {
            throw new RuntimeException( "Cannot extract a Character from a buffer with not enough bytes" );
        }

        return Character.valueOf( ( char ) ( ( in[0] << 8 ) +
            ( in[1] & 0xFF ) ) );
    }


    /**
     * {@inheritDoc}
     */
    public Character deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 2 );

        return deserialize( in );
    }
}
