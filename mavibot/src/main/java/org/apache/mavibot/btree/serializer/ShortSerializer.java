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
 * The Short serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class ShortSerializer implements ElementSerializer<Short>
{
    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Short element )
    {
        byte[] bytes = new byte[2];
        short value = element.shortValue();

        bytes[0] = ( byte ) ( value >>> 8 );
        bytes[1] = ( byte ) ( value );

        return bytes;
    }


    /**
     * A static method used to deserialize a Short from a byte array.
     * @param in The byte array containing the Short
     * @return A Short
     */
    public static Short deserialize( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 8 ) )
        {
            throw new RuntimeException( "Cannot extract a Short from a buffer with not enough bytes" );
        }

        return ( short ) ( ( in[0] << 8 ) + ( in[1] & 0xFF ) );
    }


    /**
     * {@inheritDoc}
     */
    public Short deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 2 );

        return deserialize( in );
    }
}
