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
 * The Byte serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class ByteSerializer implements ElementSerializer<Byte>
{
    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Byte element )
    {
        byte[] bytes = new byte[1];
        bytes[0] = element.byteValue();

        return bytes;
    }


    /**
     * A static method used to deserialize a Byte from a byte array.
     * @param in The byte array containing the Byte
     * @return A Byte
     */
    public static Byte deserialize( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 8 ) )
        {
            throw new RuntimeException( "Cannot extract a Byte from a buffer with not enough bytes" );
        }

        return in[0];
    }


    /**
     * {@inheritDoc}
     */
    public Byte deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 1 );

        return deserialize( in );
    }
}
