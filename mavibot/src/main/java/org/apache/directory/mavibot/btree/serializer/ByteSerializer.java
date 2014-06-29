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


import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.directory.mavibot.btree.comparator.ByteComparator;
import org.apache.directory.mavibot.btree.exception.SerializerCreationException;


/**
 * The Byte serializer.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ByteSerializer extends AbstractElementSerializer<Byte>
{
    /** A static instance of a ByteSerializer */
    public static final ByteSerializer INSTANCE = new ByteSerializer();

    /**
     * Create a new instance of ByteSerializer
     */
    private ByteSerializer()
    {
        super( ByteComparator.INSTANCE );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Byte element )
    {
        byte[] bytes = new byte[1];

        return serialize( bytes, 0, element );
    }


    /**
     * Serialize a byte
     *
     * @param value the value to serialize
     * @return The byte[] containing the serialized byte
     */
    public static byte[] serialize( byte value )
    {
        byte[] bytes = new byte[1];

        return serialize( bytes, 0, value );
    }


    /**
     * Serialize a byte
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized byte
     * @param value the value to serialize
     * @return The byte[] containing the serialized byte
     */
    public static byte[] serialize( byte[] buffer, int start, byte value )
    {
        buffer[start] = value;

        return buffer;
    }


    /**
     * A static method used to deserialize a Byte from a byte array.
     * @param in The byte array containing the Byte
     * @return A Byte
     */
    public static Byte deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a Byte from a byte array.
     * @param in The byte array containing the Byte
     * @param start the position in the byte[] we will deserialize the byte from
     * @return A Byte
     */
    public static Byte deserialize( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 1 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Byte from a buffer with not enough bytes" );
        }

        return in[start];
    }


    /**
     * A method used to deserialize a Byte from a byte array.
     * @param in The byte array containing the Byte
     * @return A Byte
     */
    public Byte fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A method used to deserialize a Byte from a byte array.
     * @param in The byte array containing the Byte
     * @param start the position in the byte[] we will deserialize the byte from
     * @return A Byte
     */
    public Byte fromBytes( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 1 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Byte from a buffer with not enough bytes" );
        }

        return in[start];
    }


    /**
     * {@inheritDoc}
     */
    public Byte deserialize( ByteBuffer buffer ) throws IOException
    {
        return buffer.get();
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
