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

import org.apache.directory.mavibot.btree.comparator.IntComparator;
import org.apache.directory.mavibot.btree.exception.SerializerCreationException;


/**
 * The Integer serializer.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class IntSerializer extends AbstractElementSerializer<Integer>
{
    /** A static instance of a IntSerializer */
    public static final IntSerializer INSTANCE = new IntSerializer();

    /**
     * Create a new instance of IntSerializer
     */
    private IntSerializer()
    {
        super( IntComparator.INSTANCE );
    }


    /**
     * A static method used to deserialize an Integer from a byte array.
     * @param in The byte array containing the Integer
     * @return An Integer
     */
    public static Integer deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize an Integer from a byte array.
     * @param in The byte array containing the Integer
     * @param start the position in the byte[] we will deserialize the int from
     * @return An Integer
     */
    public static Integer deserialize( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 4 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Integer from a buffer with not enough bytes" );
        }

        return ( in[start] << 24 ) +
            ( ( in[start + 1] & 0xFF ) << 16 ) +
            ( ( in[start + 2] & 0xFF ) << 8 ) +
            ( in[start + 3] & 0xFF );
    }


    /**
     * A method used to deserialize an Integer from a byte array.
     * @param in The byte array containing the Integer
     * @return An Integer
     */
    public Integer fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A method used to deserialize an Integer from a byte array.
     * @param in The byte array containing the Integer
     * @param start the position in the byte[] we will deserialize the int from
     * @return An Integer
     */
    public Integer fromBytes( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 4 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Integer from a buffer with not enough bytes" );
        }

        return ( in[start] << 24 ) +
            ( ( in[start + 1] & 0xFF ) << 16 ) +
            ( ( in[start + 2] & 0xFF ) << 8 ) +
            ( in[start + 3] & 0xFF );
    }


    /**
     * {@inheritDoc}
     */
    public Integer deserialize( ByteBuffer buffer ) throws IOException
    {
        return buffer.getInt();
    }


    /**
     * {@inheritDoc}
     */
    public Integer deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 4 );

        return deserialize( in );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Integer element )
    {
        return serialize( element.intValue() );
    }


    /**
     * Serialize an int
     *
     * @param value the value to serialize
     * @return The byte[] containing the serialized int
     */
    public static byte[] serialize( int value )
    {
        byte[] bytes = new byte[4];

        return serialize( bytes, 0, value );
    }


    /**
     * Serialize an int
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized int
     * @param value the value to serialize
     * @return The byte[] containing the serialized int
     */
    public static byte[] serialize( byte[] buffer, int start, int value )
    {
        buffer[start] = ( byte ) ( value >>> 24 );
        buffer[start + 1] = ( byte ) ( value >>> 16 );
        buffer[start + 2] = ( byte ) ( value >>> 8 );
        buffer[start + 3] = ( byte ) ( value );

        return buffer;
    }
}
