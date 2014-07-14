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

import org.apache.directory.mavibot.btree.comparator.LongComparator;
import org.apache.directory.mavibot.btree.exception.SerializerCreationException;


/**
 * The Long serializer.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class LongSerializer extends AbstractElementSerializer<Long>
{
    /** A static instance of a LongSerializer */
    public final static LongSerializer INSTANCE = new LongSerializer();

    /**
     * Create a new instance of LongSerializer
     */
    private LongSerializer()
    {
        super( LongComparator.INSTANCE );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Long element )
    {
        return serialize( element.longValue() );
    }


    /**
     * Serialize an long
     *
     * @param value the value to serialize
     * @return The byte[] containing the serialized long
     */
    public static byte[] serialize( long value )
    {
        byte[] bytes = new byte[8];

        return serialize( bytes, 0, value );
    }


    /**
     * Serialize an long
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized long
     * @param value the value to serialize
     * @return The byte[] containing the serialized long
     */
    public static byte[] serialize( byte[] buffer, int start, long value )
    {
        buffer[start] = ( byte ) ( value >>> 56 );
        buffer[start + 1] = ( byte ) ( value >>> 48 );
        buffer[start + 2] = ( byte ) ( value >>> 40 );
        buffer[start + 3] = ( byte ) ( value >>> 32 );
        buffer[start + 4] = ( byte ) ( value >>> 24 );
        buffer[start + 5] = ( byte ) ( value >>> 16 );
        buffer[start + 6] = ( byte ) ( value >>> 8 );
        buffer[start + 7] = ( byte ) ( value );

        return buffer;
    }


    /**
     * A static method used to deserialize a Long from a byte array.
     * @param in The byte array containing the Long
     * @param start the position in the byte[] we will deserialize the long from
     * @return A Long
     */
    public static Long deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize an Integer from a byte array.
     * @param in The byte array containing the Integer
     * @param start the position in the byte[] we will deserialize the long from
     * @return An Integer
     */
    public static Long deserialize( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 8 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Long from a buffer with not enough bytes" );
        }

        long result = ( ( long ) in[start] << 56 ) +
            ( ( in[start + 1] & 0x00FFL ) << 48 ) +
            ( ( in[start + 2] & 0x00FFL ) << 40 ) +
            ( ( in[start + 3] & 0x00FFL ) << 32 ) +
            ( ( in[start + 4] & 0x00FFL ) << 24 ) +
            ( ( in[start + 5] & 0x00FFL ) << 16 ) +
            ( ( in[start + 6] & 0x00FFL ) << 8 ) +
            ( in[start + 7] & 0x00FFL );

        return result;
    }


    /**
     * A method used to deserialize a Long from a byte array.
     * @param in The byte array containing the Long
     * @param start the position in the byte[] we will deserialize the long from
     * @return A Long
     */
    public Long fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A method used to deserialize an Integer from a byte array.
     * @param in The byte array containing the Integer
     * @param start the position in the byte[] we will deserialize the long from
     * @return An Integer
     */
    public Long fromBytes( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 8 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Long from a buffer with not enough bytes" );
        }

        long result = ( ( long ) in[start] << 56 ) +
            ( ( in[start + 1] & 0xFFL ) << 48 ) +
            ( ( in[start + 2] & 0xFFL ) << 40 ) +
            ( ( in[start + 3] & 0xFFL ) << 32 ) +
            ( ( in[start + 4] & 0xFFL ) << 24 ) +
            ( ( in[start + 5] & 0xFFL ) << 16 ) +
            ( ( in[start + 6] & 0xFFL ) << 8 ) +
            ( in[start + 7] & 0xFFL );

        return result;
    }


    /**
     * {@inheritDoc}
     */
    public Long deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 8 );

        return deserialize( in );
    }


    /**
     * {@inheritDoc}
     */
    public Long deserialize( ByteBuffer buffer ) throws IOException
    {
        return buffer.getLong();
    }
}
