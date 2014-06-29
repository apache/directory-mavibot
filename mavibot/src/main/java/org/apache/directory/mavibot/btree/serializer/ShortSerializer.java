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

import org.apache.directory.mavibot.btree.comparator.ShortComparator;
import org.apache.directory.mavibot.btree.exception.SerializerCreationException;


/**
 * The Short serializer.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ShortSerializer extends AbstractElementSerializer<Short>
{
    /** A static instance of a ShortSerializer */
    public final static ShortSerializer INSTANCE = new ShortSerializer();

    /**
     * Create a new instance of ShortSerializer
     */
    private ShortSerializer()
    {
        super( ShortComparator.INSTANCE );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Short element )
    {
        byte[] bytes = new byte[2];

        return serialize( bytes, 0, element );
    }


    /**
     * Serialize a short
     *
     * @param value the value to serialize
     * @return The byte[] containing the serialized short
     */
    public static byte[] serialize( short value )
    {
        byte[] bytes = new byte[2];

        return serialize( bytes, 0, value );
    }


    /**
     * Serialize a short
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized short
     * @param value the value to serialize
     * @return The byte[] containing the serialized short
     */
    public static byte[] serialize( byte[] buffer, int start, short value )
    {
        buffer[start] = ( byte ) ( value >>> 8 );
        buffer[start + 1] = ( byte ) ( value );

        return buffer;
    }


    /**
     * A static method used to deserialize a Short from a byte array.
     * @param in The byte array containing the Short
     * @return A Short
     */
    public static Short deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a Short from a byte array.
     * @param in The byte array containing the Short
    * @param start the position in the byte[] we will deserialize the short from
     * @return A Short
     */
    public static Short deserialize( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 2 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Short from a buffer with not enough bytes" );
        }

        return ( short ) ( ( in[start] << 8 ) + ( in[start + 1] & 0xFF ) );
    }


    /**
     * A method used to deserialize a Short from a byte array.
     * @param in The byte array containing the Short
     * @return A Short
     */
    public Short fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A method used to deserialize a Short from a byte array.
     * @param in The byte array containing the Short
     * @param start the position in the byte[] we will deserialize the short from
     * @return A Short
     */
    public Short fromBytes( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 2 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Short from a buffer with not enough bytes" );
        }

        return ( short ) ( ( in[start] << 8 ) + ( in[start + 1] & 0xFF ) );
    }


    /**
     * {@inheritDoc}
     */
    public Short deserialize( ByteBuffer buffer ) throws IOException
    {
        return buffer.getShort();
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
