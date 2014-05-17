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

import org.apache.directory.mavibot.btree.comparator.BooleanComparator;
import org.apache.directory.mavibot.btree.exception.SerializerCreationException;


/**
 * The Boolean serializer.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BooleanSerializer extends AbstractElementSerializer<Boolean>
{
    /** A static instance of a BooleanSerializer */
    public static final BooleanSerializer INSTANCE = new BooleanSerializer();

    /**
     * Create a new instance of BooleanSerializer
     */
    private BooleanSerializer()
    {
        super( BooleanComparator.INSTANCE );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Boolean element )
    {
        byte[] bytes = new byte[1];

        return serialize( bytes, 0, element );
    }


    /**
     * Serialize a boolean
     *
     * @param value the value to serialize
     * @return The byte[] containing the serialized boolean
     */
    public static byte[] serialize( boolean element )
    {
        byte[] bytes = new byte[1];

        return serialize( bytes, 0, element );
    }


    /**
     * Serialize a boolean
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized boolean
     * @param value the value to serialize
     * @return The byte[] containing the serialized boolean
     */
    public static byte[] serialize( byte[] buffer, int start, boolean element )
    {
        buffer[start] = element ? ( byte ) 0x01 : ( byte ) 0x00;

        return buffer;
    }


    /**
     * A static method used to deserialize a Boolean from a byte array.
     *
     * @param in The byte array containing the boolean
     * @return A boolean
     */
    public static Boolean deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a Boolean from a byte array.
     *
     * @param in The byte array containing the boolean
     * @param start the position in the byte[] we will deserialize the boolean from
     * @return A boolean
     */
    public static Boolean deserialize( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 1 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Boolean from a buffer with not enough bytes" );
        }

        return in[start] == 0x01;
    }


    /**
     * A method used to deserialize a Boolean from a byte array.
     *
     * @param in The byte array containing the boolean
     * @return A boolean
     */
    public Boolean fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A method used to deserialize a Boolean from a byte array.
     *
     * @param in The byte array containing the boolean
     * @param start the position in the byte[] we will deserialize the boolean from
     * @return A boolean
     */
    public Boolean fromBytes( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 1 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a Boolean from a buffer with not enough bytes" );
        }

        return in[start] == 0x01;
    }


    /**
     * {@inheritDoc}
     */
    public Boolean deserialize( ByteBuffer buffer ) throws IOException
    {
        return buffer.get() != 0x00;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 1 );

        return deserialize( in );
    }
}
