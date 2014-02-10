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
import java.util.Comparator;

import org.apache.directory.mavibot.btree.comparator.ByteArrayComparator;
import org.apache.directory.mavibot.btree.exception.SerializerCreationException;


/**
 * A serializer for a byte[].
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ByteArraySerializer extends AbstractElementSerializer<byte[]>
{
    /** A static instance of a BytearraySerializer */
    public static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

    /**
     * Create a new instance of ByteArraySerializer
     */
    private ByteArraySerializer()
    {
        super( ByteArrayComparator.INSTANCE );
    }


    /**
     * Create a new instance of ByteArraySerializer with custom comparator
     */
    public ByteArraySerializer( Comparator<byte[]> comparator )
    {
        super( comparator );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( byte[] element )
    {
        int len = -1;

        if ( element != null )
        {
            len = element.length;
        }

        byte[] bytes = null;

        switch ( len )
        {
            case 0:
                bytes = new byte[4];

                bytes[0] = 0x00;
                bytes[1] = 0x00;
                bytes[2] = 0x00;
                bytes[3] = 0x00;

                break;

            case -1:
                bytes = new byte[4];

                bytes[0] = ( byte ) 0xFF;
                bytes[1] = ( byte ) 0xFF;
                bytes[2] = ( byte ) 0xFF;
                bytes[3] = ( byte ) 0xFF;

                break;

            default:
                bytes = new byte[len + 4];

                System.arraycopy( element, 0, bytes, 4, len );

                bytes[0] = ( byte ) ( len >>> 24 );
                bytes[1] = ( byte ) ( len >>> 16 );
                bytes[2] = ( byte ) ( len >>> 8 );
                bytes[3] = ( byte ) ( len );
        }

        return bytes;
    }


    /**
     * Serialize a byte[]
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized byte[]
     * @param value the value to serialize
     * @return The byte[] containing the serialized byte[]
     */
    public static byte[] serialize( byte[] buffer, int start, byte[] element )
    {
        int len = -1;

        if ( element != null )
        {
            len = element.length;
        }

        switch ( len )
        {
            case 0:
                buffer[start] = 0x00;
                buffer[start + 1] = 0x00;
                buffer[start + 2] = 0x00;
                buffer[start + 3] = 0x00;

                break;

            case -1:
                buffer[start] = ( byte ) 0xFF;
                buffer[start + 1] = ( byte ) 0xFF;
                buffer[start + 2] = ( byte ) 0xFF;
                buffer[start + 3] = ( byte ) 0xFF;

                break;

            default:

                buffer[start] = ( byte ) ( len >>> 24 );
                buffer[start + 1] = ( byte ) ( len >>> 16 );
                buffer[start + 2] = ( byte ) ( len >>> 8 );
                buffer[start + 3] = ( byte ) ( len );

                System.arraycopy( element, 0, buffer, 4 + start, len );
        }

        return buffer;

    }


    /**
     * A static method used to deserialize a byte array from a byte array.
     *
     * @param in The byte array containing the byte array
     * @return A byte[]
     */
    public static byte[] deserialize( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 4 ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int len = IntSerializer.deserialize( in );

        switch ( len )
        {
            case 0:
                return new byte[]
                    {};

            case -1:
                return null;

            default:
                byte[] result = new byte[len];
                System.arraycopy( in, 4, result, 0, len );

                return result;
        }
    }


    /**
     * A static method used to deserialize a byte array from a byte array.
     *
     * @param in The byte array containing the byte array
     * @param start the position in the byte[] we will deserialize the byte[] from
     * @return A byte[]
     */
    public static byte[] deserialize( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 4 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int len = IntSerializer.deserialize( in, start );

        switch ( len )
        {
            case 0:
                return new byte[]
                    {};

            case -1:
                return null;

            default:
                byte[] result = new byte[len];
                System.arraycopy( in, 4 + start, result, 0, len );

                return result;
        }
    }


    /**
     * A method used to deserialize a byte array from a byte array.
     *
     * @param in The byte array containing the byte array
     * @return A byte[]
     */
    public byte[] fromBytes( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 4 ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int len = IntSerializer.deserialize( in );

        switch ( len )
        {
            case 0:
                return new byte[]
                    {};

            case -1:
                return null;

            default:
                byte[] result = new byte[len];
                System.arraycopy( in, 4, result, 0, len );

                return result;
        }
    }


    /**
     * A method used to deserialize a byte array from a byte array.
     *
     * @param in The byte array containing the byte array
     * @param start the position in the byte[] we will deserialize the byte[] from
     * @return A byte[]
     */
    public byte[] fromBytes( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length < 4 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int len = IntSerializer.deserialize( in, start );

        switch ( len )
        {
            case 0:
                return new byte[]
                    {};

            case -1:
                return null;

            default:
                byte[] result = new byte[len];
                System.arraycopy( in, 4 + start, result, 0, len );

                return result;
        }
    }


    /**
     * {@inheritDoc}
     */
    public byte[] deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 4 );

        int len = IntSerializer.deserialize( in );

        switch ( len )
        {
            case 0:
                return new byte[]
                    {};

            case -1:
                return null;

            default:
                in = bufferHandler.read( len );

                return in;
        }
    }


    /**
     * {@inheritDoc}
     */
    public byte[] deserialize( ByteBuffer buffer ) throws IOException
    {
        int len = buffer.getInt();

        switch ( len )
        {
            case 0:
                return new byte[]
                    {};

            case -1:
                return null;

            default:
                byte[] bytes = new byte[len];

                buffer.get( bytes );

                return bytes;
        }
    }
}
