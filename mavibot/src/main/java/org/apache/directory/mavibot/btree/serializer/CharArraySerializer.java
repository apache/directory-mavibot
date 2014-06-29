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

import org.apache.directory.mavibot.btree.comparator.CharArrayComparator;
import org.apache.directory.mavibot.btree.exception.SerializerCreationException;


/**
 * A serializer for a char[].
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class CharArraySerializer extends AbstractElementSerializer<char[]>
{
    /** A static instance of a CharArraySerializer */
    public static final CharArraySerializer INSTANCE = new CharArraySerializer();

    /**
     * Create a new instance of CharArraySerializer
     */
    private CharArraySerializer()
    {
        super( CharArrayComparator.INSTANCE );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( char[] element )
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
                bytes = new byte[len * 2 + 4];
                int pos = 4;

                bytes[0] = ( byte ) ( len >>> 24 );
                bytes[1] = ( byte ) ( len >>> 16 );
                bytes[2] = ( byte ) ( len >>> 8 );
                bytes[3] = ( byte ) ( len );

                for ( char c : element )
                {
                    bytes[pos++] = ( byte ) ( c >>> 8 );
                    bytes[pos++] = ( byte ) ( c );
                }
        }

        return bytes;
    }


    /**
     * A static method used to deserialize a char array from a byte array.
     *
     * @param in The byte array containing the char array
     * @return A char[]
     */
    public static char[] deserialize( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 4 ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int len = IntSerializer.deserialize( in );

        switch ( len )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[len];

                for ( int i = 4; i < len * 2 + 4; i += 2 )
                {
                    result[i] = Character.valueOf( ( char ) ( ( in[i] << 8 ) +
                        ( in[i + 1] & 0xFF ) ) );
                }

                return result;
        }
    }


    /**
     * A method used to deserialize a char array from a byte array.
     *
     * @param in The byte array containing the char array
     * @return A char[]
     */
    public char[] fromBytes( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length - start < 4 ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int len = IntSerializer.deserialize( in, start );

        switch ( len )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[len];

                for ( int i = 4; i < len * 2 + 4; i += 2 )
                {
                    result[i] = Character.valueOf( ( char ) ( ( in[i] << 8 ) +
                        ( in[i + 1] & 0xFF ) ) );
                }

                return result;
        }
    }


    /**
     * A method used to deserialize a char array from a byte array.
     *
     * @param in The byte array containing the char array
     * @return A char[]
     */
    public char[] fromBytes( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 4 ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int len = IntSerializer.deserialize( in );

        switch ( len )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[len];

                for ( int i = 4; i < len * 2 + 4; i += 2 )
                {
                    result[i] = Character.valueOf( ( char ) ( ( in[i] << 8 ) +
                        ( in[i + 1] & 0xFF ) ) );
                }

                return result;
        }
    }


    /**
     * {@inheritDoc}
     */
    public char[] deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 4 );

        int len = IntSerializer.deserialize( in );

        switch ( len )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[len];
                byte[] buffer = bufferHandler.read( len * 2 );

                for ( int i = 0; i < len * 2; i += 2 )
                {
                    result[i] = Character.valueOf( ( char ) ( ( buffer[i] << 8 ) +
                        ( buffer[i + 1] & 0xFF ) ) );
                }

                return result;
        }
    }


    /**
     * {@inheritDoc}
     */
    public char[] deserialize( ByteBuffer buffer ) throws IOException
    {
        int len = buffer.getInt();

        switch ( len )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[len];

                for ( int i = 0; i < len; i++ )
                {
                    result[i] = buffer.getChar();
                }

                return result;
        }
    }
}
