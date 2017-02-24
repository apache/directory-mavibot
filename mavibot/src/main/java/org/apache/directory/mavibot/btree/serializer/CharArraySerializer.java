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
        int nbChars = -1;

        if ( element != null )
        {
            nbChars = element.length;
        }

        byte[] bytes;

        switch ( nbChars )
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
                bytes = new byte[nbChars * 2 + 4];
                int pos = 4;

                bytes[0] = ( byte ) ( nbChars >>> 24 );
                bytes[1] = ( byte ) ( nbChars >>> 16 );
                bytes[2] = ( byte ) ( nbChars >>> 8 );
                bytes[3] = ( byte ) ( nbChars );

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

        int nbChars = IntSerializer.deserialize( in );

        switch ( nbChars )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[nbChars];

                for ( int i = 4; i < nbChars * 2 + 4; i += 2 )
                {
                    result[i] = ( char ) ( ( in[i] << 8 ) + ( in[i + 1] & 0xFF ) );
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
    @Override
    public char[] fromBytes( byte[] in, int start )
    {
        if ( ( in == null ) || ( in.length - start < 4 ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int nbChars = IntSerializer.deserialize( in, start );

        switch ( nbChars )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[nbChars];

                for ( int i = 4; i < nbChars * 2 + 4; i += 2 )
                {
                    result[i] = ( char ) ( ( in[i] << 8 ) + ( in[i + 1] & 0xFF ) );
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
    @Override
    public char[] fromBytes( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 4 ) )
        {
            throw new SerializerCreationException( "Cannot extract a byte[] from a buffer with not enough bytes" );
        }

        int nbChars = IntSerializer.deserialize( in );

        switch ( nbChars )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[nbChars];

                for ( int i = 4; i < nbChars * 2 + 4; i += 2 )
                {
                    result[i] = ( char ) ( ( in[i] << 8 ) + ( in[i + 1] & 0xFF ) );
                }

                return result;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public char[] deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 4 );

        int nbChars = IntSerializer.deserialize( in );

        switch ( nbChars )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[nbChars];
                byte[] buffer = bufferHandler.read( nbChars * 2 );

                for ( int i = 0; i < nbChars * 2; i += 2 )
                {
                    result[i] = ( char ) ( ( buffer[i] << 8 ) + ( buffer[i + 1] & 0xFF ) );
                }

                return result;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public char[] deserialize( ByteBuffer buffer ) throws IOException
    {
        int nbChars = buffer.getInt();

        switch ( nbChars )
        {
            case 0:
                return new char[]
                    {};

            case -1:
                return null;

            default:
                char[] result = new char[nbChars];

                for ( int i = 0; i < nbChars; i++ )
                {
                    result[i] = buffer.getChar();
                }

                return result;
        }
    }
}
