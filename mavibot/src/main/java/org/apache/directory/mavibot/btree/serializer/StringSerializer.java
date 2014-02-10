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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.directory.mavibot.btree.comparator.StringComparator;
import org.apache.directory.mavibot.btree.exception.SerializerCreationException;
import org.apache.directory.mavibot.btree.util.Strings;


/**
 * The String serializer.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class StringSerializer extends AbstractElementSerializer<String>
{
    /** A static instance of a StringSerializer */
    public static final StringSerializer INSTANCE = new StringSerializer();

    /**
     * Create a new instance of StringSerializer
     */
    private StringSerializer()
    {
        super( StringComparator.INSTANCE );
    }


    /**
     * Create a new instance of StringSerializer with custom comparator
     */
    public StringSerializer( Comparator<String> comparator )
    {
        super( comparator );
    }


    /**
     * A static method used to deserialize a String from a byte array.
     * @param in The byte array containing the String
     * @return A String
     */
    public static String deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a String from a byte array.
     * @param in The byte array containing the String
     * @return A String
     */
    public static String deserialize( byte[] in, int start )
    {
        int length = IntSerializer.deserialize( in, start );

        if ( length == 0xFFFFFFFF )
        {
            return null;
        }

        if ( in.length < length + 4 + start )
        {
            throw new SerializerCreationException( "Cannot extract a String from a buffer with not enough bytes" );
        }

        return Strings.utf8ToString( in, start + 4, length );
    }


    /**
     * A method used to deserialize a String from a byte array.
     * @param in The byte array containing the String
     * @return A String
     */
    public String fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A method used to deserialize a String from a byte array.
     * @param in The byte array containing the String
     * @return A String
     */
    public String fromBytes( byte[] in, int start )
    {
        int length = IntSerializer.deserialize( in, start );

        if ( length == 0xFFFFFFFF )
        {
            return null;
        }

        if ( in.length < length + start )
        {
            throw new SerializerCreationException( "Cannot extract a String from a buffer with not enough bytes" );
        }

        return Strings.utf8ToString( in, start + 4, length );
    }


    /**
     * Serialize a String. We store the length on 4 bytes, then the String
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized String
     * @param value the value to serialize
     * @return The byte[] containing the serialized String
     */
    public static byte[] serialize( byte[] buffer, int start, String element )
    {
        int len = -1;

        if ( element != null )
        {
            len = element.length();
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
                try
                {
                    byte[] strBytes = element.getBytes( "UTF-8" );

                    buffer = new byte[strBytes.length + 4];

                    System.arraycopy( strBytes, 0, buffer, 4, strBytes.length );

                    buffer[start] = ( byte ) ( strBytes.length >>> 24 );
                    buffer[start + 1] = ( byte ) ( strBytes.length >>> 16 );
                    buffer[start + 2] = ( byte ) ( strBytes.length >>> 8 );
                    buffer[start + 3] = ( byte ) ( strBytes.length );
                }
                catch ( UnsupportedEncodingException uee )
                {
                    // if this happens something is really strange
                    throw new SerializerCreationException( uee );
                }
        }

        return buffer;
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( String element )
    {
        int len = -1;

        if ( element != null )
        {
            len = element.length();
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
                char[] chars = element.toCharArray();
                byte[] tmpBytes = new byte[chars.length * 2];

                int pos = 0;
                len = 0;

                for ( char c : chars )
                {
                    if ( ( c & 0xFF80 ) == 0 )
                    {
                        tmpBytes[pos++] = ( byte ) c;
                    }
                    else if ( ( c & 0xF800 ) == 0 )
                    {
                        tmpBytes[pos++] = ( byte ) ( ( byte ) 0x00C0 | ( byte ) ( ( c & 0x07C0 ) >> 6 ) );
                        tmpBytes[pos++] = ( byte ) ( ( byte ) 0x80 | ( byte ) ( c & 0x003F ) );
                    }
                    else
                    {
                        tmpBytes[pos++] = ( byte ) ( ( byte ) 0x80 | ( byte ) ( c & 0x001F ) );
                        tmpBytes[pos++] = ( byte ) ( ( byte ) 0x80 | ( byte ) ( c & 0x07C0 ) );
                        tmpBytes[pos++] = ( byte ) ( ( byte ) 0xE0 | ( byte ) ( c & 0x7800 ) );
                    }
                }

                bytes = new byte[pos + 4];

                bytes[0] = ( byte ) ( pos >>> 24 );
                bytes[1] = ( byte ) ( pos >>> 16 );
                bytes[2] = ( byte ) ( pos >>> 8 );
                bytes[3] = ( byte ) ( pos );

                System.arraycopy( tmpBytes, 0, bytes, 4, pos );
        }

        return bytes;
    }


    /**
     * {@inheritDoc}
     * @throws IOException
     */
    public String deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 4 );

        int len = IntSerializer.deserialize( in );

        switch ( len )
        {
            case 0:
                return "";

            case -1:
                return null;

            default:
                in = bufferHandler.read( len );

                return Strings.utf8ToString( in );
        }
    }


    /**
     * {@inheritDoc}
     */
    public String deserialize( ByteBuffer buffer ) throws IOException
    {
        int len = buffer.getInt();

        switch ( len )
        {
            case 0:
                return "";

            case -1:
                return null;

            default:
                byte[] bytes = new byte[len];

                buffer.get( bytes );
                char[] chars = new char[len];
                int clen = 0;

                for ( int i = 0; i < len; i++ )
                {
                    byte b = bytes[i];

                    if ( b >= 0 )
                    {
                        chars[clen++] = ( char ) b;
                    }
                    else
                    {
                        if ( ( b & 0xE0 ) == 0 )
                        {
                            // 3 bytes long char
                            i++;
                            byte b2 = bytes[i];
                            i++;
                            byte b3 = bytes[i];
                            chars[clen++] = ( char ) ( ( ( b & 0x000F ) << 12 ) | ( ( b2 & 0x003F ) << 6 ) | ( ( b3 & 0x003F ) ) );
                        }
                        else
                        {
                            // 2 bytes long char
                            i++;
                            byte b2 = bytes[i];
                            chars[clen++] = ( char ) ( ( ( b & 0x001F ) << 6 ) | ( b2 & 0x003F ) );
                        }
                    }
                }

                return new String( chars, 0, clen );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int compare( String type1, String type2 )
    {
        if ( type1 == type2 )
        {
            return 0;
        }

        if ( type1 == null )
        {
            if ( type2 == null )
            {
                return 0;
            }
            else
            {
                return -1;
            }
        }
        else
        {
            if ( type2 == null )
            {
                return 1;
            }
            else
            {
                return type1.compareTo( type2 );
            }
        }
    }
}
