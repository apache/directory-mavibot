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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.mavibot.btree.comparator.StringComparator;
import org.apache.mavibot.btree.util.Strings;


/**
 * The String serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class StringSerializer extends AbstractElementSerializer<String>
{
    /**
     * Create a new instance of StringSerializer
     */
    public StringSerializer()
    {
        super( new StringComparator() );
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
            throw new RuntimeException( "Cannot extract a String from a buffer with not enough bytes" );
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
                    throw new RuntimeException( uee );
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
                try
                {
                    byte[] strBytes = element.getBytes( "UTF-8" );

                    bytes = new byte[strBytes.length + 4];

                    System.arraycopy( strBytes, 0, bytes, 4, strBytes.length );

                    bytes[0] = ( byte ) ( strBytes.length >>> 24 );
                    bytes[1] = ( byte ) ( strBytes.length >>> 16 );
                    bytes[2] = ( byte ) ( strBytes.length >>> 8 );
                    bytes[3] = ( byte ) ( strBytes.length );
                }
                catch ( UnsupportedEncodingException uee )
                {
                    // if this happens something is really strange
                    throw new RuntimeException( uee );
                }
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

            default:
                byte[] bytes = new byte[len];

                buffer.get( bytes );

                return Strings.utf8ToString( bytes );
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
