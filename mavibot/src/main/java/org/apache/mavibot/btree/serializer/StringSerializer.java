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


/**
 * The String serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class StringSerializer implements ElementSerializer<String>
{
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
     */
    public String deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 4 );

        int len = ( in[0] << 24 ) +
            ( ( in[1] & 0xFF ) << 16 ) +
            ( ( in[2] & 0xFF ) << 8 ) +
            ( in[3] & 0xFF );

        switch ( len )
        {
            case 0:
                return "";

            case -1:
                return null;

            default:
                try
                {
                    in = bufferHandler.read( len );

                    return new String( in, 0, len, "UTF-8" );
                }
                catch ( UnsupportedEncodingException uee )
                {
                    // if this happens something is really strange
                    throw new RuntimeException( uee );
                }
        }
    }
}
