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
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.mavibot.btree.comparator.LongArrayComparator;


/**
 * A serializer for a Long[].
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class LongArraySerializer implements ElementSerializer<long[]>
{
    /** The associated comparator */
    private final Comparator<long[]> comparator;


    /**
     * Create a new instance of LongSerializer
     */
    public LongArraySerializer()
    {
        comparator = new LongArrayComparator();
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( long[] element )
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
                int pos = 0;

                // The number of longs
                bytes[pos++] = ( byte ) ( len >>> 24 );
                bytes[pos++] = ( byte ) ( len >>> 16 );
                bytes[pos++] = ( byte ) ( len >>> 8 );
                bytes[pos++] = ( byte ) ( len );

                // Serialize the longs now
                for ( long value : element )
                {
                    bytes[pos++] = ( byte ) ( value >>> 56 );
                    bytes[pos++] = ( byte ) ( value >>> 48 );
                    bytes[pos++] = ( byte ) ( value >>> 40 );
                    bytes[pos++] = ( byte ) ( value >>> 32 );
                    bytes[pos++] = ( byte ) ( value >>> 24 );
                    bytes[pos++] = ( byte ) ( value >>> 16 );
                    bytes[pos++] = ( byte ) ( value >>> 8 );
                    bytes[pos++] = ( byte ) ( value );
                }
        }

        return bytes;
    }


    /**
     * {@inheritDoc}
     */
    public long[] deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 4 );

        int len = IntSerializer.deserialize( in );

        switch ( len )
        {
            case 0:
                return new long[]
                    {};

            case -1:
                return null;

            default:
                long[] longs = new long[len];

                int pos = 4;

                for ( int i = 0; i < len; i++ )
                {
                    longs[i] = ( ( long ) in[pos++] << 56 ) +
                        ( ( in[pos++] & 0xFFL ) << 48 ) +
                        ( ( in[pos++] & 0xFFL ) << 40 ) +
                        ( ( in[pos++] & 0xFFL ) << 32 ) +
                        ( ( in[pos++] & 0xFFL ) << 24 ) +
                        ( ( in[pos++] & 0xFFL ) << 16 ) +
                        ( ( in[pos++] & 0xFFL ) << 8 ) +
                        ( in[pos++] & 0xFFL );
                }

                return longs;
        }
    }


    /**
     * {@inheritDoc}
     */
    public long[] deserialize( ByteBuffer buffer ) throws IOException
    {
        int len = buffer.getInt();

        switch ( len )
        {
            case 0:
                return new long[]
                    {};

            case -1:
                return null;

            default:
                long[] longs = new long[len];

                for ( int i = 0; i < len; i++ )
                {
                    longs[i] = buffer.getLong();
                }

                return longs;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int compare( long[] type1, long[] type2 )
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
                if ( type1.length < type2.length )
                {
                    int pos = 0;

                    for ( long b1 : type1 )
                    {
                        long b2 = type2[pos];

                        if ( b1 == b2 )
                        {
                            pos++;
                        }
                        else if ( b1 < b2 )
                        {
                            return -1;
                        }
                        else
                        {
                            return 1;
                        }
                    }

                    return 1;
                }
                else
                {
                    int pos = 0;

                    for ( long b2 : type2 )
                    {
                        long b1 = type1[pos];

                        if ( b1 == b2 )
                        {
                            pos++;
                        }
                        else if ( b1 < b2 )
                        {
                            return -1;
                        }
                        else
                        {
                            return 1;
                        }
                    }

                    return -11;
                }
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<long[]> getComparator()
    {
        return comparator;
    }
}
