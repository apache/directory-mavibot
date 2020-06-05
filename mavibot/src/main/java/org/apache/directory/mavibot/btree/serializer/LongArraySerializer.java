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

import org.apache.directory.mavibot.btree.comparator.LongArrayComparator;


/**
 * A serializer for a Long[].
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class LongArraySerializer extends AbstractElementSerializer<long[]>
{
    /** A static instance of a LongArraySerializer */
    public static final LongArraySerializer INSTANCE = new LongArraySerializer();

    /**
     * Create a new instance of LongSerializer
     */
    private LongArraySerializer()
    {
        super( LongArrayComparator.INSTANCE );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( long[] element )
    {
        int nbLongs = -1;

        if ( element != null )
        {
            nbLongs = element.length;
        }

        byte[] bytes;
        int pos = 0;

        switch ( nbLongs )
        {
            case 0:
                bytes = new byte[4];

                // The number of Long. Here, 0
                bytes[pos++] = 0x00;
                bytes[pos++] = 0x00;
                bytes[pos++] = 0x00;
                bytes[pos] = 0x00;

                break;

            case -1:
                bytes = new byte[4];

                // The number of Long. Here, null
                bytes[pos++] = ( byte ) 0xFF;
                bytes[pos++] = ( byte ) 0xFF;
                bytes[pos++] = ( byte ) 0xFF;
                bytes[pos] = ( byte ) 0xFF;

                break;

            default:
                int dataLen = nbLongs * 8 + 4;
                bytes = new byte[dataLen];

                // The number of longs
                bytes[pos++] = ( byte ) ( nbLongs >>> 24 );
                bytes[pos++] = ( byte ) ( nbLongs >>> 16 );
                bytes[pos++] = ( byte ) ( nbLongs >>> 8 );
                bytes[pos++] = ( byte ) ( nbLongs );

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
    @Override
    public long[] deserialize( BufferHandler bufferHandler ) throws IOException
    {
        // Now, read the number of Longs
        byte[] in = bufferHandler.read( 4 );

        int nbLongs = IntSerializer.deserialize( in );

        switch ( nbLongs )
        {
            case 0:
                return new long[]
                    {};

            case -1:
                return null;

            default:
                long[] longs = new long[nbLongs];
                in = bufferHandler.read( nbLongs * 8 );

                int pos = 0;

                for ( int i = 0; i < nbLongs; i++ )
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
    @Override
    public long[] deserialize( ByteBuffer buffer ) throws IOException
    {
        // The number of longs
        int nbLongs = buffer.getInt();

        switch ( nbLongs )
        {
            case 0:
                return new long[]
                    {};

            case -1:
                return null;

            default:
                long[] longs = new long[nbLongs];

                for ( int i = 0; i < nbLongs; i++ )
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
    public long[] fromBytes( byte[] buffer ) throws IOException
    {
        int nbLongs = IntSerializer.deserialize( buffer );
        int pos = 4;

        switch ( nbLongs )
        {
            case 0:
                return new long[]
                    {};

            case -1:
                return null;

            default:
                long[] longs = new long[nbLongs];

                for ( int i = 0; i < nbLongs; i++ )
                {
                    longs[i] = LongSerializer.deserialize( buffer, pos );
                    pos += 8;
                }

                return longs;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long[] fromBytes( byte[] buffer, int pos ) throws IOException
    {
        int nbLongs = IntSerializer.deserialize( buffer, pos );
        int newPos = pos + 4;

        switch ( nbLongs )
        {
            case 0:
                return new long[]
                    {};

            case -1:
                return null;

            default:
                long[] longs = new long[nbLongs];

                for ( int i = 0; i < nbLongs; i++ )
                {
                    longs[i] = LongSerializer.deserialize( buffer, newPos );
                    newPos += 8;
                }

                return longs;
        }
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString( long[] values )
    {
        StringBuilder sb = new StringBuilder();
        
        boolean isFirst = true;
        sb.append( '[' );
        
        for ( long value : values )
        {
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                sb.append( ',' );
            }
            
            sb.append( String.format( "%016x", value ) );
        }
        
        sb.append( ']' );
        
        return sb.toString();
    }
}
