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
import java.util.Comparator;

import org.apache.mavibot.btree.comparator.LongComparator;


/**
 * The Long serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class LongSerializer implements ElementSerializer<Long>
{
    /** The associated comparator */
    private final Comparator<Long> comparator;


    /**
     * Create a new instance of LongSerializer
     */
    public LongSerializer()
    {
        comparator = new LongComparator();
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Long element )
    {
        byte[] bytes = new byte[8];
        long value = element.longValue();

        bytes[0] = ( byte ) ( value >>> 56 );
        bytes[1] = ( byte ) ( value >>> 48 );
        bytes[2] = ( byte ) ( value >>> 40 );
        bytes[3] = ( byte ) ( value >>> 32 );
        bytes[4] = ( byte ) ( value >>> 24 );
        bytes[5] = ( byte ) ( value >>> 16 );
        bytes[6] = ( byte ) ( value >>> 8 );
        bytes[7] = ( byte ) ( value );

        return bytes;
    }


    /**
     * A static method used to deserialize a Long from a byte array.
     * @param in The byte array containing the Long
     * @return A Long
     */
    public static Long deserialize( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 8 ) )
        {
            throw new RuntimeException( "Cannot extract a Long from a buffer with not enough bytes" );
        }

        long result = ( ( long ) in[0] << 56 ) +
            ( ( in[1] & 0xFFL ) << 48 ) +
            ( ( in[2] & 0xFFL ) << 40 ) +
            ( ( in[3] & 0xFFL ) << 32 ) +
            ( ( in[4] & 0xFFL ) << 24 ) +
            ( ( in[5] & 0xFFL ) << 16 ) +
            ( ( in[6] & 0xFFL ) << 8 ) +
            ( in[7] & 0xFFL );

        return result;
    }


    /**
     * {@inheritDoc}
     */
    public Long deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 8 );

        return deserialize( in );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int compare( Long type1, Long type2 )
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


    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<Long> getComparator()
    {
        return comparator;
    }
}
