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

import org.apache.mavibot.btree.comparator.IntComparator;


/**
 * The Integer serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class IntSerializer implements ElementSerializer<Integer>
{
    /** The associated comparator */
    private final Comparator<Integer> comparator;


    /**
     * Create a new instance of IntSerializer
     */
    public IntSerializer()
    {
        comparator = new IntComparator();
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Integer element )
    {
        byte[] bytes = new byte[4];
        int value = element.intValue();

        bytes[0] = ( byte ) ( value >>> 24 );
        bytes[1] = ( byte ) ( value >>> 16 );
        bytes[2] = ( byte ) ( value >>> 8 );
        bytes[3] = ( byte ) ( value );

        return bytes;
    }


    /**
     * A static method used to deserialize an Integer from a byte array.
     * @param in The byte array containing the Integer
     * @return An Integer
     */
    public static Integer deserialize( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 4 ) )
        {
            throw new RuntimeException( "Cannot extract a Integer from a buffer with not enough bytes" );
        }

        return ( in[0] << 24 ) +
            ( ( in[1] & 0xFF ) << 16 ) +
            ( ( in[2] & 0xFF ) << 8 ) +
            ( in[3] & 0xFF );
    }


    /**
     * {@inheritDoc}
     */
    public Integer deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 4 );

        return deserialize( in );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int compare( Integer type1, Integer type2 )
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
    public Comparator<Integer> getComparator()
    {
        return comparator;
    }
}
