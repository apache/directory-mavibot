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

import org.apache.mavibot.btree.comparator.BooleanComparator;


/**
 * The Boolean serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class BooleanSerializer implements ElementSerializer<Boolean>
{
    /** The associated comparator */
    private final Comparator<Boolean> comparator;


    /**
     * Create a new instance of BooleanSerializer
     */
    public BooleanSerializer()
    {
        comparator = new BooleanComparator();
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serialize( Boolean element )
    {
        byte[] bytes = new byte[1];
        bytes[0] = element.booleanValue() ? ( byte ) 0x01 : ( byte ) 0x00;

        return bytes;
    }


    /**
     * A static method used to deserialize a Boolean from a byte array.
     * @param in The byte array containing the boolean
     * @return A boolean
     */
    public static Boolean deserialize( byte[] in )
    {
        if ( ( in == null ) || ( in.length < 1 ) )
        {
            throw new RuntimeException( "Cannot extract a Boolean from a buffer with not enough bytes" );
        }

        return in[0] == 0x01;
    }


    /**
     * {@inheritDoc}
     */
    public Boolean deserialize( ByteBuffer buffer ) throws IOException
    {
        return buffer.get() == 0x01;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] in = bufferHandler.read( 1 );

        return deserialize( in );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int compare( Boolean type1, Boolean type2 )
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
    public Comparator<Boolean> getComparator()
    {
        return comparator;
    }
}
