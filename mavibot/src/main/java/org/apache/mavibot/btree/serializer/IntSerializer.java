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


/**
 * The Integer serializer.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class IntSerializer implements ElementSerializer<Integer>
{
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
     * {@inheritDoc}
     */
    public Integer deserialize( byte[] in )
    {
        return ( in[0] << 24 ) +
            ( ( in[1] & 0xFF ) << 16 ) +
            ( ( in[2] & 0xFF ) << 8 ) +
            ( in[3] & 0xFF );
    }
}
