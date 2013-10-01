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
package org.apache.directory.mavibot.btree.managed;


import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.apache.directory.mavibot.btree.util.Strings;


/**
 * A class storing either a key, or an offset to the key on the page's byte[]
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class KeyHolder<K>
{
    /** The deserialized key */
    private K key;

    /** The ByteBuffer storing the key */
    private ByteBuffer raw;

    /** The Key serializer */
    private ElementSerializer<K> keySerializer;


    /* No Qualifier */KeyHolder( K key, ElementSerializer<K> keySerializer )
    {
        this.key = key;
        this.keySerializer = keySerializer;
        raw = ByteBuffer.wrap( keySerializer.serialize( key ) );
    }


    /* No Qualifier */KeyHolder( K key, ByteBuffer raw, ElementSerializer<K> keySerializer )
    {
        this.key = key;
        this.keySerializer = keySerializer;

        if ( raw != null )
        {
            this.raw = raw;
        }
        else
        {
            raw = ByteBuffer.wrap( keySerializer.serialize( key ) );
        }
    }


    /**
     * @return the key
     */
    public K getKey()
    {
        if ( key == null )
        {
            try
            {
                key = keySerializer.deserialize( raw );
            }
            catch ( IOException ioe )
            {
                // Nothing we can do here...
            }
        }

        return key;
    }


    /**
     * @param key the Key to store in into the KeyHolder
     */
    public void setKey( K key )
    {
        this.key = key;
        raw = ByteBuffer.wrap( keySerializer.serialize( key ) );
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "KeyHolder[" );

        if ( key == null )
        {
            if ( raw.isDirect() )
            {
                sb.append( "raw" );
            }
            else
            {
                Strings.dumpBytes( raw.array() );
            }
        }
        else
        {
            sb.append( key );
        }

        sb.append( "]" );

        return sb.toString();
    }
}
