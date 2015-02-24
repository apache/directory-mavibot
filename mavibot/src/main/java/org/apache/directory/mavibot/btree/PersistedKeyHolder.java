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
package org.apache.directory.mavibot.btree;


import java.io.IOException;

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * A class storing either a key, or an offset to the key on the page's byte[]
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * 
 * <K> The key type
 */
/* No qualifier */class PersistedKeyHolder<K> extends KeyHolder<K>
{
    /** The ByteBuffer storing the key */
    private byte[] raw;

    /** The Key serializer */
    private ElementSerializer<K> keySerializer;


    /**
     * Create a new KeyHolder instance
     * @param keySerializer The KeySerializer instance
     * @param key The key to store
     */
    /* no qualifier */PersistedKeyHolder( ElementSerializer<K> keySerializer, K key )
    {
        super( key );
        this.keySerializer = keySerializer;
        raw = keySerializer.serialize( key );
    }


    /**
     * Create a new KeyHolder instance
     * @param keySerializer The KeySerializer instance
     * @param raw the bytes representing the serialized key
     */
    /* no qualifier */PersistedKeyHolder( ElementSerializer<K> keySerializer, byte[] raw )
    {
        super( null );
        this.keySerializer = keySerializer;
        this.raw = raw;
    }


    /**
     * @return the key
     */
    /* no qualifier */K getKey()
    {
        if ( key == null )
        {
            try
            {
                key = keySerializer.fromBytes( raw );
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
    /* no qualifier */void setKey( K key )
    {
        this.key = key;
        raw = keySerializer.serialize( key );
    }


    /**
     * @return The internal serialized byte[]
     */
    /* No qualifier */byte[] getRaw()
    {
        return raw;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "PersistedKeyHolder[" );

        if ( key != null )
        {
            sb.append( key );
            sb.append( ", " );
        }
        else if ( raw != null )
        {
            K key = getKey();
            sb.append( ":" ).append( key ).append( ":," );
        }
        else
        {
            sb.append( "null," );
        }

        if ( raw != null )
        {
            sb.append( raw.length );
        }
        else
        {
            sb.append( "null" );
        }

        sb.append( "]" );

        return sb.toString();
    }
}
