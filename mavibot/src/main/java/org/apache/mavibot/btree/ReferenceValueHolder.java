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
package org.apache.mavibot.btree;


import java.lang.ref.SoftReference;


/**
 * A Value holder. As we may not store all the values in memory (except for an in-memory
 * BTree), we will use a SoftReference to keep a reference to a Value, and if it's null,
 * then we will load the Value from the underlying physical support, using the offset. 
 * 
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
/* No qualifier */class ReferenceValueHolder<K, V> implements ValueHolder<K, V>
{
    /** The offset for a value stored on disk */
    private long offset;

    /** The reference to the Value instance, or null if it's not present */
    private SoftReference<V> reference;


    /**
     * Create a new holder storing an offest and a SoftReference containing the value.
     * 
     * @param offset The offset in disk for this value
     * @param value The value to store into a SoftReference
     */
    public ReferenceValueHolder( long offset, V value )
    {
        this.offset = offset;
        this.reference = new SoftReference<V>( value );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V getValue( BTree<K, V> btree )
    {
        V value = reference.get();

        if ( value != null )
        {
            return value;
        }

        // We have to fetch the value from disk, using the offset now
        return fetchValue( btree );
    }


    /**
     * Retrieve the value from the disk, using the BTree and offset
     * @return
     */
    private V fetchValue( BTree<K, V> btree )
    {
        return null;
    }
}
