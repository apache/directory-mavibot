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


/**
 * A In-Memory Value holder. The value is always present in memory.
 * 
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
/* No qualifier */class MemoryValueHolder<K, V> implements ValueHolder<K, V>
{
    /** The BTree */
    private BTree<K, V> btree;

    /** The reference to the Value instance, or null if it's not present */
    private V reference;


    /**
     * Create a new holder storing an offest and a SoftReference containing the value.
     * 
     * @param offset The offset in disk for this value
     * @param value The value to store into a SoftReference
     */
    public MemoryValueHolder( BTree<K, V> btree, V value )
    {
        this.btree = btree;
        this.reference = value;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V getValue( BTree<K, V> btree )
    {
        return reference;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( btree.getName() ).append( " : '" );

        V value = getValue( btree );

        sb.append( value );

        sb.append( "'" );

        return sb.toString();
    }
}
