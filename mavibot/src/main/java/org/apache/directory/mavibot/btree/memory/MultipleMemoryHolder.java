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
package org.apache.directory.mavibot.btree.memory;


import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.UUID;


/**
 * A holder for values of duplicate keys. The values are either present in memory
 * or loaded on the fly from disk when needed.
 * 
 * @param <K> The type of the BTree key
 * @param <V> The type of the BTree value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class MultipleMemoryHolder<K, V> implements ElementHolder<V, K, V>
{
    /** The BTree */
    private BTree<K, V> btree;

    /** the offset of the value container btree. This value is set only when the parent BTree is in managed mode */
    private long valContainerOffset = -1;

    /** The reference to the Value instance, or null if it's not present. This will be null when the parent BTree is in managed mode */
    private BTree<V, V> valueContainer;

    /** This value is set only when the parent BTree is in managed mode */
    private SoftReference<BTree<V, V>> reference;

    /** the single value of the key when only one value exists for the key */
    private V singleValue;


    /**
     * Create a new holder storing an offset and a SoftReference containing the value.
     * 
     * @param offset The offset in disk for this value
     * @param value The value to store into a SoftReference
     */
    public MultipleMemoryHolder( BTree<K, V> btree, V value )
    {
        this.btree = btree;
        this.singleValue = value;
    }


    /**
     * 
     * Creates a new instance of DuplicateKeyMemoryHolder.
     * 
     * Note: the valueContainer should have a valid offset, in other words
     *       the valueContainer should always be the one that is already
     *       managed by RecordManager
     * 
     * @param btree the parent BTree
     * @param valueContainer the BTree holding the values of a duplicate key
     *        present in the parent tree
     */
    MultipleMemoryHolder( BTree<K, V> btree, BTree<V, V> valueContainer )
    {
        this.btree = btree;
        this.valueContainer = valueContainer;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V getValue( BTree<K, V> btree )
    {
        if ( valueContainer != null )
        {
            // wrong cast to please compiler
            return ( V ) valueContainer;
        }
        else
        {
            return singleValue;
        }
    }


    void switchToSingleValMode()
    {
        if ( ( valueContainer == null ) || ( reference == null ) )
        {
            return;
        }

        try
        {
            //delete the btree using offset
            singleValue = valueContainer.rootPage.getLeftMostKey();
            valueContainer = null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }


    BTree<V, V> createAndSwitchToSubTree()
    {
        if ( reference != null )
        {
            throw new IllegalStateException( "Subtree was already created with offset " + valContainerOffset );
        }

        try
        {
            BTree<V, V> valueContainer = new BTree<V, V>( UUID.randomUUID().toString(), btree.getValueSerializer(),
                btree.getValueSerializer() );

            this.valueContainer = valueContainer;

            valueContainer.insert( singleValue, null, 0 );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return valueContainer;
    }


    /**
     * Tells if there is only one value in this element holder
     * 
     * @return true if single value is present, false if multiple values are present
     */
    public boolean isSingleValue()
    {
        return ( valueContainer == null );
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "'" );

        V value = getValue( btree );

        sb.append( value );

        sb.append( "'" );

        return sb.toString();
    }
}
