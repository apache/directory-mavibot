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


import java.util.Comparator;


/**
 * The Tuple class is used when we browse a btree, it will contain the results
 * fetched from the btree.
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 */
public class Tuple<K, V> implements Comparable<Tuple<K, V>>
{
    /** The key */
    protected K key;

    /** The value */
    protected V value;

    /** The key comparator */
    protected Comparator<K> keyComparator;


    /**
     * Creates a Tuple with no content
     */
    public Tuple()
    {
    }


    /**
     * Creates a Tuple containing a key and its associated value.
     * @param key The key
     * @param value The associated value
     */
    public Tuple( K key, V value )
    {
        this.key = key;
        this.value = value;
    }


    /**
     * Creates a Tuple containing a key and its associated value.
     * @param key The key
     * @param value The associated value
     */
    public Tuple( K key, V value, Comparator<K> keyComparator )
    {
        this.key = key;
        this.value = value;
        this.keyComparator = keyComparator;
    }


    /**
     * @return the key
     */
    public K getKey()
    {
        return key;
    }


    /**
     * @param key the key to set
     */
    public void setKey( K key )
    {
        this.key = key;
    }


    /**
     * @return the value
     */
    public V getValue()
    {
        return value;
    }


    /**
     * @param value the value to set
     */
    public void setValue( V value )
    {
        this.value = value;
    }


    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return key.hashCode();
    }


    /**
     * @see Object#equals()
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }

        if ( !( obj instanceof Tuple ) )
        {
            return false;
        }

        if ( this.key == null )
        {
            return ( ( Tuple<K, V> ) obj ).key == null;
        }

        return this.key.equals( ( ( Tuple<K, V> ) obj ).key );
    }


    /**
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo( Tuple<K, V> t )
    {
        if ( keyComparator != null )
        {
            return keyComparator.compare( key, t.key );
        }
        else
        {
            return 0;
        }
    }


    /**
     * @return the keyComparator
     */
    public Comparator<K> getKeyComparator()
    {
        return keyComparator;
    }


    /**
     * @param keyComparator the keyComparator to set
     */
    public void setKeyComparator( Comparator<K> keyComparator )
    {
        this.keyComparator = keyComparator;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "<" + key + "," + value + ">";
    }
}
