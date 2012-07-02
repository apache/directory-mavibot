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
 * The Tuple class is used when we browse a btree, it will contain the results
 * fetched from the btree.
 * 
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 */
public class Tuple<K, V>
{
    /** The key */
    private K key;
    
    /** The value */
    private V value;
    
    /**
     * Creates a Tuple with no content
     */
    /* No qualifier */ Tuple()
    {
    }
    
    
    /**
     * Creates a Tuple containing a key and its associated value.
     * @param key The key
     * @param value The associated value
     */
    /* No qualifier */ Tuple( K key, V value )
    {
        this.key = key;
        this.value = value;
    }


    /**
     * @return the key
     */
    /* No qualifier */ K getKey()
    {
        return key;
    }


    /**
     * @param key the key to set
     */
    /* No qualifier*/ void setKey( K key )
    {
        this.key = key;
    }


    /**
     * @return the value
     */
    /* No qualifier */ V getValue()
    {
        return value;
    }


    /**
     * @param value the value to set
     */
    /* No qualifier*/ void setValue( V value )
    {
        this.value = value;
    }

    
    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "<" + key + "," + value + ">";
    }
}
