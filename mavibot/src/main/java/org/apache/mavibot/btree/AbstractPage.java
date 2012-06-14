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
 * A MVCC abstract Page. It stores the field and the methods shared by the Node and Leaf
 * classes.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class AbstractPage<K, V> implements Page<K, V>
{
    /** Parent B+Tree. */
    protected transient BTree<K, V> btree;

    /** This BPage's revision */
    protected long revision;
    
    /** This BPage's record ID in the PageManager. */
    protected long recordId;
    
    /** Keys of children nodes */
    protected K[] keys;
    
    /** The number of current values in the Page */
    protected int nbElems;
    
    /**
     * {@inheritDoc}
     */
    public long getNbElems()
    {
        return nbElems;
    }

    /**
     * {@inheritDoc}
     */
    public InsertResult<K, V> insert( long revision, K key, V value )
    {
        return null;
    }
    
    
    /**
     * Find the position of the given key in the page. If we have found the key,
     * we will return its position as a negative value.
     * <p/>
     * Assuming that the array is zero-indexed, the returned value will be : <br/>
     *   position = - ( position + 1)
     * <br/>
     * So for the following table of keys : <br/>
     * <pre>
     * +---+---+---+---+
     * | a | b | c | d |
     * +---+---+---+---+
     *   0   1   2   3
     * </pre>
     * looking for 'a' will return -1 (-(0+1)) and looking for 'c' will return -3 (-(2+1)).<br/>
     * Computing the real position is just a matter to get -(position++).
     * 
     * @param key The key to find
     * @return The position in the page.
     */
    protected int findKeyInPage( K key )
    {
        int left = 0;
        int right = nbElems - 1;

        // binary search
        while ( left < right )
        {
            int middle = ( left + right ) >>> 1;
            
            int comp = compare( keys[middle], key );
            
            if ( comp < 0 )
            {
                left = middle + 1;
            }
            else if ( comp > 0 )
            {
                if ( middle == left )
                {
                    return left;
                }
                
                right = middle - 1;
            }
            else
            {
                // Special case : the key already exists,
                // we can return immediately. The value will be
                // negative, and as the index may be 0, we subtract 1
                return - ( middle + 1 );
            }
        }
        
        // Special case : we don't know if the key is present
        int comp = compare( keys[left], key );
        
        if ( comp == 0 )
        {
            return - ( left + 1 );
        }
        else
        {
            if ( comp < 0 )
            {
                return left + 1;
            }
            else
            {
                return left;
            }
        }
    }


    /**
     * Compare two keys
     * 
     * @param key1 The first key
     * @param key2 The second key
     * @return -1 if the first key is above the second one, 1 if it's below, and 0
     * if the two keys are equal
     */
    private final int compare( K key1, K key2 )
    {
        if ( key1 == key2 )
        {
            return 0;
        }
        
        if ( key1 == null )
        {
            return 1;
        }
        
        if ( key2 == null )
        {
            return -1;
        }
        
        return btree.getComparator().compare( key1, key2 );
    }
}
