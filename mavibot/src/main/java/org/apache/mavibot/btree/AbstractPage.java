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
public abstract class AbstractPage<K, V> implements Page<K, V>
{
    /** Parent B+Tree. */
    protected transient BTree<K, V> btree;

    /** This BPage's revision */
    protected long revision;
    
    /** This BPage's ID in the PageManager. */
    protected long id;
    
    /** Keys of children nodes */
    protected K[] keys;
    
    /** The number of current values in the Page */
    protected int nbElems;
    
    
    /**
     * Creates a default empty AbstractPage
     * 
     * @param btree The associated BTree
     */
    protected AbstractPage( BTree<K, V> btree )
    {
        this.btree = btree;
    }
    
    
    /**
     * Internal constructor used to create Page instance used when a page is being copied or overflow
     */
    @SuppressWarnings("unchecked") // Cannot create an array of generic objects
    protected AbstractPage( BTree<K, V> btree, long revision, int nbElems )
    {
        this.btree = btree;
        this.revision = revision;
        this.nbElems = nbElems;
        this.keys = (K[])new Object[nbElems];
        id = btree.generateRecordId();
    }

    
    /**
     * {@inheritDoc}
     */
    public long getNbElems()
    {
        return nbElems;
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
    protected int findPos( K key )
    {
        // Deal with the special key where we have an empty page
        if ( nbElems == 0 )
        {
            return 0;
        }
        
        int min = 0;
        int max = nbElems - 1;

        // binary search
        while ( min < max )
        {
            int middle = ( min + max + 1 ) >> 1;
            
            int comp = compare( keys[middle], key );
            
            if ( comp < 0 )
            {
                min = middle + 1;
            }
            else if ( comp > 0 )
            {
                max = middle - 1;
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
        int comp = compare( keys[max], key );
        
        if ( comp == 0 )
        {
            return - ( max + 1 );
        }
        else
        {
            if ( comp < 0 )
            {
                return max + 1;
            }
            else
            {
                return min;
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
    
    
    /**
     * {@inheritDoc}
     */
    public long getRevision()
    {
        return revision;
    }


    /**
     * @return the id
     */
    public long getId()
    {
        return id;
    }


    /**
     * @param id the id to set
     */
    public void setId( long id )
    {
        this.id = id;
    }
    
    
    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "r" ).append( revision );
        sb.append( ", ID:" ).append( id );
        sb.append( ", nbElems:" ).append( nbElems );
        
        return sb.toString();
    }
    
    
    /**
     * {@inheritDoc}
     */
    public String dumpPage( String tabs )
    {
        return "";
    }
}
