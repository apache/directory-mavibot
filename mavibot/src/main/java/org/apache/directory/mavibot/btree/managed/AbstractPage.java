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
import java.lang.reflect.Array;


/**
 * A MVCC abstract Page. It stores the field and the methods shared by the Node and Leaf
 * classes.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier */abstract class AbstractPage<K, V> implements Page<K, V>
{
    /** Parent B+Tree. */
    protected transient BTree<K, V> btree;

    /** This BPage's revision */
    protected long revision;

    /** Keys of children nodes */
    protected KeyHolder<K>[] keys;

    /** The number of current values in the Page */
    protected int nbElems;

    /** The first {@link PageIO} storing the serialized Page on disk */
    private long offset = -1L;

    /** The last {@link PageIO} storing the serialized Page on disk */
    private long lastOffset = -1L;


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
    @SuppressWarnings("unchecked")
    // Cannot create an array of generic objects
    protected AbstractPage( BTree<K, V> btree, long revision, int nbElems )
    {
        this.btree = btree;
        this.revision = revision;
        this.nbElems = nbElems;
        this.keys = ( KeyHolder[] ) Array.newInstance( KeyHolder.class, nbElems );
    }


    /**
     * Selects the sibling (the previous or next page with the same parent) which has
     * the more element assuming it's above N/2
     * 
     * @param parent The parent of the current page
     * @param The position of the current page reference in its parent
     * @return The position of the sibling, or -1 if we have'nt found any sibling
     * @throws IOException If we have an error while trying to access the page
     */
    protected int selectSibling( Node<K, V> parent, int parentPos ) throws IOException
    {
        if ( parentPos == 0 )
        {
            // The current page is referenced on the left of its parent's page :
            // we will not have a previous page with the same parent
            return 1;
        }

        if ( parentPos == parent.getNbElems() )
        {
            // The current page is referenced on the right of its parent's page :
            // we will not have a next page with the same parent
            return parentPos - 1;
        }

        Page<K, V> prevPage = parent.children[parentPos - 1].getValue( btree );
        Page<K, V> nextPage = parent.children[parentPos + 1].getValue( btree );

        int prevPageSize = prevPage.getNbElems();
        int nextPageSize = nextPage.getNbElems();

        if ( prevPageSize >= nextPageSize )
        {
            return parentPos - 1;
        }
        else
        {
            return parentPos + 1;
        }
    }


    /**
     * {@inheritDoc}
     */
    public int getNbElems()
    {
        return nbElems;
    }


    /**
     * Finds the position of the given key in the page. If we have found the key,
     * we will return its position as a negative value.
     * <p/>
     * Assuming that the array is zero-indexed, the returned value will be : <br/>
     *   position = - ( position + 1)
     * <br/>
     * So for the following table of keys : <br/>
     * <pre>
     * +---+---+---+---+
     * | b | d | f | h |
     * +---+---+---+---+
     *   0   1   2   3
     * </pre>
     * looking for 'b' will return -1 (-(0+1)) and looking for 'f' will return -3 (-(2+1)).<br/>
     * Computing the real position is just a matter to get -(position++).
     * <p/>
     * If we don't find the key in the table, we will return the position of the key
     * immediately above the key we are looking for. <br/>
     * For instance, looking for :
     * <ul>
     * <li>'a' will return 0</li>
     * <li>'b' will return -1</li>
     * <li>'c' will return 1</li>
     * <li>'d' will return -2</li>
     * <li>'e' will return 2</li>
     * <li>'f' will return -3</li>
     * <li>'g' will return 3</li>
     * <li>'h' will return -4</li>
     * <li>'i' will return 4</li>
     * </ul>
     * 
     * 
     * @param key The key to find
     * @return The position in the page.
     */
    public int findPos( K key )
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

            int comp = compare( keys[middle].getKey(), key );

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
                return -( middle + 1 );
            }
        }

        // Special case : we don't know if the key is present
        int comp = compare( keys[max].getKey(), key );

        if ( comp == 0 )
        {
            return -( max + 1 );
        }
        else
        {
            if ( comp < 0 )
            {
                return max + 1;
            }
            else
            {
                return max;
            }
        }
    }


    /**
     * Compares two keys
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
     * {@inheritDoc}
     */
    public K getKey( int pos )
    {
        if ( pos < nbElems )
        {
            return keys[pos].getKey();
        }
        else
        {
            return null;
        }
    }


    /**
     * Sets the key at a give position
     * 
     * @param pos The position in the keys array
     * @param key the key to inject
     */
    /* No qualifier*/void setKey( int pos, K key )
    {
        keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), key );
    }


    /**
     * Sets the key at a give position
     * 
     * @param pos The position in the keys array
     * @param buffer the serialized key to inject
     */
    /* No qualifier*/void setKey( int pos, byte[] buffer )
    {
        keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), buffer );
    }


    /**
     * {@inheritDoc}
     */
    public long getOffset()
    {
        return offset;
    }


    /**
     * @param offset the offset to set
     */
    /* No qualifier */void setOffset( long offset )
    {
        this.offset = offset;
    }


    /**
     * {@inheritDoc}
     */
    public long getLastOffset()
    {
        return lastOffset;
    }


    /**
     * {@inheritDoc}
     */
    /* No qualifier */void setLastOffset( long lastOffset )
    {
        this.lastOffset = lastOffset;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "r" ).append( revision );
        sb.append( ", nbElems:" ).append( nbElems );

        if ( offset > 0 )
        {
            sb.append( ", offset:" ).append( offset );
        }

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
