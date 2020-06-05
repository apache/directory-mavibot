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
import java.lang.reflect.Array;

import org.omg.CORBA.NO_PERMISSION;

/**
 * A MVCC abstract Page. It stores the keys/children and the methods shared by the {@link Node} and {@link Leaf}
 * classes (the keys and values/children). It also hold the number of elements the page contains, and its current revision.
 * <p>
 * Keys are stored in {@link KeyHolder}s, values in {@link ValueHolder}.
 * <p>
 * The {@link Page} extends {@link AbstractWALObject} as any pae may be stored in the WAL at some point.
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/abstract class AbstractPage<K, V> extends AbstractWALObject<K, V> implements Page<K, V>
{
    /** Keys of children nodes */
    protected KeyHolder<K>[] keys;

    /** The number of current values in the Page */
    protected int pageNbElems;

    /**
     * Creates a default empty AbstractPage
     *
     * @param btreeInfo The associated BTree info
     */
    protected AbstractPage( BTreeInfo<K, V> btreeInfo )
    {
        super( btreeInfo );
    }


    /**
     * Internal constructor used to create Page instance used when a page is being copied or overflow
     */
    @SuppressWarnings("unchecked")
    // Cannot create an array of generic objects
    protected AbstractPage( BTreeInfo<K, V> btreeInfo, long revision, int pageNbElems )
    {
        super( btreeInfo );
        this.revision = revision;
        this.pageNbElems = pageNbElems;
        this.keys = ( KeyHolder[] ) Array.newInstance( KeyHolder.class, btreeInfo.getPageNbElem() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getPageNbElems()
    {
        return pageNbElems;
    }


    /**
     * Sets the number of element in this page
     * 
     * @param pageNbElems The number of elements
     */
    /* no qualifier */void setPageNbElems( int pageNbElems )
    {
        this.pageNbElems = pageNbElems;
    }


    /**
     * Returns the key at a given position
     *
     * @param pos The position of the key we want to retrieve
     * @return The key found at the given position
     */
    protected K getKey( int pos )
    {
        if ( ( pos < pageNbElems ) && ( keys[pos] != null ) )
        {
            return keys[pos].getKey();
        }
        else
        {
            return null;
        }
    }
    
    
    /**
     * Set a parent page child value, either an offset (a positive value) or an ID (a negative value) if the offset is -1.
     * 
     * @param parent The node to update
     * @param pos The position in the parent node
     * @param child The child to refer to
     */
    protected void setChild( Node<K, V> parent, int pos, Page<K, V> child )
    {
        long childOffset = child.getOffset();
        
        if ( childOffset == BTreeConstants.NO_PAGE )
        {
            // If the reference is -1, then it's a new page. Use its ID as a reference, with a 
            // negative value.
            parent.children[pos] = -child.getId();
        }
        else
        {
            parent.children[pos] = childOffset;
        }
    }
    
    
    /**
     * Set the page child value, either an offset (a positive value) or an ID (a negative value) if the offset is -1.
     * 
     * @param children The parent's children
     * @param pos The position in the parent node
     * @param child The child to refer to
     */
    protected void setChild( long[] children, int pos, Page<K, V> child )
    {
        children[pos] = child.getOffset();
        
        if ( children[pos] == BTreeConstants.NO_PAGE )
        {
            children[pos] = -child.getId();
        }
    }


    /**
     * Selects the sibling (the previous or next page with the same parent) which has
     * the more element assuming it's above N/2
     *
     * @param transaction The {@link Transaction} we are running in 
     * @param parent The parent of the current page
     * @param parentPos The position of the current page reference in its parent
     * @return The position of the sibling, or -1 if we have'nt found any sibling
     */
    protected int selectSibling( Transaction transaction, Node<K, V> parent, int parentPos ) throws IOException
    {
        if ( parentPos == 0 )
        {
            // The current page is referenced on the left of its parent's page :
            // we will not have a previous page with the same parent
            return 1;
        }

        if ( parentPos == parent.getPageNbElems() )
        {
            // The current page is referenced on the right of its parent's page :
            // we will not have a next page with the same parent
            return parentPos - 1;
        }

        // Ok, in the middle of a page, select the one with the higher number of children,
        // or the left one
        Page<K, V> prevPage = parent.getPage( transaction, parentPos - 1 );
        Page<K, V> nextPage = parent.getPage( transaction, parentPos + 1 );

        if ( prevPage == null )
        {
            prevPage = parent.getPage( transaction, parentPos - 1 );
        }
        int prevPageSize = prevPage.getPageNbElems();
        int nextPageSize = nextPage.getPageNbElems();

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
     * @return the array containing the {@link Page}'s keys.
     */
    public KeyHolder<K>[] getKeys()
    {
        return keys;
    }


    /**
     * Sets the key at a give position
     *
     * @param pos The position in the keys array
     * @param key The key to inject
     */
    /* no qualifier */void setKey( int pos, KeyHolder<K> key )
    {
        keys[pos] = key;
    }


    /**
     * @param keys The keys to set
     */
    /* no qualifier */void setKeys( KeyHolder<K>[] keys )
    {
        this.keys = keys;
    }


    /**
     * Compares two keys
     *
     * @param key1 The first key
     * @param key2 The second key
     * @return -1 if the first key is above the second one, 1 if it's below, and 0
     * if the two keys are equal
     */
    protected final int compare( K key1, K key2 )
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

        return btreeInfo.getKeySerializer().getComparator().compare( key1, key2 );
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
     * immediately above the key we are looking for, as a positive value. <br/>
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
    protected int findPos( K key )
    {
        // Deal with the special key where we have an empty page
        if ( pageNbElems == 0 )
        {
            return 0;
        }

        int min = 0;
        int max = pageNbElems - 1;

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
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "r" ).append( revision );
        sb.append( ", B-tree:" ).append(  getName() );
        sb.append( ", pageNbElems:" ).append( pageNbElems );
        sb.append( ", id:" ).append( id );

        if ( offset > 0 )
        {
            sb.append( ", offset: 0x" ).append( Long.toHexString( offset ) );
        }
        
        if ( pageIOs != null )
        {
            sb.append( ", nb pageIOs:" ).append( pageIOs.length );
        }

        return sb.toString();
    }
}
