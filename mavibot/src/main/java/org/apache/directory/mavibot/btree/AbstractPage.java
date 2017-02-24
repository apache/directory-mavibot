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

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;


/**
 * A MVCC abstract Page. It stores the field and the methods shared by the Node and Leaf
 * classes (the keys and values/children).
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

    /** Children pages associated with keys. */
    protected PageHolder<K, V>[] children;

    /** The number of current values in the Page */
    protected int nbPageElems;

    /** This Page's revision */
    protected long revision;

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
    protected AbstractPage( BTree<K, V> btree, long revision, int nbPageElems )
    {
        this.btree = btree;
        this.revision = revision;
        this.nbPageElems = nbPageElems;
        this.keys = ( KeyHolder[] ) Array.newInstance( KeyHolder.class, btree.getPageNbElem() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getNbPageElems()
    {
        return nbPageElems;
    }


    /**
     * Sets the number of element in this page
     * @param nbPageElems The number of elements
     */
    /* no qualifier */void setNbPageElems( int nbPageElems )
    {
        this.nbPageElems = nbPageElems;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public K getKey( int pos )
    {
        if ( ( pos < nbPageElems ) && ( keys[pos] != null ) )
        {
            return keys[pos].getKey();
        }
        else
        {
            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasKey( K key ) throws IOException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            // Here, if we have found the key in the node, then we must go down into
            // the right child, not the left one
            return children[-pos].getValue().hasKey( key );
        }
        else
        {
            Page<K, V> page = children[pos].getValue();

            return page.hasKey( key );
        }
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */Page<K, V> getReference( int pos ) throws IOException
    {
        if ( pos < nbPageElems + 1 )
        {
            if ( children[pos] != null )
            {
                return children[pos].getValue();
            }
            else
            {
                return null;
            }
        }
        else
        {
            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TupleCursor<K, V> browse( Transaction transaction, K key, ParentPos<K, V>[] stack, int depth )
        throws IOException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            pos = -pos;
        }

        // We first stack the current page
        stack[depth++] = new ParentPos<>( this, pos );

        Page<K, V> page = children[pos].getValue();

        return page.browse( transaction, key, stack, depth );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains( K key, V value ) throws IOException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            // Here, if we have found the key in the node, then we must go down into
            // the right child, not the left one
            return children[-pos].getValue().contains( key, value );
        }
        else
        {
            return children[pos].getValue().contains( key, value );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public DeleteResult<K, V> delete( WriteTransaction transaction, K key ) throws IOException
    {
        return delete( transaction, key, null, -1 );
    }


    /**
     * The real delete implementation. It can be used for internal deletion in the B-tree.
     *
     * @param key The key to delete
     * @param parent The parent page
     * @param parentPos The position of this page in the parent page
     * @return The result
     * @throws IOException If we had an issue while processing the deletion
     */
    /* no qualifier */abstract DeleteResult<K, V> delete( WriteTransaction transaction, K key, Page<K, V> parent,
        int parentPos ) throws IOException;


    /**
     * {@inheritDoc}
     */
    @Override
    public V get( K key ) throws IOException, KeyNotFoundException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            // Here, if we have found the key in the node, then we must go down into
            // the right child, not the left one
            return children[-pos].getValue().get( key );
        }
        else
        {
            return children[pos].getValue().get( key );
        }
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */Page<K, V> getPage( int pos )
    {
        if ( ( pos >= 0 ) && ( pos < children.length ) && ( children[pos] != null ) )
        {
            return children[pos].getValue();
        }
        else
        {
            return null;
        }
    }


    /**
     * Inject a pageHolder into the node, at a given position
     * 
     * @param pos The position of the added pageHolder
     * @param pageHolder The pageHolder to add
     */
    /* no qualifier */void setPageHolder( int pos, PageHolder<K, V> pageHolder )
    {
        if ( ( pos >= 0 ) && ( pos < children.length ) )
        {
            children[pos] = pageHolder;
        }
    }


    /**
     * Sets the value at a give position
     * @param pos The position in the values array
     * @param value the value to inject
     */
    /* no qualifier */void setValue( int pos, ValueHolder<V> value )
    {
        // Implementation in the leaves
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TupleCursor<K, V> browse( Transaction transaction, ParentPos<K, V>[] stack, int depth )
        throws IOException
    {
        stack[depth++] = new ParentPos<>( this, 0 );

        Page<K, V> page = children[0].getValue();

        return page.browse( transaction, stack, depth );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public KeyCursor<K> browseKeys( Transaction transaction, ParentPos<K, K>[] stack, int depth )
        throws IOException
    {
        stack[depth++] = new ParentPos( this, 0 );

        Page<K, V> page = children[0].getValue();

        return page.browseKeys( transaction, stack, depth );
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
    protected int selectSibling( Transaction transaction, Page<K, V> parent, int parentPos ) throws IOException
    {
        if ( parentPos == 0 )
        {
            // The current page is referenced on the left of its parent's page :
            // we will not have a previous page with the same parent
            return 1;
        }

        if ( parentPos == parent.getNbPageElems() )
        {
            // The current page is referenced on the right of its parent's page :
            // we will not have a next page with the same parent
            return parentPos - 1;
        }

        Page<K, V> prevPage = ( ( AbstractPage<K, V> ) parent ).getPage( parentPos - 1 );
        Page<K, V> nextPage = ( ( AbstractPage<K, V> ) parent ).getPage( parentPos + 1 );

        int prevPageSize = prevPage.getNbPageElems();
        int nextPageSize = nextPage.getNbPageElems();

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
    @Override
    public K getLeftMostKey()
    {
        return keys[0].getKey();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public K getRightMostKey()
    {
        return keys[nbPageElems - 1].getKey();
    }


    /**
     * @return the keys
     */
    /* no qualifier */KeyHolder<K>[] getKeys()
    {
        return keys;
    }


    /**
     * Sets the key at a give position
     *
     * @param pos The position in the keys array
     * @param key the key to inject
     */
    /* no qualifier */void setKey( int pos, KeyHolder<K> key )
    {
        keys[pos] = key;
    }


    /**
     * @param revision the keys to set
     */
    /* no qualifier */void setKeys( KeyHolder<K>[] keys )
    {
        this.keys = keys;
    }


    /**
     * @return the revision
     */
    @Override
    public long getRevision()
    {
        return revision;
    }


    /**
     * @param revision the revision to set
     */
    /* no qualifier */void setRevision( long revision )
    {
        this.revision = revision;
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

        return btree.getKeyComparator().compare( key1, key2 );
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
    @Override
    public int findPos( K key )
    {
        // Deal with the special key where we have an empty page
        if ( nbPageElems == 0 )
        {
            return 0;
        }

        int min = 0;
        int max = nbPageElems - 1;

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
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> findLeftMost() throws IOException
    {
        return children[0].getValue().findLeftMost();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> findRightMost() throws IOException
    {
        return children[nbPageElems].getValue().findRightMost();
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getName()
    {
        return btree.getName();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String dumpPage( String tabs )
    {
        StringBuilder sb = new StringBuilder();

        if ( nbPageElems > 0 )
        {
            // Start with the first child
            sb.append( children[0].getValue().dumpPage( tabs + "    " ) );

            for ( int i = 0; i < nbPageElems; i++ )
            {
                sb.append( tabs );
                sb.append( "<" );
                sb.append( getKey( i ) ).append( ">\n" );
                sb.append( children[i + 1].getValue().dumpPage( tabs + "    " ) );
            }
        }

        return sb.toString();
    }


    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "r" ).append( revision );
        sb.append( ", nbPageElems:" ).append( nbPageElems );
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
