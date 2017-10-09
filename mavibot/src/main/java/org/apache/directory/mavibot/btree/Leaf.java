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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;


/**
 * A MVCC Leaf. It stores the keys and values. It does not have any children.
 * We have one key per value. Values are stored in {@link ValueHolder}, as we may not
 * deserialize them.
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier */class Leaf<K, V> extends AbstractPage<K, V>
{
    /** Values associated with keys */
    private ValueHolder<V>[] values;


    /**
     * Constructor used to create a new Leaf when we read it from a file.
     *
     * @param btree The BTree this page belongs to.
     */
    /* No qualifier */Leaf( BTreeInfo<K, V> btreeInfo )
    {
        super( btreeInfo );
    }


    /**
     * Internal constructor used to create Page instance used when a page is being copied or overflow
     *
     * @param btree The BTree this page belongs to.
     * @param revision The page revision
     * @param pageNbElems The number of elements this page will contain
     */
    @SuppressWarnings("unchecked")
    /* No qualifier */Leaf( BTreeInfo<K, V> btreeInfo, long revision, int pageNbElems )
    {
        super( btreeInfo, revision, pageNbElems );
        values = ( ValueHolder<V>[] ) Array.newInstance( ValueHolder.class, btreeInfo.getPageNbElem() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public InsertResult<K, V> insert( WriteTransaction transaction, K key, V value ) throws IOException
    {
        // Find the key into this leaf
        int pos = findPos( key );

        if ( pos < 0 )
        {
            // We already have the key in the page : replace the value
            // into a copy of this page, unless the page has already be copied
            return replaceElement( transaction, value, -( pos + 1 ) );
        }

        // The key is not present in the leaf. We have to add it in the page
        if ( pageNbElems < btreeInfo.getPageNbElem() )
        {
            // The current page is not full, it can contain the added element.
            // We insert it into a copied page and return the result
            return addElement( transaction, key, value, pos );
        }
        else
        {
            // The Page is already full : we split it and return the overflow element,
            // after having created two pages.
            return addAndSplit( transaction, key, value, pos );
        }
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public DeleteResult<K, V> delete( WriteTransaction transaction, K key, Node<K, V> parent, int parentPos ) throws IOException
    {
        // Check that the leaf is not empty
        if ( pageNbElems == 0 )
        {
            // Empty leaf
            return NotPresentResult.NOT_PRESENT;
        }

        // Find the key in the page
        int pos = findPos( key );

        if ( pos >= 0 )
        {
            // Not found : return the not present result.
            return NotPresentResult.NOT_PRESENT;
        }
        else
        {
            pos = - ( pos + 1 );
        }
        
        // The removed element
        Tuple<K, V> removedElement = new Tuple<>( keys[pos].getKey(), values[pos].get() );
        
        // If the parent is null, then this page is the root page.
        if ( parent == null )
        {
            Leaf<K, V> newLeaf = transaction.newLeaf( btreeInfo, pageNbElems - 1);

            // Create the result
            DeleteResult<K, V> defaultResult = new RemoveResult<>( newLeaf, removedElement, pos );

            // Just remove the entry if it's present, or replace it if we have more than one value in the ValueHolder
            copyAfterRemovingElement( newLeaf, pos );
            
            // Move the new leaf in the WAL, and the old page in the copied page list, if needed 
            transaction.removeWALObject( id );
            transaction.updateWAL( revision, this, newLeaf );

            return defaultResult;
        }
        else
        {
            // The current page is not the root. Check if the leaf has more than N/2
            // elements
            int halfSize = btreeInfo.getPageNbElem() / 2;

            if ( pageNbElems == halfSize )
            {
                // We have to find a sibling now, and either borrow an entry from it
                // if it has more than N/2 elements, or to merge the two pages.
                // Check in both next and previous page, if they have the same parent
                // and select the biggest page with the same parent to borrow an element.
                int siblingPos = selectSibling( transaction, parent, parentPos );
                Leaf<K, V> sibling = ( Leaf<K, V> ) ( parent.getPage( transaction, siblingPos ) );

                if ( sibling.pageNbElems == halfSize )
                {
                    // We will merge the current page with its sibling
                    return mergeWithSibling( transaction, removedElement, sibling, ( siblingPos < parentPos ), pos );
                }
                else
                {
                    // We can borrow the element from the left sibling
                    if ( siblingPos < parentPos )
                    {
                        return borrowFromLeft( transaction, removedElement, sibling, pos );
                    }
                    else
                    {
                        // Borrow from the right sibling
                        return borrowFromRight( transaction, removedElement, sibling, pos );
                    }
                }
            }
            else
            {
                Leaf<K, V> newLeaf = transaction.newLeaf( btreeInfo, pageNbElems - 1);

                // Create the result
                DeleteResult<K, V> defaultResult = new RemoveResult<>( newLeaf, removedElement, pos );

                // The page has more than N/2 elements.
                // We simply remove the element from the page, and if it was the leftmost,
                // we return the new pivot (it will replace any instance of the removed
                // key in its parents)
                copyAfterRemovingElement( newLeaf, pos );
                
                transaction.removeWALObject( id );
                transaction.updateWAL( revision, this, newLeaf );

                return defaultResult;
            }
        }
    }


    /**
     * Merges the sibling with the current leaf, after having removed the element in the page.
     * <pre>
     * Before :
     * 
     *        +---+---+---+---+
     *        | ~ | u | w | ~ |
     *        +---+---+---+---+
     *                |   |
     *          +-----+   +---------+
     *          |                   |
     *          v                   v
     *  +---+---+---+---+   +---+---+---+---+
     *  | u | v |   |   |   | w | x |   |   |
     *  +---+---+---+---+   +---+---+---+---+
     *  | U | V |   |   |   | W | X |   |   |
     *  +---+---+---+---+   +---+---+---+---+
     *    ^
     *    |
     *    +--- Removed element
     * 
     * After :
     * 
     *        +---+---+---+---+
     *        | ~ | v | ~ | ~ |
     *        +---+---+---+---+
     *            |   |   |
     *      ...---+   |   |
     *                |   |
     *          +-----+   +---...
     *          |
     *          v
     *  +---+---+---+---+
     *  | v | w | x |   |
     *  +---+---+---+---+
     *  | V | W | X |   |
     *  +---+---+---+---+
     *    ^
     *    |
     *    +--- Removed element
     * </pre>
     */
    private DeleteResult<K, V> mergeWithSibling( WriteTransaction transaction, Tuple<K, V> removedElement,
        Leaf<K, V> sibling, boolean isFromLeft, int pos )
    {
        // Create the new page. It will contain N - 1 elements (the maximum number)
        // as we merge two pages that contain N/2 elements minus the one we remove
        Leaf<K, V> newLeaf = transaction.newLeaf( btreeInfo, btreeInfo.getPageNbElem() - 1 );

        if ( isFromLeft )
        {
            // The sibling is on the left
            // Copy all the elements from the sibling first
            System.arraycopy( sibling.keys, 0, newLeaf.keys, 0, sibling.pageNbElems );
            System.arraycopy( sibling.values, 0, newLeaf.values, 0, sibling.pageNbElems );

            // Copy all the elements from the page up to the deletion position
            if ( pos > 0 )
            { 
                System.arraycopy( keys, 0, newLeaf.keys, sibling.pageNbElems, pos );
                System.arraycopy( values, 0, newLeaf.values, sibling.pageNbElems, pos );
            }

            // And copy the remaining elements after the deletion point
            if ( pos < pageNbElems - 1 )
            {
                System.arraycopy( keys, pos + 1, newLeaf.keys, sibling.pageNbElems + pos, pageNbElems - pos - 1 );
                System.arraycopy( values, pos + 1, newLeaf.values, sibling.pageNbElems + pos, pageNbElems - pos - 1 );
            }
        }
        else
        {
            // The sibling is on the right
            // Copy all the elements from the page up to the deletion position
            if ( pos > 0 )
            {
                System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
                System.arraycopy( values, 0, newLeaf.values, 0, pos );
            }

            // Then copy the remaining elements after the deletion point
            if ( pos < pageNbElems - 1 )
            {
                System.arraycopy( keys, pos + 1, newLeaf.keys, pos, pageNbElems - pos - 1 );
                System.arraycopy( values, pos + 1, newLeaf.values, pos, pageNbElems - pos - 1 );
            }

            // And copy all the elements from the sibling
            System.arraycopy( sibling.keys, 0, newLeaf.keys, pageNbElems - 1, sibling.pageNbElems );
            System.arraycopy( sibling.values, 0, newLeaf.values, pageNbElems - 1, sibling.pageNbElems );
        }

        // Add the new leaf to the WAL
        // And add the sibling to the copied pages
        transaction.removeWALObject( id );
        transaction.removeWALObject( sibling.id );
        transaction.updateWAL( revision, this, newLeaf );
        
        // And create the result
        return new MergedWithSiblingResult<>( newLeaf, removedElement );
    }


    /**
     * Borrows an element from the left sibling, creating a new sibling with one
     * less element and creating a new page where the element to remove has been
     * deleted and the borrowed element added on the left.
     * <pre>
     * Before :
     * 
     *            +---+---+---+---+
     *            | ~ | x | ~ | ~ |
     *            +---+---+---+---+
     *                |   |
     *          +-----+   +---------+
     *          |                   |
     *          v                   v
     *  +---+---+---+---+   +---+---+---+---+
     *  | u | v | w |   |   | x | y |   |   |
     *  +---+---+---+---+   +---+---+---+---+
     *  | U | V | W |   |   | X | Y |   |   |
     *  +---+---+---+---+   +---+---+---+---+
     *                            ^
     *                            |
     *                            +--- Removed element
     *                            
     * After :
     *            +---+---+---+---+
     *            | ~ | w | ~ | ~ |
     *            +---+---+---+---+
     *                |   |
     *          +-----+   +---------+
     *          |                   |
     *          v                   v
     *  +---+---+---+---+   +---+---+---+---+
     *  | u | v |   |   |   | w | x |   |   |
     *  +---+---+---+---+   +---+---+---+---+
     *  | U | V |   |   |   | W | X |   |   |
     *  +---+---+---+---+   +---+---+---+---+
     * </pre>
     */
    private DeleteResult<K, V> borrowFromLeft( WriteTransaction transaction, Tuple<K, V> removedElement, Leaf<K, V> sibling, int pos )
    {
        // The sibling is on the left, borrow the sibling rightmost element
        K siblingKey = sibling.keys[sibling.pageNbElems - 1].getKey();
        ValueHolder<V> siblingValue = sibling.values[sibling.pageNbElems - 1];

        // Create the new sibling, with one less element at the end
        Leaf<K, V> newSibling = ( Leaf<K, V> ) sibling.copy( transaction, sibling.pageNbElems - 1 );
        
        // Store the new sibling in the WAL
        transaction.updateWAL( sibling.getRevision(), sibling, newSibling );

        // Create the new page and add the new element at the beginning
        // First copy the current page, with the same size
        Leaf<K, V> newLeaf = transaction.newLeaf( btreeInfo, pageNbElems );

        // Insert the borrowed element
        newLeaf.keys[0] = new KeyHolder<K>( btreeInfo.getKeySerializer(), siblingKey );
        newLeaf.values[0] = siblingValue;

        // Copy the keys and the values up to the removal position,
        if ( pos > 0 )
        {
            System.arraycopy( keys, 0, newLeaf.keys, 1, pos );
            System.arraycopy( values, 0, newLeaf.values, 1, pos );
        }

        // And copy the remaining elements
        if ( pos < pageNbElems - 1 )
        {
            System.arraycopy( keys, pos + 1, newLeaf.keys, pos + 1, pageNbElems - pos - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, pos + 1, pageNbElems - pos - 1 );
        }

        // Store the new child in the WAL
        transaction.removeWALObject( sibling.id );
        transaction.removeWALObject( id );
        transaction.updateWAL( revision, sibling, newSibling );
        transaction.updateWAL( revision, this, newLeaf );

        return new BorrowedFromLeftResult<>( newLeaf, newSibling, removedElement );
    }


    /**
     * Borrows an element from the right sibling, creating a new sibling with one
     * less element and creating a new page where the element to remove has been
     * deleted and the borrowed element added on the right.
     * <pre>
     * Before :
     * 
     *            +---+---+---+---+
     *            | ~ | w | ~ | ~ |
     *            +---+---+---+---+
     *                |   |
     *          +-----+   +---------+
     *          |                   |
     *          v                   v
     *  +---+---+---+---+   +---+---+---+---+
     *  | u | v |   |   |   | w | x | y |   |
     *  +---+---+---+---+   +---+---+---+---+
     *  | U | V |   |   |   | W | X | Y |   |
     *  +---+---+---+---+   +---+---+---+---+
     *    ^
     *    |
     *    +--- Removed element
     *                            
     * After :
     *            +---+---+---+---+
     *            | ~ | x | ~ | ~ |
     *            +---+---+---+---+
     *                |   |
     *          +-----+   +---------+
     *          |                   |
     *          v                   v
     *  +---+---+---+---+   +---+---+---+---+
     *  | v | w |   |   |   | x | y |   |   |
     *  +---+---+---+---+   +---+---+---+---+
     *  | V | W |   |   |   | X | Y |   |   |
     *  +---+---+---+---+   +---+---+---+---+
     * </pre>
     */
    private DeleteResult<K, V> borrowFromRight( WriteTransaction transaction, Tuple<K, V> removedElement, Leaf<K, V> sibling, int pos )
    {
        // The sibling is on the right, borrow the leftmost element
        K siblingKey = sibling.keys[0].getKey();
        ValueHolder<V> siblingHolder = sibling.values[0];

        // Create the new sibling
        Leaf<K, V> newSibling = transaction.newLeaf( btreeInfo, sibling.pageNbElems - 1 );

        // Copy the keys and the values from 1 to N - 1 in the new sibling
        System.arraycopy( sibling.keys, 1, newSibling.keys, 0, sibling.pageNbElems - 1 );
        System.arraycopy( sibling.values, 1, newSibling.values, 0, sibling.pageNbElems - 1 );
        
        // Store the new sibling in the WAL
        transaction.updateWAL( sibling.getRevision(), sibling, newSibling );

        // Create the new page and add the new element at the end
        // First copy the current page, with the same size
        Leaf<K, V> newLeaf = transaction.newLeaf( btreeInfo, pageNbElems );

        // Insert the borrowed element at the end
        newLeaf.keys[pageNbElems - 1] = new KeyHolder<K>( btreeInfo.getKeySerializer(), siblingKey );
        newLeaf.values[pageNbElems - 1] = siblingHolder;

        // Copy the keys and the values up to the deletion position,
        System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
        System.arraycopy( values, 0, newLeaf.values, 0, pos );

        // And copy the remaining elements
        System.arraycopy( keys, pos + 1, newLeaf.keys, pos, pageNbElems - pos - 1 );
        System.arraycopy( values, pos + 1, newLeaf.values, pos, pageNbElems - pos - 1 );
        
        // Store the new leaf in the WAL
        transaction.removeWALObject( sibling.id );
        transaction.removeWALObject( id );
        transaction.updateWAL( revision, sibling, newSibling );
        transaction.updateWAL( revision, this, newLeaf );

        return new BorrowedFromRightResult<>( newLeaf, newSibling, removedElement );
    }


    /**
     * Copies the elements of the current page to a new page, without the removed element.
     * <pre>
     *        +--- removed pos
     *        |
     *        v
     *  +---+---+---+---+---+
     *  | A | B | C | D | E |
     *  +---+---+---+---+---+
     *    |   |   |   |   |
     *    |   x   |   |   |
     *    |       |   |   |
     *    |   +---+   |   |
     *    |   |       |   |
     *    |   |   +---+   |
     *    |   |   |       |
     *    |   |   |   +---+
     *    |   |   |   |
     *    v   v   v   v
     *  +---+---+---+---+
     *  | A | C | D | E |
     *  +---+---+---+---+
     * </pre
     */
    private void copyAfterRemovingElement( Leaf<K, V> newLeaf, int pos )
    {
        // Deal with the special case of a page with only one element by skipping
        // the copy, as we won't have any remaining  element in the page
        if ( pageNbElems == 1 )
        {
            return;
        }

        // Copy the keys and the values up to the insertion position
        if ( pos > 0 )
        {
            System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
            System.arraycopy( values, 0, newLeaf.values, 0, pos );
        }

        // And copy the elements after the position
        int remaining = keys.length - pos - 1;
        
        if ( remaining > 0 )
        {
            System.arraycopy( keys, pos + 1, newLeaf.keys, pos, remaining );
            System.arraycopy( values, pos + 1, newLeaf.values, pos, remaining );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V get( Transaction transaction, K key ) throws KeyNotFoundException, IOException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            // Found key, return the value
            return values[-( pos + 1 )].get();
        }
        else
        {
            // The key wasn't found
            throw KeyNotFoundException.INSTANCE;
        }
    }


    /**
     * {@inheritDoc}
     */
    /* No qualifier */KeyHolder<K> getKeyHolder( int pos )
    {
        if ( pos < pageNbElems )
        {
            return keys[pos];
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
    public boolean hasKey( Transaction transaction, K key ) throws IOException
    {
        int pos = findPos( key );

        return ( pos < 0 );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains( Transaction transaction, K key, V value ) throws IOException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            ValueHolder<V> valueHolder = values[-( pos + 1 )];

            return valueHolder.contains( value );
        }
        else
        {
            return false;
        }
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */ValueHolder<V> getValue( int pos )
    {
        if ( pos < pageNbElems )
        {
            return values[pos];
        }
        else
        {
            return null;
        }
    }


    /**
     * Sets the value at a give position
     * @param pos The position in the values array
     * @param value the value to inject
     */
    /* no qualifier */void setValue( int pos, ValueHolder<V> value )
    {
        values[pos] = value;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TupleCursor<K, V> browse( Transaction transaction, K key, ParentPos<K, V>[] stack, int depth ) throws IOException
    {
        int pos = findPos( key );

        // First use case : the leaf is empty (this is a root page)
        if ( pageNbElems == 0 )
        {
            // We have to return an empty cursor
            return new EmptyTupleCursor<>();
        }

        // The cursor we will return
        TupleCursor<K, V> cursor = new TupleCursor<>( transaction, stack, depth );

        // Depending on the position, we will proceed differently :
        // 1) if the key is found in the page, the cursor will be 
        // set to its position
        // 2) The key has not been found, we set the position to the
        // value findPos returned
        if ( pos < 0 )
        {
            // The key has been found.
            pos = -( pos + 1 );

            // Start at the found position in the page
            stack[depth] = new ParentPos<>( this, pos );
        }
        else
        {
            // The key has not been found, there are keys above this one. 
            // Select the value just above if we have some
            if ( pos == 0 )
            {
                // We are before the first value
                stack[depth] = new ParentPos<>( this, pos );
                
                cursor.beforeFirst();
            }
            else if ( pos < pageNbElems )
            {
                // There is at least one key above the one we are looking for.
                // This will be the starting point.
                stack[depth] = new ParentPos<>( this, pos );
            }
            else
            {
                // We are at the end of the tree.
                // No children, we are at the end of the root page
                stack[depth] = new ParentPos<>( this, pos );

                // As we are done, set the cursor at the end
                cursor.afterLast();
            }
        }

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TupleCursor<K, V> browse( Transaction transaction, ParentPos<K, V>[] stack, int depth )
    {
        int pos = 0;
        TupleCursor<K, V> cursor;

        if ( pageNbElems == 0 )
        {
            // The tree is empty, it's the root, we have nothing to return
            stack[depth] = new ParentPos<>( null, -1 );

            return new TupleCursor<>( transaction, stack, depth );
        }
        else
        {
            // Start at the beginning of the page
            ParentPos<K, V> parentPos = new ParentPos<>( this, pos );
            stack[depth] = parentPos;
            cursor = new TupleCursor<>( transaction, stack, depth );
        }

        return cursor;
    }


    /**
     * Copy the current page and all of the keys, values and children, if it's not a leaf.
     *
     * @param revision The new revision
     * @param pageNbElems The number of elements to copy
     * @return The copied page
     */
    public Page<K, V> copy( WriteTransaction transaction, int pageNbElems )
    {
        Leaf<K, V> newLeaf = transaction.newLeaf( btreeInfo, pageNbElems );

        // Copy the keys and the values
        System.arraycopy( keys, 0, newLeaf.keys, 0, pageNbElems );

        if ( values != null )
        {
            // It' not enough to copy the ValueHolder, we have to clone them
            // as ValueHolders are mutable
            int pos = 0;

            for ( ValueHolder<V> valueHolder : values )
            {
                if ( valueHolder != null )
                {
                    try
                    {
                        newLeaf.values[pos++] = valueHolder.clone();
                    }
                    catch ( CloneNotSupportedException e )
                    {
                        e.printStackTrace();
                    }
                }

                // Stop when we have copied pageNbElems values
                if ( pos == pageNbElems )
                {
                    break;
                }
            }
        }

        return newLeaf;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Page<K, V> copy( WriteTransaction transaction )
    {
        Leaf<K, V> newLeaf = transaction.newLeaf( btreeInfo, pageNbElems );

        // Copy the keys and the values
        System.arraycopy( keys, 0, newLeaf.keys, 0, pageNbElems );

        if ( ( values != null ) && ( pageNbElems > 0 ) )
        {
            // It' not enough to copy the ValueHolder, we have to clone them
            // as ValueHolders are mutable
            int pos = 0;

            for ( ValueHolder<V> valueHolder : values )
            {
                try
                {
                    newLeaf.values[pos++] = valueHolder.clone();
                }
                catch ( CloneNotSupportedException e )
                {
                    e.printStackTrace();
                }

                // Stop when we have copied pageNbElems values
                if ( pos == pageNbElems )
                {
                    break;
                }
            }
        }

        return newLeaf;
    }


    /**
     * Copy the current page if needed, and replace the value at the position we have found the key.
     */
    private InsertResult<K, V> replaceElement( WriteTransaction transaction, V value, int pos )
    {
        Leaf<K, V> newLeaf = this;
    
        // Get the previous value from the leaf (it's a copy)
        ValueHolder<V> valueHolder = values[pos];

        boolean valueExists = valueHolder.contains( value );
        
        // Check if the value does not already exist in the leaf
        if ( valueExists )
        {
            // Nothing to do, just return
            return ExistsResult.EXISTS;
        }

        // If the page was created in a previous revision, copy it
        if ( revision != transaction.getRevision() )
        {
            newLeaf = ( Leaf<K, V> ) copy( transaction, pageNbElems );
        }

        // Get the previous value from the leaf
        V replacedValue = newLeaf.values[pos].get();
        newLeaf.values[pos].set( value );

        // Create the result
        InsertResult<K, V> result = new ModifyResult<>( newLeaf, newLeaf.keys[pos].getKey(), replacedValue );
        
        // Update the WAL with the newly created leaf, or an updated version of the leaf
        transaction.updateWAL( revision, this, newLeaf );

        return result;
    }
    
    
    /**
     * Add a new element in a page, at a given position. The page will not be fulled up by this addition.
     */
    private InsertResult<K, V> addElement( WriteTransaction transaction, K key, V value, int pos )
    {
        Leaf<K, V> newLeaf = this;
        
        // If the page was created in a previous revision, copy it
        if ( revision != transaction.getRevision() )
        {
            newLeaf = ( Leaf<K, V> ) copy( transaction, pageNbElems );
        }

        // Inject the <K, V> into the new leaf
        Page<K, V> modifiedPage = newLeaf.addElement( key, value, pos );

        // And return a modified result
        InsertResult<K, V> result = new ModifyResult<>( modifiedPage, key, null );
        
        // Add the new leaf in the transaction pages map, and add
        // the old leaf into the CopiedPages B-tree, if needed
        transaction.updateWAL( revision, this, newLeaf );

        return result;
    }
    

    /**
     * Adds a new <K, V> into the current page at a given position. We return the
     * modified page. The modified page will have one more element than the current page.
     */
    private Page<K, V> addElement( K key, V value, int pos )
    {
        // Create the value holder
        ValueHolder<V> valueHolder = new ValueHolder<>( btreeInfo, value );

        // Deal with the special case of an empty page
        if ( pageNbElems == 0 )
        {
            keys[0] = new KeyHolder<>( btreeInfo.getKeySerializer(), key );
            values[0] = valueHolder;
        }
        else
        {
            // Copy the keys and the values from the insertion point one position to the right
            int nbElementToMove = pageNbElems - pos;
            System.arraycopy( keys, pos, keys, pos + 1, nbElementToMove );
            System.arraycopy( values, pos, values, pos + 1, nbElementToMove );

            // Add the new element
            keys[pos] = new KeyHolder<K>( btreeInfo.getKeySerializer(), key );
            values[pos] = valueHolder;
        }

        pageNbElems++;

        return this;
    }
    

    /**
     * Delete an element into the current page at a given position. We return the
     * modified page. The modified page will have one less element than the current page.
     *
     * @param pos The position into the page
     * @return The modified page with the element removed
     */
    public Tuple<K, V> deleteElement( int pos )
    {
        Tuple<K, V> removedElement = new Tuple<>( keys[pos].getKey(), values[pos].get() );
        
        // Copy the keys and the values from the insertion point one position to the right,
        // if this is not the last element
        int nbElementToMove = pageNbElems - pos - 1;
        
        if ( nbElementToMove > 0 )
        {
            System.arraycopy( keys, pos + 1, keys, pos, nbElementToMove );
            System.arraycopy( values, pos + 1, values, pos, nbElementToMove );
        }

        pageNbElems--;
        
        return removedElement;
    }


    /**
     * Split a full page into two new pages, a left, a right and a pivot element. The new pages will
     * each contains half of the original elements. <br/>
     * The pivot will be computed, depending on the place we will inject the newly added element. <br/>
     * If the newly added element is in the middle, we will use it
     * as a pivot. Otherwise, we will use either the last element in the left page if the element is added
     * on the left, or the first element in the right page if it's added on the right.
     */
    private SplitResult<K, V> addAndSplit( WriteTransaction transaction, K key, V value, int pos ) throws IOException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        int middle = btreeInfo.getPageNbElem() >> 1;
        Leaf<K, V> leftLeaf;
        Leaf<K, V> rightLeaf;
        ValueHolder<V> valueHolder = new ValueHolder<>( btreeInfo, value );

        // Determinate where to store the new value
        if ( pos <= middle )
        {
            // The left page will contain the new value. Create it, with a new ID
            leftLeaf = transaction.newLeaf( btreeInfo, middle + 1 );

            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, 0, leftLeaf.keys, 0, pos );
            System.arraycopy( values, 0, leftLeaf.values, 0, pos );

            // Add the new element
            leftLeaf.keys[pos] = new KeyHolder<K>( btreeInfo.getKeySerializer(), key );
            leftLeaf.values[pos] = valueHolder;

            // And copy the remaining elements
            System.arraycopy( keys, pos, leftLeaf.keys, pos + 1, middle - pos );
            System.arraycopy( values, pos, leftLeaf.values, pos + 1, middle - pos );

            // Now, create the right page, with a new ID
            rightLeaf = transaction.newLeaf( btreeInfo, middle );

            // Copy the keys and the values in the right page
            System.arraycopy( keys, middle, rightLeaf.keys, 0, middle );
            System.arraycopy( values, middle, rightLeaf.values, 0, middle );
        }
        else
        {
            // Create the left page, with a new ID
            leftLeaf = transaction.newLeaf( btreeInfo, middle );

            // Copy all the elements into the left page
            System.arraycopy( keys, 0, leftLeaf.keys, 0, middle );
            System.arraycopy( values, 0, leftLeaf.values, 0, middle );

            // Now, create the right page, with a new ID
            rightLeaf = transaction.newLeaf( btreeInfo, middle + 1 );

            int rightPos = pos - middle;

            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, middle, rightLeaf.keys, 0, rightPos );
            System.arraycopy( values, middle, rightLeaf.values, 0, rightPos );

            // Add the new element
            rightLeaf.keys[rightPos] = new KeyHolder<>( btreeInfo.getKeySerializer(), key );
            rightLeaf.values[rightPos] = valueHolder;

            // And copy the remaining elements
            System.arraycopy( keys, pos, rightLeaf.keys, rightPos + 1, pageNbElems - pos );
            System.arraycopy( values, pos, rightLeaf.values, rightPos + 1, pageNbElems - pos );
        }

        // Get the pivot
        K pivot = rightLeaf.keys[0].getKey();
        
        // Inject the created leaves in the transaction
        transaction.addWALObject( leftLeaf );
        transaction.addWALObject( rightLeaf );
        
        // And remove the original page from the transaction
        transaction.removeWALObject( id );
        
        // Add the old page into the CopiedPages B-tree if it isn't part of the WAL
        if ( isBTreeUser() && ( revision != transaction.getRevision() ) )
        {
            transaction.addCopiedWALObject( this );
        }

        // Create the result
        return new SplitResult<>( pivot, leftLeaf, rightLeaf );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public K getLeftMostKey( Transaction transaction )
    {
        return keys[0].getKey();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public K getRightMostKey( Transaction transaction ) throws IOException
    {
        return keys[pageNbElems - 1].getKey();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> findLeftMost( Transaction transaction )
    {
        return new Tuple<>( keys[0].getKey(), values[0].get() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> findRightMost( Transaction transaction )
    {
        return new Tuple<>( keys[pageNbElems - 1].getKey(), values[pageNbElems - 1].get() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeaf()
    {
        return true;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNode()
    {
        return false;
    }


    /**
     * Serialize a Leaf's Value.
     */
    private int serializeLeafValue( int pos, List<byte[]> serializedData )
    {
        ValueHolder<V> valueHolder = getValue( pos );
        int dataSize = 0;

        // We have a serialized value. Just flush it
        byte[] data = valueHolder.getRaw();

        // Store the data if it's not 0
        if ( data.length > 0 )
        {
            serializedData.add( data );
            dataSize = data.length;
        }

        return dataSize;
    }


    /**
     * Serialize a Leaf's key
     */
    private int serializeLeafKey( int pos, List<byte[]> serializedData )
    {
        int dataSize = 0;
        KeyHolder<K> keyHolder = getKeyHolder( pos );
        byte[] keyData = keyHolder.getRaw();

        if ( keyData != null )
        {
            // We have to store the serialized key data
            serializedData.add( keyData );
            dataSize += keyData.length;
        }
        else
        {
            throw new RuntimeException( "Key cannot be null" );
        }

        return dataSize;
    }


    /**
     * Write a root page with no elements in it
     */
    private PageIO[] serializeRootPage( WriteTransaction transaction, long revision ) throws IOException
    {
        RecordManager recordManager = transaction.getRecordManager();
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        // We will have 1 single page if we have no elements
        int bufferSize = BTreeConstants.LONG_SIZE + BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE;

        // This is either a new root page or a new page that will be filled later
        pageIOs = recordManager.getFreePageIOs( recordManagerHeader, bufferSize );

        // We need first to create a byte[] that will contain all the data
        // For the root page, this is easy, as we only have to store the revision,
        // and the number of elements, which is 0.
        long position = 0L;

        position = recordManager.store( recordManagerHeader, position, id, pageIOs[0] );
        position = recordManager.store( recordManagerHeader, position, revision, pageIOs[0] );
        position = recordManager.store( recordManagerHeader, position, 0, pageIOs[0] );

        // Update the page size now
        pageIOs[0].setSize( ( int ) position );

        offset = pageIOs[0].getOffset();

        return pageIOs;
    }


    /**
     * Serialize a new Leaf. It will contain the following data :<br/>
     * <ul>
     *   <li>the page id : a long</li>
     *   <li>the revision : a long</li>
     *   <li>the number of elements in the page : a positive int </li>
     *   <li>the data size : an int</li>
     *   <li>the keys and values, N times :
     *     <ul>
     *       <li>key[n] : a serialized key</li>
     *       <li>value[n] : a serialized value</li>
     *     </ul>
     *   </li>
     * </ul>
     * Note that keys and values are stored alternatively :
     * <pre> 
     * V[0], K[0], V[1], K[1], ... V[N], K[N]
     * </pre>
     * 
     * {@inheritDoc}
     */
    @Override
    public PageIO[] serialize( WriteTransaction transaction ) throws IOException
    {
        RecordManager recordManager = transaction.getRecordManager();
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        if ( pageNbElems == 0 )
        {
            return serializeRootPage( transaction, revision );
        }
        else
        {
            // Prepare a list of byte[] that will contain the serialized page
            int nbBuffers = 1 + 1 + 1 + 1 + pageNbElems * 2;
            int dataSize = 0;
            int serializedSize = 0;

            // Now, we can create the list with the right size
            List<byte[]> serializedData = new ArrayList<>( nbBuffers );

            // The id
            byte[] buffer = LongSerializer.serialize( id );
            serializedData.add( buffer );
            serializedSize += buffer.length;

            // The revision
            buffer = LongSerializer.serialize( revision );
            serializedData.add( buffer );
            serializedSize += buffer.length;

            // The number of elements
            buffer = IntSerializer.serialize( pageNbElems );
            serializedData.add( buffer );
            serializedSize += buffer.length;

            // Iterate on the keys and values. We first serialize the value, then the key
            // until we are done with all of them. 
            for ( int pos = 0; pos < pageNbElems; pos++ )
            {
                // Start with the value
                dataSize += serializeLeafValue( pos, serializedData );
                dataSize += serializeLeafKey( pos, serializedData );
            }

            // Store the data size at the third position in the list of buffers
            // (ie, just after the number of elements, and just before the keys/values)
            buffer = IntSerializer.serialize( dataSize );
            serializedData.add( 3, buffer );
            serializedSize += buffer.length;

            serializedSize += dataSize;

            // We are done. Allocate the pages we need to store the data, if we don't have
            // a pageIOs or not enough room in it.
            if ( pageIOs != null ) 
            {
                int nbNeededPages = RecordManager.computeNbPages( recordManagerHeader, dataSize );
                
                if ( nbNeededPages > pageIOs.length )
                {
                    pageIOs = recordManager.getFreePageIOs( recordManagerHeader, serializedSize );
                }
                else
                {
                    // Resize the page
                    pageIOs[0].setSize( serializedSize );
                }
            }
            else
            {
                pageIOs = recordManager.getFreePageIOs( recordManagerHeader, serializedSize );
            }

            // And store the data into those pages
            long position = 0L;

            for ( byte[] bytes : serializedData )
            {
                position = recordManager.storeRaw( recordManagerHeader, position, bytes, pageIOs );
            }

            offset = pageIOs[0].getOffset();
            
            return pageIOs;
        }
    }
    
    
    /**
     * Deserialize a Leaf. It will contain the following data :<br/>
     * <ul>
     * <li>the ID : a long</li>
     * <li>the revision : a long</li>
     * <li>the number of elements in the page : a positive int </li>
     * <li>the size of the values/keys when serialized
     * <li>the keys : an array of serialized keys</li>
     * <li>the values : an array of serialized values</li>
     * </ul>
     * 
     * The three first values have already been deserialized by the caller.
     * {@inheritDoc}
     */
    public Leaf<K, V> deserialize( ByteBuffer byteBuffer ) throws IOException
    {
        // Iterate on the keys and values. We first serialize the value, then the key
        // until we are done with all of them. If we are serializing a page, we have
        // to serialize one more value
        for ( int pos = 0; pos < pageNbElems; pos++ )
        {
            // Start with the value
            V value = btreeInfo.getValueSerializer().deserialize( byteBuffer );
            ValueHolder<V> valueHolder = new ValueHolder<>( btreeInfo, value );
            BTreeFactory.setValue( this, pos, valueHolder );

            // Then the key
            K key = btreeInfo.getKeySerializer().deserialize( byteBuffer );
            this.setKey( pos, new KeyHolder<>( btreeInfo.getKeySerializer(), key ) );
        }
        
        return this;
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public KeyCursor<K> browseKeys( Transaction transaction, ParentPos<K, K>[] stack, int depth ) throws IOException
    {
        int pos = 0;
        KeyCursor<K> cursor;

        if ( pageNbElems == 0 )
        {
            // The tree is empty, it's the root, we have nothing to return
            stack[depth] = new ParentPos<>( null, -1 );

            return new KeyCursor<>( transaction, btreeInfo, stack, depth );
        }
        else
        {
            // Start at the beginning of the page
            ParentPos<K, K> parentPos = new ParentPos( this, pos );

            stack[depth] = parentPos;

            cursor = new KeyCursor<>( transaction, btreeInfo, stack, depth );
        }

        return cursor;
    }


    /**
     * sets the values to null
     * WARNING: only used by the internal API (especially during the bulk loading)
     */
    /* no qualifier */void _clearValues_()
    {
        values = null;
    }


    /**
     * @return the values
     */
    /* no qualifier */ValueHolder<V>[] getValues()
    {
        return values;
    }


    /**
     * @param values the values to set
     */
    /* no qualifier */void setValues( ValueHolder<V>[] values )
    {
        this.values = values;
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public String prettyPrint()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "{Leaf(" ).append( id ).append( ")@" );
        
        if ( offset == BTreeConstants.NO_PAGE )
        {
            sb.append( "---" );
        }
        else
        {
            sb.append( String.format( "0x%4X", offset ) );
        }
        
        sb.append( ",<" );
        sb.append( getName() ).append( ':' ).append( getRevision() );
        sb.append( ">}" );

        return sb.toString();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String dumpPage( String tabs )
    {
        StringBuilder sb = new StringBuilder();

        sb.append( tabs );

        if ( pageNbElems > 0 )
        {
            boolean isFirst = true;

            for ( int i = 0; i < pageNbElems; i++ )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }

                sb.append( "<" ).append( keys[i] ).append( "," );

                if ( values != null )
                {
                    sb.append( values[i] );
                }
                else
                {
                    sb.append( "null" );
                }

                sb.append( ">" );
            }
        }

        sb.append( "\n" );

        return sb.toString();
    }


    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Leaf[" );
        sb.append( super.toString() );

        sb.append( "] -> {" );

        if ( pageNbElems > 0 )
        {
            boolean isFirst = true;

            for ( int i = 0; i < pageNbElems; i++ )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }

                sb.append( "<" ).append( keys[i] ).append( "," );

                if ( values != null )
                {
                    sb.append( values[i] );
                }
                else
                {
                    sb.append( "null" );
                }

                sb.append( ">" );
            }
        }

        sb.append( "}" );

        return sb.toString();
    }
}
