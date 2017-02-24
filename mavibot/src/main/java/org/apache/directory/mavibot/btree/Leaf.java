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
    Leaf( BTree<K, V> btree )
    {
        super( btree );
    }


    /**
     * Internal constructor used to create Page instance used when a page is being copied or overflow
     *
     * @param btree The BTree this page belongs to.
     * @param revision The page revision
     * @param nbPageElems The number of elements this page will contain
     */
    @SuppressWarnings("unchecked")
    Leaf( BTree<K, V> btree, long revision, int nbPageElems )
    {
        super( btree, revision, nbPageElems );
        values = ( ValueHolder<V>[] ) Array.newInstance( ValueHolder.class, btree.getPageNbElem() );
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
        if ( nbPageElems < btree.getPageNbElem() )
        {
            // The current page is not full, it can contain the added element.
            // We insert it into a copied page and return the result
            return modifyLeaf( transaction, key, value, pos );
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
    @SuppressWarnings("unchecked")
    @Override
    /* no qualifier */DeleteResult<K, V> delete( WriteTransaction transaction, K key, Page<K, V> parent, int parentPos )
        throws IOException
    {
        long revision = transaction.getRevision();
        
        // Check that the leaf is not empty
        if ( nbPageElems == 0 )
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

        // Get the removed element
        Tuple<K, V> removedElement;

        // flag to detect if a key was completely removed
        int index = -( pos + 1 );

        ValueHolder<V> valueHolder = values[index];

        // we have to delete the whole value
        removedElement = new Tuple<>( keys[index].getKey(), valueHolder.get() ); // the entire value was removed

        Leaf<K, V> newLeaf;

        // No value, we can remove the key
        newLeaf = new Leaf<>( btree, revision, nbPageElems - 1 );
        newLeaf.initId( transaction.getRecordManagerHeader() );

        // Create the result
        DeleteResult<K, V> defaultResult = new RemoveResult<>( newLeaf, removedElement );

        // If the parent is null, then this page is the root page.
        if ( parent == null )
        {
            // Just remove the entry if it's present, or replace it if we have more than one value in the ValueHolder
            copyAfterRemovingElement( newLeaf, index );

            // The current page is added in the copied page list
            defaultResult.addCopiedPage( this );

            return defaultResult;
        }
        else
        {
            // The current page is not the root. Check if the leaf has more than N/2
            // elements
            int halfSize = btree.getPageNbElem() / 2;

            if ( nbPageElems == halfSize )
            {
                // We have to find a sibling now, and either borrow an entry from it
                // if it has more than N/2 elements, or to merge the two pages.
                // Check in both next and previous page, if they have the same parent
                // and select the biggest page with the same parent to borrow an element.
                int siblingPos = selectSibling( transaction, parent, parentPos );
                Leaf<K, V> sibling = ( Leaf<K, V> ) ( ( ( Node<K, V> ) parent )
                    .getPage( siblingPos ) );

                if ( sibling.nbPageElems == halfSize )
                {
                    // We will merge the current page with its sibling
                    return mergeWithSibling( transaction, removedElement, sibling, siblingPos < parentPos , index );
                }
                else
                {
                    // We can borrow the element from the left sibling
                    if ( siblingPos < parentPos )
                    {
                        return borrowFromLeft( transaction, removedElement, sibling, index );
                    }
                    else
                    {
                        // Borrow from the right sibling
                        return borrowFromRight( transaction, removedElement, sibling, index );
                    }
                }
            }
            else
            {
                // The page has more than N/2 elements.
                // We simply remove the element from the page, and if it was the leftmost,
                // we return the new pivot (it will replace any instance of the removed
                // key in its parents)
                copyAfterRemovingElement( newLeaf, index );

                // The current page is added in the copied page list
                defaultResult.addCopiedPage( this );

                return defaultResult;
            }
        }
    }


    /**
     * Merges the sibling with the current leaf, after having removed the element in the page.
     *
     * @param revision The new revision
     * @param sibling The sibling we will merge with
     * @param isLeft Tells if the sibling is on the left or on the right
     * @param pos The position of the removed element
     * @return The new created leaf containing the sibling and the old page.
     * @throws IOException If we have an error while trying to access the page
     */
    private DeleteResult<K, V> mergeWithSibling( WriteTransaction transaction, Tuple<K, V> removedElement,
        Leaf<K, V> sibling, boolean isLeft, int pos ) throws IOException
    {
        long revision = transaction.getRevision();
        
        // Create the new page. It will contain N - 1 elements (the maximum number)
        // as we merge two pages that contain N/2 elements minus the one we remove
        Leaf<K, V> newLeaf = new Leaf<>( btree, revision, btree.getPageNbElem() - 1 );
        newLeaf.initId( transaction.getRecordManagerHeader() );

        if ( isLeft )
        {
            // The sibling is on the left
            // Copy all the elements from the sibling first
            System.arraycopy( sibling.keys, 0, newLeaf.keys, 0, sibling.nbPageElems );
            System.arraycopy( sibling.values, 0, newLeaf.values, 0, sibling.nbPageElems );

            // Copy all the elements from the page up to the deletion position
            System.arraycopy( keys, 0, newLeaf.keys, sibling.nbPageElems, pos );
            System.arraycopy( values, 0, newLeaf.values, sibling.nbPageElems, pos );

            // And copy the remaining elements after the deletion point
            System.arraycopy( keys, pos + 1, newLeaf.keys, sibling.nbPageElems + pos, nbPageElems - pos - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, sibling.nbPageElems + pos, nbPageElems - pos - 1 );
        }
        else
        {
            // The sibling is on the right
            // Copy all the elements from the page up to the deletion position
            System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
            System.arraycopy( values, 0, newLeaf.values, 0, pos );

            // Then copy the remaining elements after the deletion point
            System.arraycopy( keys, pos + 1, newLeaf.keys, pos, nbPageElems - pos - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, pos, nbPageElems - pos - 1 );

            // And copy all the elements from the sibling
            System.arraycopy( sibling.keys, 0, newLeaf.keys, nbPageElems - 1, sibling.nbPageElems );
            System.arraycopy( sibling.values, 0, newLeaf.values, nbPageElems - 1, sibling.nbPageElems );
        }

        // And create the result
        DeleteResult<K, V> result = new MergedWithSiblingResult<>( newLeaf, removedElement );

        result.addCopiedPage( this );
        result.addCopiedPage( sibling );

        return result;
    }


    /**
     * Borrows an element from the left sibling, creating a new sibling with one
     * less element and creating a new page where the element to remove has been
     * deleted and the borrowed element added on the left.
     *
     * @param revision The new revision for all the pages
     * @param sibling The left sibling
     * @param pos The position of the element to remove
     * @return The resulting pages
     * @throws IOException If we have an error while trying to access the page
     */
    private DeleteResult<K, V> borrowFromLeft( WriteTransaction transaction, Tuple<K, V> removedElement, Leaf<K, V> sibling,
        int pos )
        throws IOException
    {
        long revision = transaction.getRevision();
        
        // The sibling is on the left, borrow the rightmost element
        K siblingKey = sibling.keys[sibling.nbPageElems - 1].getKey();
        ValueHolder<V> siblingValue;
        siblingValue = sibling.values[sibling.nbPageElems - 1];

        // Create the new sibling, with one less element at the end
        Leaf<K, V> newSibling = ( Leaf<K, V> ) sibling.copy( transaction, sibling.nbPageElems - 1 );
        newSibling.initId( transaction.getRecordManagerHeader() );

        // Create the new page and add the new element at the beginning
        // First copy the current page, with the same size
        Leaf<K, V> newLeaf = new Leaf<>( btree, revision, nbPageElems );
        newLeaf.initId( transaction.getRecordManagerHeader() );

        // Insert the borrowed element
        newLeaf.keys[0] = new KeyHolder<K>( btree.getKeySerializer(), siblingKey );
        newLeaf.values[0] = siblingValue;

        // Copy the keys and the values up to the insertion position,
        System.arraycopy( keys, 0, newLeaf.keys, 1, pos );
        System.arraycopy( values, 0, newLeaf.values, 1, pos );

        // And copy the remaining elements
        System.arraycopy( keys, pos + 1, newLeaf.keys, pos + 1, keys.length - pos - 1 );
        System.arraycopy( values, pos + 1, newLeaf.values, pos + 1, values.length - pos - 1 );

        DeleteResult<K, V> result = new BorrowedFromLeftResult<>( newLeaf, newSibling, removedElement );

        // Add the copied pages to the list
        result.addCopiedPage( this );
        result.addCopiedPage( sibling );

        return result;
    }


    /**
     * Borrows an element from the right sibling, creating a new sibling with one
     * less element and creating a new page where the element to remove has been
     * deleted and the borrowed element added on the right.
     *
     * @param revision The new revision for all the pages
     * @param sibling The right sibling
     * @param pos The position of the element to remove
     * @return The resulting pages
     * @throws IOException If we have an error while trying to access the page
     */
    private DeleteResult<K, V> borrowFromRight( WriteTransaction transaction, Tuple<K, V> removedElement, Leaf<K, V> sibling,
        int pos )
        throws IOException
    {
        long revision = transaction.getRevision();
        
        // The sibling is on the left, borrow the rightmost element
        K siblingKey = sibling.keys[0].getKey();
        ValueHolder<V> siblingHolder;
        siblingHolder = sibling.values[0];

        // Create the new sibling
        Leaf<K, V> newSibling = new Leaf<>( btree, revision, sibling.nbPageElems - 1 );
        newSibling.initId( transaction.getRecordManagerHeader() );

        // Copy the keys and the values from 1 to N in the new sibling
        System.arraycopy( sibling.keys, 1, newSibling.keys, 0, sibling.nbPageElems - 1 );
        System.arraycopy( sibling.values, 1, newSibling.values, 0, sibling.nbPageElems - 1 );

        // Create the new page and add the new element at the end
        // First copy the current page, with the same size
        Leaf<K, V> newLeaf = new Leaf<>( btree, revision, nbPageElems );
        newLeaf.initId( transaction.getRecordManagerHeader() );

        // Insert the borrowed element at the end
        newLeaf.keys[nbPageElems - 1] = new KeyHolder<K>( btree.getKeySerializer(), siblingKey );
        newLeaf.values[nbPageElems - 1] = siblingHolder;

        // Copy the keys and the values up to the deletion position,
        System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
        System.arraycopy( values, 0, newLeaf.values, 0, pos );

        // And copy the remaining elements
        System.arraycopy( keys, pos + 1, newLeaf.keys, pos, keys.length - pos - 1 );
        System.arraycopy( values, pos + 1, newLeaf.values, pos, values.length - pos - 1 );

        DeleteResult<K, V> result = new BorrowedFromRightResult<>( newLeaf, newSibling, removedElement );

        // Add the copied pages to the list
        result.addCopiedPage( this );
        result.addCopiedPage( sibling );

        return result;
    }


    /**
     * Copies the elements of the current page to a new page
     *
     * @param keyRemoved a flag stating if the key was removed
     * @param newLeaf The new page into which the remaining keys and values will be copied
     * @param pos The position into the page of the element to remove
     * @throws IOException If we have an error while trying to access the page
     */
    private void copyAfterRemovingElement( Leaf<K, V> newLeaf, int pos )
        throws IOException
    {
        // Deal with the special case of a page with only one element by skipping
        // the copy, as we won't have any remaining  element in the page
        if ( nbPageElems == 1 )
        {
            return;
        }

        // Copy the keys and the values up to the insertion position
        System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
        System.arraycopy( values, 0, newLeaf.values, 0, pos );

        // And copy the elements after the position
        System.arraycopy( keys, pos + 1, newLeaf.keys, pos, keys.length - pos - 1 );
        System.arraycopy( values, pos + 1, newLeaf.values, pos, values.length - pos - 1 );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V get( K key ) throws KeyNotFoundException, IOException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            return values[-( pos + 1 )].get();
        }
        else
        {
            throw KeyNotFoundException.INSTANCE;
        }
    }


    /**
     * {@inheritDoc}
     */
    /* No qualifier */KeyHolder<K> getKeyHolder( int pos )
    {
        if ( pos < nbPageElems )
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
    public boolean hasKey( K key )
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            return true;
        }

        return false;
    }


    @Override
    public boolean contains( K key, V value ) throws IOException
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
        if ( pos < nbPageElems )
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
    @Override
    /* no qualifier */void setValue( int pos, ValueHolder<V> value )
    {
        values[pos] = value;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TupleCursor<K, V> browse( Transaction transaction, K key, ParentPos<K, V>[] stack, int depth )
    {
        int pos = findPos( key );

        // First use case : the leaf is empty (this is a root page)
        if ( nbPageElems == 0 )
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
            else if ( pos < nbPageElems )
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

        if ( nbPageElems == 0 )
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
     * @param nbPageElems The number of elements to copy
     * @return The copied page
     */
    public Page<K, V> copy( WriteTransaction transaction, int nbPageElems )
    {
        Leaf<K, V> newLeaf = new Leaf<>( btree, transaction.getRevision(), nbPageElems );
        newLeaf.setId( id );

        // Copy the keys and the values
        System.arraycopy( keys, 0, newLeaf.keys, 0, nbPageElems );

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

                // Stop when we have copied nbPageElems values
                if ( pos == nbPageElems )
                {
                    break;
                }
            }
        }

        return newLeaf;
    }


    /**
     * Copy the current page and all of the keys, values and children, if it's not a leaf.
     *
     * @param revision The new revision
     * @param nbPageElems The number of elements to copy
     * @return The copied page
     */
    @Override
    public Page<K, V> copy( WriteTransaction transaction )
    {
        long revision = transaction.getRevision();
        
        Leaf<K, V> newLeaf = new Leaf<>( btree, revision, nbPageElems );
        newLeaf.initId( transaction.getRecordManagerHeader() );

        // Copy the keys and the values
        System.arraycopy( keys, 0, newLeaf.keys, 0, nbPageElems );

        if ( ( values != null ) && ( nbPageElems > 0 ) )
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

                // Stop when we have copied nbPageElems values
                if ( pos == nbPageElems )
                {
                    break;
                }
            }
        }

        return newLeaf;
    }


    /**
     * Copy the current page if needed, and replace the value at the position we have found the key.
     *
     * @param transaction The {@link WriteTransaction} we are processing this update in
     * @param key The new key
     * @param value the new value
     * @param pos The position of the key in the page
     * @return A {@link InserResult} instance, containing the reference to the new page, or an 
     * {@link ExistsResult} instance, if the value already exists in the page.
     * @throws IOException If we have an error while trying to access the page
     */
    private InsertResult<K, V> replaceElement( WriteTransaction transaction, V value, int pos )
        throws IOException
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
            newLeaf = ( Leaf<K, V> ) copy( transaction, nbPageElems );
        }

        // Get the previous value from the leaf
        V replacedValue = newLeaf.values[pos].get();
        newLeaf.values[pos].set( value );

        // Create the result
        InsertResult<K, V> result = new ModifyResult<>( newLeaf, replacedValue );
        
        // If we have copied a page, put it in the transaction
        if ( revision != transaction.getRevision() )
        {
            transaction.addWALObject( newLeaf );
            
            // And store the old leaf into teh CopiedPages B-tree, if we are not
            // processing the CopiedPages B-tree itself
            if ( btree.getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
            {
                transaction.addCopiedWALObject( this );
            }
        }

        return result;
    }

    
    /**
     * Add a new element in a page, at a gven position. The page will not be fulled up by this addition.
     *
     * @param transaction The {@link WriteTransaction} we are processing this update in
     * @param key The key to add
     * @param value The value to add
     * @param pos The position of the insertion of the new element
     * @return An {@link InserResult} instance, containing a reference to the newly created Leaf.
     * @throws IOException If we have an error while trying to access the page
     */
    private InsertResult<K, V> modifyLeaf( WriteTransaction transaction, K key, V value, int pos ) throws IOException
    {
        Leaf<K, V> newLeaf = this;
        
        // If the page was created in a previous revision, copy it
        if ( revision != transaction.getRevision() )
        {
            newLeaf = ( Leaf<K, V> ) copy( transaction, nbPageElems );
        }

        // Inject the <K, V> into the new leaf
        Page<K, V> modifiedPage = newLeaf.addElement( key, value, pos );

        // And return a modified result
        InsertResult<K, V> result = new ModifyResult<>( modifiedPage, null );
        
        // Add the new leaf in the transaction pages map, and add
        // the old leaf into the CopiedPages B-tree, if needed
        if ( revision != transaction.getRevision() )
        {
            transaction.addWALObject( newLeaf );
            
            if ( btree.getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
            {
                transaction.addCopiedWALObject( this );
            }
        }

        return result;
    }
    

    /**
     * Adds a new <K, V> into the current page at a given position. We return the
     * modified page. The modified page will have one more element than the current page.
     *
     * @param revision The revision of the modified page
     * @param key The key to insert
     * @param value The value to insert
     * @param pos The position into the page
     * @return The modified page with the <K,V> element added
     */
    private Page<K, V> addElement( K key, V value, int pos )
    {
        // Create the value holder
        ValueHolder<V> valueHolder = new ValueHolder<>( btree, value );

        // Deal with the special case of an empty page
        if ( nbPageElems == 0 )
        {
            keys[0] = new KeyHolder<>( btree.getKeySerializer(), key );
            values[0] = valueHolder;
        }
        else
        {
            // Copy the keys and the values from the insertion point one position to the right
            int nbElementToMove = nbPageElems - pos;
            System.arraycopy( keys, pos, keys, pos + 1, nbElementToMove );
            System.arraycopy( values, pos, values, pos + 1, nbElementToMove );

            // Add the new element
            keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), key );
            values[pos] = valueHolder;
        }

        nbPageElems++;

        return this;
    }


    /**
     * Split a full page into two new pages, a left, a right and a pivot element. The new pages will
     * each contains half of the original elements. <br/>
     * The pivot will be computed, depending on the place we will inject the newly added element. <br/>
     * If the newly added element is in the middle, we will use it
     * as a pivot. Otherwise, we will use either the last element in the left page if the element is added
     * on the left, or the first element in the right page if it's added on the right.
     *
     * @param transaction The {@link WriteTransaction} we are processing this update in
     * @param key The key to add
     * @param value The value to add
     * @param pos The position of the insertion of the new element
     * @return An {@SplitResult} instance containing the pivot, and the new left and right pages. We haven't 
     * created the new parent Node yet.
     * @throws IOException If we have an error while trying to access the page
     */
    private SplitResult<K, V> addAndSplit( WriteTransaction transaction, K key, V value, int pos ) throws IOException
    {
        long revision = transaction.getRevision();
        int middle = btree.getPageNbElem() >> 1;
        Leaf<K, V> leftLeaf;
        Leaf<K, V> rightLeaf;
        ValueHolder<V> valueHolder = new ValueHolder<>( btree, value );

        // Determinate where to store the new value
        if ( pos <= middle )
        {
            // The left page will contain the new value
            leftLeaf = new Leaf<>( btree, revision, middle + 1 );
            leftLeaf.initId( transaction.getRecordManagerHeader() );

            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, 0, leftLeaf.keys, 0, pos );
            System.arraycopy( values, 0, leftLeaf.values, 0, pos );

            // Add the new element
            leftLeaf.keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), key );
            leftLeaf.values[pos] = valueHolder;

            // And copy the remaining elements
            System.arraycopy( keys, pos, leftLeaf.keys, pos + 1, middle - pos );
            System.arraycopy( values, pos, leftLeaf.values, pos + 1, middle - pos );

            // Now, create the right page
            rightLeaf = new Leaf<>( btree, revision, middle );
            rightLeaf.initId( transaction.getRecordManagerHeader() );

            // Copy the keys and the values in the right page
            System.arraycopy( keys, middle, rightLeaf.keys, 0, middle );
            System.arraycopy( values, middle, rightLeaf.values, 0, middle );
        }
        else
        {
            // Create the left page
            leftLeaf = new Leaf<>( btree, revision, middle );
            leftLeaf.initId( transaction.getRecordManagerHeader() );

            // Copy all the element into the left page
            System.arraycopy( keys, 0, leftLeaf.keys, 0, middle );
            System.arraycopy( values, 0, leftLeaf.values, 0, middle );

            // Now, create the right page
            rightLeaf = new Leaf<>( btree, revision, middle + 1 );
            rightLeaf.initId( transaction.getRecordManagerHeader() );

            int rightPos = pos - middle;

            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, middle, rightLeaf.keys, 0, rightPos );
            System.arraycopy( values, middle, rightLeaf.values, 0, rightPos );

            // Add the new element
            rightLeaf.keys[rightPos] = new KeyHolder<>( btree.getKeySerializer(), key );
            rightLeaf.values[rightPos] = valueHolder;

            // And copy the remaining elements
            System.arraycopy( keys, pos, rightLeaf.keys, rightPos + 1, nbPageElems - pos );
            System.arraycopy( values, pos, rightLeaf.values, rightPos + 1, nbPageElems - pos );
        }

        // Get the pivot
        K pivot = rightLeaf.keys[0].getKey();
        
        // Inject the created leaves in the transaction
        transaction.addWALObject( leftLeaf );
        transaction.addWALObject( rightLeaf );
        
        // And remove the original page from the transaction
        transaction.removeWALObject( id );
        
        // Add the old page into the CopiedPages B-tree if we aren't processing it
        if ( btree.getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
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
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> findLeftMost() throws IOException
    {
        K key = keys[0].getKey();

        return new Tuple<>( key, values[0].get() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> findRightMost() throws IOException
    {

        K key = keys[nbPageElems - 1].getKey();
        V value = values[nbPageElems - 1].get();

        return new Tuple<>( key, value );
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
        throws IOException
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
        int bufferSize = RecordManager.LONG_SIZE + RecordManager.LONG_SIZE + RecordManager.INT_SIZE;

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
     * <li>the revision : a long</li>
     * <li>the number of elements in the page : a positive int </li>
     * <li>the keys : an array of serialized keys</li>
     * <li>the values : an array of serialized values</li>
     * </ul>
     * {@inheritDoc}
     */
    @Override
    public PageIO[] serialize( WriteTransaction transaction ) throws IOException
    {
        RecordManager recordManager = transaction.getRecordManager();
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        if ( nbPageElems == 0 )
        {
            return serializeRootPage( transaction, revision );
        }
        else
        {
            // Prepare a list of byte[] that will contain the serialized page
            int nbBuffers = 1 + 1 + 1 + 1 + nbPageElems * 2;
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
            buffer = IntSerializer.serialize( nbPageElems );
            serializedData.add( buffer );
            serializedSize += buffer.length;

            // Iterate on the keys and values. We first serialize the value, then the key
            // until we are done with all of them. If we are serializing a page, we have
            // to serialize one more value
            for ( int pos = 0; pos < nbPageElems; pos++ )
            {
                // Start with the value
                dataSize += serializeLeafValue( pos, serializedData );
                dataSize += serializeLeafKey( pos, serializedData );
            }

            // Store the data size
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
    @Override
    public Page<K, V> deserialize( Transaction transaction, ByteBuffer byteBuffer ) throws IOException
    {
        // Iterate on the keys and values. We first serialize the value, then the key
        // until we are done with all of them. If we are serializing a page, we have
        // to serialize one more value
        for ( int pos = 0; pos < nbPageElems; pos++ )
        {
            // Start with the value
            V value = btree.getValueSerializer().deserialize( byteBuffer );
            ValueHolder<V> valueHolder = new ValueHolder<>( btree, value );
            BTreeFactory.setValue( btree, this, pos, valueHolder );

            // Then the key
            K key = btree.getKeySerializer().deserialize( byteBuffer );
            BTreeFactory.setKey( btree, this, pos, key );
        }
        
        return this;
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

        if ( nbPageElems > 0 )
        {
            boolean isFirst = true;

            for ( int i = 0; i < nbPageElems; i++ )
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


    /**
     * same as {@link #addElement(long, Object, Object, int)} except the values are not copied.
     * This method is only used while inserting an element into a sub-BTree.
     */
    private Page<K, V> addSubTreeElement( WriteTransaction transaction, K key, int pos )
    {
        long revision = transaction.getRevision();
        
        // First copy the current page, but add one element in the copied page
        Leaf<K, V> newLeaf = new Leaf<>( btree, revision, nbPageElems + 1 );
        newLeaf.initId( transaction.getRecordManagerHeader() );

        // Deal with the special case of an empty page
        if ( nbPageElems == 0 )
        {
            newLeaf.keys[0] = new KeyHolder<K>( btree.getKeySerializer(), key );
        }
        else
        {
            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, 0, newLeaf.keys, 0, pos );

            // Add the new element
            newLeaf.keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), key );

            // And copy the remaining elements
            System.arraycopy( keys, pos, newLeaf.keys, pos + 1, keys.length - pos );
        }

        return newLeaf;
    }


    /**
     * same as {@link #addAndSplit(long, Object, Object, int)} except the values are not copied.
     * This method is only used while inserting an element into a sub-BTree.
     */
    private InsertResult<K, V> addAndSplitSubTree( WriteTransaction transaction, K key, int pos )
    {
        long revision = transaction.getRevision();
        
        int middle = btree.getPageNbElem() >> 1;
        Leaf<K, V> leftLeaf;
        Leaf<K, V> rightLeaf;

        // Determinate where to store the new value
        if ( pos <= middle )
        {
            // The left page will contain the new value
            leftLeaf = new Leaf<>( btree, revision, middle + 1 );
            leftLeaf.initId( transaction.getRecordManagerHeader() );

            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, 0, leftLeaf.keys, 0, pos );

            // Add the new element
            leftLeaf.keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), key );

            // And copy the remaining elements
            System.arraycopy( keys, pos, leftLeaf.keys, pos + 1, middle - pos );

            // Now, create the right page
            rightLeaf = new Leaf<>( btree, revision, middle );
            rightLeaf.initId( transaction.getRecordManagerHeader() );

            // Copy the keys and the values in the right page
            System.arraycopy( keys, middle, rightLeaf.keys, 0, middle );
        }
        else
        {
            // Create the left page
            leftLeaf = new Leaf<>( btree, revision, middle );
            leftLeaf.initId( transaction.getRecordManagerHeader() );

            // Copy all the element into the left page
            System.arraycopy( keys, 0, leftLeaf.keys, 0, middle );

            // Now, create the right page
            rightLeaf = new Leaf<>( btree, revision, middle + 1 );
            rightLeaf.initId( transaction.getRecordManagerHeader() );

            int rightPos = pos - middle;

            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, middle, rightLeaf.keys, 0, rightPos );

            // Add the new element
            rightLeaf.keys[rightPos] = new KeyHolder<K>( btree.getKeySerializer(), key );

            // And copy the remaining elements
            System.arraycopy( keys, pos, rightLeaf.keys, rightPos + 1, nbPageElems - pos );
        }

        // Get the pivot
        K pivot = rightLeaf.keys[0].getKey();

        // Create the result
        return new SplitResult<>( pivot, leftLeaf, rightLeaf );
    }


    /**
     * {@inheritDoc}
     */
    public KeyCursor<K> browseKeys( ReadTransaction transaction, ParentPos<K, K>[] stack, int depth )
    {
        int pos = 0;
        KeyCursor<K> cursor;

        if ( nbPageElems == 0 )
        {
            // The tree is empty, it's the root, we have nothing to return
            stack[depth] = new ParentPos<>( null, -1 );

            return new KeyCursor<>( transaction, stack, depth );
        }
        else
        {
            // Start at the beginning of the page
            ParentPos<K, K> parentPos = new ParentPos( this, pos );

            stack[depth] = parentPos;

            cursor = new KeyCursor<>( transaction, stack, depth );
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
        
        if ( offset == RecordManager.NO_PAGE )
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

        if ( nbPageElems > 0 )
        {
            boolean isFirst = true;

            for ( int i = 0; i < nbPageElems; i++ )
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
}
