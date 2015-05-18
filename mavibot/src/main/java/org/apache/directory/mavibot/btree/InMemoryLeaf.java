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

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;


/**
 * A MVCC Leaf. It stores the keys and values. It does not have any children.
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier */class InMemoryLeaf<K, V> extends AbstractPage<K, V>
{
    /** Values associated with keys */
    protected ValueHolder<V>[] values;


    /**
     * Constructor used to create a new Leaf when we read it from a file.
     *
     * @param btree The BTree this page belongs to.
     */
    InMemoryLeaf( BTree<K, V> btree )
    {
        super( btree );
    }


    /**
     * Internal constructor used to create Page instance used when a page is being copied or overflow
     *
     * @param btree The BTree this page belongs to.
     * @param revision The page revision
     * @param nbElems The number of elements this page will contain
     */
    @SuppressWarnings("unchecked")
    InMemoryLeaf( BTree<K, V> btree, long revision, int nbElems )
    {
        super( btree, revision, nbElems );

        this.values = ( InMemoryValueHolder<V>[] ) Array.newInstance( InMemoryValueHolder.class, nbElems );
    }


    /**
     * {@inheritDoc}
     */
    public InsertResult<K, V> insert( K key, V value, long revision ) throws IOException
    {
        // Find the key into this leaf
        int pos = findPos( key );

        if ( pos < 0 )
        {
            // We already have the key in the page : replace the value
            // into a copy of this page, unless the page has already be copied
            int index = -( pos + 1 );

            // Replace the existing value in a copy of the current page
            InsertResult<K, V> result = replaceElement( revision, key, value, index );

            return result;
        }

        // The key is not present in the leaf. We have to add it in the page
        if ( nbElems < btree.getPageSize() )
        {
            // The current page is not full, it can contain the added element.
            // We insert it into a copied page and return the result
            Page<K, V> modifiedPage = addElement( revision, key, value, pos );

            InsertResult<K, V> result = new ModifyResult<K, V>( modifiedPage, null );
            result.addCopiedPage( this );

            return result;
        }
        else
        {
            // The Page is already full : we split it and return the overflow element,
            // after having created two pages.
            InsertResult<K, V> result = addAndSplit( revision, key, value, pos );
            result.addCopiedPage( this );

            return result;
        }
    }


    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    /* no qualifier */DeleteResult<K, V> delete( K key, V value, long revision, Page<K, V> parent, int parentPos )
        throws IOException
    {
        // Check that the leaf is not empty
        if ( nbElems == 0 )
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
        Tuple<K, V> removedElement = null;

        // flag to detect if a key was completely removed
        boolean keyRemoved = false;

        int index = -( pos + 1 );

        ValueHolder<V> valueHolder = values[index];

        if ( value == null )
        {
            // we have to delete the whole value
            removedElement = new Tuple<K, V>( getKey( index ), valueHolder.getCursor().next() ); // the entire value was removed
            keyRemoved = true;
        }
        else
        {
            if ( valueHolder.contains( value ) )
            {
                // this is a case to delete entire <K,sub-BTree> or <K,single-V>
                if ( valueHolder.size() == 1 )
                {
                    // Ok, we can remove the value
                    removedElement = new Tuple<K, V>( getKey( index ), null ); // the entire value was removed
                    keyRemoved = true;
                }
                else
                {
                    // Update the ValueHolder
                    valueHolder.remove( value );
                    removedElement = new Tuple<K, V>( getKey( index ), value ); // the entire value was removed
                }
            }
            else
            {
                // Not found
                return NotPresentResult.NOT_PRESENT;
            }
        }

        InMemoryLeaf<K, V> newLeaf = null;

        if ( keyRemoved )
        {
            newLeaf = new InMemoryLeaf<K, V>( btree, revision, nbElems - 1 );
        }
        else
        {
            newLeaf = new InMemoryLeaf<K, V>( btree, revision, nbElems );
        }

        // Create the result
        DeleteResult<K, V> defaultResult = new RemoveResult<K, V>( newLeaf, removedElement );

        // If the parent is null, then this page is the root page.
        if ( parent == null )
        {
            // Just remove the entry if it's present
            copyAfterRemovingElement( keyRemoved, newLeaf, index );

            // The current page is added in the copied page list
            defaultResult.addCopiedPage( this );

            return defaultResult;
        }
        else if ( keyRemoved )
        {
            // The current page is not the root. Check if the leaf has more than N/2
            // elements
            int halfSize = btree.getPageSize() / 2;

            if ( nbElems == halfSize )
            {
                // We have to find a sibling now, and either borrow an entry from it
                // if it has more than N/2 elements, or to merge the two pages.
                // Check in both next and previous page, if they have the same parent
                // and select the biggest page with the same parent to borrow an element.
                int siblingPos = selectSibling( parent, parentPos );
                InMemoryLeaf<K, V> sibling = ( InMemoryLeaf<K, V> ) ( ( ( InMemoryNode<K, V> ) parent )
                    .getPage( siblingPos ) );

                if ( sibling.getNbElems() == halfSize )
                {
                    // We will merge the current page with its sibling
                    DeleteResult<K, V> result = mergeWithSibling( removedElement, revision, sibling,
                        ( siblingPos < parentPos ), index );

                    return result;
                }
                else
                {
                    // We can borrow the element from the left sibling
                    if ( siblingPos < parentPos )
                    {
                        DeleteResult<K, V> result = borrowFromLeft( removedElement, revision, sibling, index );

                        return result;
                    }
                    else
                    {
                        // Borrow from the right sibling
                        DeleteResult<K, V> result = borrowFromRight( removedElement, revision, sibling, index );

                        return result;
                    }
                }
            }
            else
            {
                // The page has more than N/2 elements.
                // We simply remove the element from the page, and if it was the leftmost,
                // we return the new pivot (it will replace any instance of the removed
                // key in its parents)
                copyAfterRemovingElement( keyRemoved, newLeaf, index );

                // The current page is added in the copied page list
                defaultResult.addCopiedPage( this );

                return defaultResult;
            }
        }
        else
        {
            // Last, not least : we can copy the full page
            // Copy the keys and the values
            System.arraycopy( getKeys(), 0, newLeaf.getKeys(), 0, nbElems );
            System.arraycopy( values, 0, newLeaf.values, 0, nbElems );

            // The current page is added in the copied page list
            defaultResult.addCopiedPage( this );

            return defaultResult;
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
    private DeleteResult<K, V> mergeWithSibling( Tuple<K, V> removedElement, long revision, InMemoryLeaf<K, V> sibling,
        boolean isLeft, int pos )
        throws EndOfFileExceededException, IOException
    {
        // Create the new page. It will contain N - 1 elements (the maximum number)
        // as we merge two pages that contain N/2 elements minus the one we remove
        InMemoryLeaf<K, V> newLeaf = new InMemoryLeaf<K, V>( btree, revision, btree.getPageSize() - 1 );

        if ( isLeft )
        {
            // The sibling is on the left
            // Copy all the elements from the sibling first
            System.arraycopy( sibling.getKeys(), 0, newLeaf.getKeys(), 0, sibling.nbElems );
            System.arraycopy( sibling.values, 0, newLeaf.values, 0, sibling.nbElems );

            // Copy all the elements from the page up to the deletion position
            System.arraycopy( getKeys(), 0, newLeaf.getKeys(), sibling.nbElems, pos );
            System.arraycopy( values, 0, newLeaf.values, sibling.nbElems, pos );

            // And copy the remaining elements after the deletion point
            System.arraycopy( getKeys(), pos + 1, newLeaf.getKeys(), sibling.nbElems + pos, nbElems - pos - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, sibling.nbElems + pos, nbElems - pos - 1 );
        }
        else
        {
            // The sibling is on the right
            // Copy all the elements from the page up to the deletion position
            System.arraycopy( getKeys(), 0, newLeaf.getKeys(), 0, pos );
            System.arraycopy( values, 0, newLeaf.values, 0, pos );

            // Then copy the remaining elements after the deletion point
            System.arraycopy( getKeys(), pos + 1, newLeaf.getKeys(), pos, nbElems - pos - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, pos, nbElems - pos - 1 );

            // And copy all the elements from the sibling
            System.arraycopy( sibling.getKeys(), 0, newLeaf.getKeys(), nbElems - 1, sibling.nbElems );
            System.arraycopy( sibling.values, 0, newLeaf.values, nbElems - 1, sibling.nbElems );
        }

        // And create the result
        DeleteResult<K, V> result = new MergedWithSiblingResult<K, V>( newLeaf,
            removedElement );

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
    private DeleteResult<K, V> borrowFromLeft( Tuple<K, V> removedElement, long revision, InMemoryLeaf<K, V> sibling,
        int pos )
        throws IOException
    {
        // The sibling is on the left, borrow the rightmost element
        K siblingKey = sibling.getKey( sibling.getNbElems() - 1 );
        ValueHolder<V> siblingValue = sibling.values[sibling.getNbElems() - 1];

        // Create the new sibling, with one less element at the end
        InMemoryLeaf<K, V> newSibling = ( InMemoryLeaf<K, V> ) sibling.copy( revision, sibling.getNbElems() - 1 );

        // Create the new page and add the new element at the beginning
        // First copy the current page, with the same size
        InMemoryLeaf<K, V> newLeaf = new InMemoryLeaf<K, V>( btree, revision, nbElems );

        // Insert the borrowed element
        newLeaf.setKey( 0, new KeyHolder<K>( siblingKey ) );
        newLeaf.values[0] = siblingValue;

        // Copy the keys and the values up to the insertion position,
        System.arraycopy( getKeys(), 0, newLeaf.getKeys(), 1, pos );
        System.arraycopy( values, 0, newLeaf.values, 1, pos );

        // And copy the remaining elements
        System.arraycopy( getKeys(), pos + 1, newLeaf.getKeys(), pos + 1, getKeys().length - pos - 1 );
        System.arraycopy( values, pos + 1, newLeaf.values, pos + 1, values.length - pos - 1 );

        DeleteResult<K, V> result = new BorrowedFromLeftResult<K, V>( newLeaf, newSibling, removedElement );

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
    private DeleteResult<K, V> borrowFromRight( Tuple<K, V> removedElement, long revision, InMemoryLeaf<K, V> sibling,
        int pos )
        throws IOException
    {
        // The sibling is on the left, borrow the rightmost element
        K siblingKey = sibling.getKey( 0 );
        ValueHolder<V> siblingHolder = sibling.values[0];

        // Create the new sibling
        InMemoryLeaf<K, V> newSibling = new InMemoryLeaf<K, V>( btree, revision, sibling.getNbElems() - 1 );

        // Copy the keys and the values from 1 to N in the new sibling
        System.arraycopy( sibling.getKeys(), 1, newSibling.getKeys(), 0, sibling.nbElems - 1 );
        System.arraycopy( sibling.values, 1, newSibling.values, 0, sibling.nbElems - 1 );

        // Create the new page and add the new element at the end
        // First copy the current page, with the same size
        InMemoryLeaf<K, V> newLeaf = new InMemoryLeaf<K, V>( btree, revision, nbElems );

        // Insert the borrowed element at the end
        newLeaf.setKey( nbElems - 1, new KeyHolder<K>( siblingKey ) );
        newLeaf.values[nbElems - 1] = siblingHolder;

        // Copy the keys and the values up to the deletion position,
        System.arraycopy( getKeys(), 0, newLeaf.getKeys(), 0, pos );
        System.arraycopy( values, 0, newLeaf.values, 0, pos );

        // And copy the remaining elements
        System.arraycopy( getKeys(), pos + 1, newLeaf.getKeys(), pos, getKeys().length - pos - 1 );
        System.arraycopy( values, pos + 1, newLeaf.values, pos, values.length - pos - 1 );

        DeleteResult<K, V> result = new BorrowedFromRightResult<K, V>( newLeaf, newSibling, removedElement );

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
    private void copyAfterRemovingElement( boolean keyRemoved, InMemoryLeaf<K, V> newLeaf, int pos ) throws IOException
    {
        if ( keyRemoved )
        {
            // Deal with the special case of a page with only one element by skipping
            // the copy, as we won't have any remaining  element in the page
            if ( nbElems == 1 )
            {
                return;
            }

            // Copy the keys and the values up to the insertion position
            System.arraycopy( getKeys(), 0, newLeaf.getKeys(), 0, pos );
            System.arraycopy( values, 0, newLeaf.values, 0, pos );

            // And copy the elements after the position
            System.arraycopy( getKeys(), pos + 1, newLeaf.getKeys(), pos, getKeys().length - pos - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, pos, values.length - pos - 1 );
        }
        else
        // one of the many values of the same key was removed, no change in the number of keys
        {
            System.arraycopy( getKeys(), 0, newLeaf.getKeys(), 0, nbElems );
            System.arraycopy( values, 0, newLeaf.values, 0, nbElems );
        }
    }


    /**
     * {@inheritDoc}
     */
    public V get( K key ) throws KeyNotFoundException, IOException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            InMemoryValueHolder<V> valueHolder = ( InMemoryValueHolder<V> ) values[-( pos + 1 )];

            V value = valueHolder.getCursor().next();

            return value;
        }
        else
        {
            throw KeyNotFoundException.INSTANCE;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public ValueCursor<V> getValues( K key ) throws KeyNotFoundException, IOException, IllegalArgumentException
    {
        if ( !btree.isAllowDuplicates() )
        {
            throw new IllegalArgumentException( "Duplicates are not allowed in this tree" );
        }

        int pos = findPos( key );

        if ( pos < 0 )
        {
            InMemoryValueHolder<V> valueHolder = ( InMemoryValueHolder<V> ) values[-( pos + 1 )];

            return valueHolder.getCursor();
        }
        else
        {
            throw KeyNotFoundException.INSTANCE;
        }
    }


    /**
     * {@inheritDoc}
     */
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
        if ( pos < nbElems )
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
    public TupleCursor<K, V> browse( K key, ReadTransaction<K, V> transaction, ParentPos<K, V>[] stack, int depth )
    {
        int pos = findPos( key );
        TupleCursor<K, V> cursor = null;

        if ( pos < 0 )
        {
            pos = -( pos + 1 );

            // Start at the beginning of the page
            ParentPos<K, V> parentPos = new ParentPos<K, V>( this, pos );

            // Create the value cursor
            parentPos.valueCursor = values[pos].getCursor();

            stack[depth] = parentPos;

            cursor = new TupleCursor<K, V>( transaction, stack, depth );
        }
        else
        {
            // The key has not been found. Select the value just above, if we have one
            if ( pos < nbElems )
            {
                ParentPos<K, V> parentPos = new ParentPos<K, V>( this, pos );

                // Create the value cursor
                parentPos.valueCursor = values[pos].getCursor();

                stack[depth] = parentPos;

                cursor = new TupleCursor<K, V>( transaction, stack, depth );
            }
            else if ( nbElems > 0 )
            {
                // after the last element
                ParentPos<K, V> parentPos = new ParentPos<K, V>( this, nbElems - 1 );

                // Create the value cursor
                parentPos.valueCursor = values[nbElems - 1].getCursor();

                stack[depth] = parentPos;

                cursor = new TupleCursor<K, V>( transaction, stack, depth );

                try
                {
                    cursor.afterLast();
                }
                catch ( IOException e )
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            else
            {
                // Not found, because there are no elements : return a null cursor
                stack[depth] = null;

                cursor = new TupleCursor<K, V>( transaction, null, 0 );
            }
        }

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browse( ReadTransaction<K, V> transaction, ParentPos<K, V>[] stack, int depth )
    {
        int pos = 0;
        TupleCursor<K, V> cursor = null;

        if ( nbElems == 0 )
        {
            // The tree is empty, it's the root, we have nothing to return
            stack[depth] = new ParentPos<K, V>( null, -1 );

            return new TupleCursor<K, V>( transaction, stack, depth );
        }
        else
        {
            // Start at the beginning of the page
            ParentPos<K, V> parentPos = new ParentPos<K, V>( this, pos );

            // Create the value cursor
            parentPos.valueCursor = values[0].getCursor();

            stack[depth] = parentPos;

            cursor = new TupleCursor<K, V>( transaction, stack, depth );
        }

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public KeyCursor<K> browseKeys( ReadTransaction<K, K> transaction, ParentPos<K, K>[] stack, int depth )
    {
        int pos = 0;
        KeyCursor<K> cursor = null;

        if ( nbElems == 0 )
        {
            // The tree is empty, it's the root, we have nothing to return
            stack[depth] = new ParentPos<K, K>( null, -1 );

            return new KeyCursor<K>( transaction, stack, depth );
        }
        else
        {
            // Start at the beginning of the page
            ParentPos<K, K> parentPos = new ParentPos( this, pos );

            stack[depth] = parentPos;

            cursor = new KeyCursor<K>( transaction, stack, depth );
        }

        return cursor;
    }


    /**
     * Copy the current page and all of the keys, values and children, if it's not a leaf.
     *
     * @param revision The new revision
     * @param nbElems The number of elements to copy
     * @return The copied page
     */
    private Page<K, V> copy( long revision, int nbElems )
    {
        InMemoryLeaf<K, V> newLeaf = new InMemoryLeaf<K, V>( btree, revision, nbElems );

        // Copy the keys and the values
        System.arraycopy( getKeys(), 0, newLeaf.getKeys(), 0, nbElems );
        System.arraycopy( values, 0, newLeaf.values, 0, nbElems );

        return newLeaf;
    }


    /**
     * Copy the current page if needed, and replace the value at the position we have found the key.
     *
     * @param revision The new page revision
     * @param key The new key
     * @param value the new value
     * @param pos The position of the key in the page
     * @return The copied page
     * @throws IOException If we have an error while trying to access the page
     */
    private InsertResult<K, V> replaceElement( long revision, K key, V value, int pos )
        throws IOException
    {
        InMemoryLeaf<K, V> newLeaf = this;

        // Get the previous value from the leaf (it's a copy)
        ValueHolder<V> valueHolder = values[pos];

        boolean valueExists = valueHolder.contains( value );

        if ( this.revision != revision )
        {
            // The page hasn't been modified yet, we need to copy it first
            newLeaf = ( InMemoryLeaf<K, V> ) copy( revision, nbElems );
        }

        // Get the previous value from the leaf (it's a copy)
        valueHolder = newLeaf.values[pos];
        V replacedValue = null;

        if ( !valueExists && btree.isAllowDuplicates() )
        {
            valueHolder.add( value );
            newLeaf.values[pos] = valueHolder;
        }
        else if ( valueExists && btree.isAllowDuplicates() )
        {
            // As strange as it sounds, we need to remove the value to reinject it.
            // There are cases where the value retrieval just use one part of the
            // value only (typically for LDAP Entries, where we use the DN)
            replacedValue = valueHolder.remove( value );
            valueHolder.add( value );
        }
        else if ( !btree.isAllowDuplicates() )
        {
            replacedValue = valueHolder.replaceValueArray( value );
        }

        // Create the result
        InsertResult<K, V> result = new ModifyResult<K, V>( newLeaf, replacedValue );
        result.addCopiedPage( this );

        return result;
    }


    /**
     * Adds a new <K, V> into a copy of the current page at a given position. We return the
     * modified page. The new page will have one more element than the current page.
     *
     * @param revision The revision of the modified page
     * @param key The key to insert
     * @param value The value to insert
     * @param pos The position into the page
     * @return The modified page with the <K,V> element added
     */
    private Page<K, V> addElement( long revision, K key, V value, int pos )
    {
        // First copy the current page, but add one element in the copied page
        InMemoryLeaf<K, V> newLeaf = new InMemoryLeaf<K, V>( btree, revision, nbElems + 1 );

        // Atm, store the value in memory
        InMemoryValueHolder<V> valueHolder = new InMemoryValueHolder<V>( btree, value );

        // Deal with the special case of an empty page
        if ( nbElems == 0 )
        {
            newLeaf.setKey( 0, new KeyHolder<K>( key ) );
            newLeaf.values[0] = valueHolder;
        }
        else
        {
            // Copy the keys and the values up to the insertion position
            System.arraycopy( getKeys(), 0, newLeaf.getKeys(), 0, pos );
            System.arraycopy( values, 0, newLeaf.values, 0, pos );

            // Add the new element
            newLeaf.setKey( pos, new KeyHolder<K>( key ) );
            newLeaf.values[pos] = valueHolder;

            // And copy the remaining elements
            System.arraycopy( getKeys(), pos, newLeaf.getKeys(), pos + 1, getKeys().length - pos );
            System.arraycopy( values, pos, newLeaf.values, pos + 1, values.length - pos );
        }

        return newLeaf;
    }


    /**
     * Split a full page into two new pages, a left, a right and a pivot element. The new pages will
     * each contains half of the original elements. <br/>
     * The pivot will be computed, depending on the place
     * we will inject the newly added element. <br/>
     * If the newly added element is in the middle, we will use it
     * as a pivot. Otherwise, we will use either the last element in the left page if the element is added
     * on the left, or the first element in the right page if it's added on the right.
     *
     * @param revision The new revision for all the created pages
     * @param key The key to add
     * @param value The value to add
     * @param pos The position of the insertion of the new element
     * @return An OverflowPage containing the pivot, and the new left and right pages
     */
    private InsertResult<K, V> addAndSplit( long revision, K key, V value, int pos )
    {
        int middle = btree.getPageSize() >> 1;
        InMemoryLeaf<K, V> leftLeaf = null;
        InMemoryLeaf<K, V> rightLeaf = null;
        InMemoryValueHolder<V> valueHolder = new InMemoryValueHolder<V>( btree, value );

        // Determinate where to store the new value
        if ( pos <= middle )
        {
            // The left page will contain the new value
            leftLeaf = new InMemoryLeaf<K, V>( btree, revision, middle + 1 );

            // Copy the keys and the values up to the insertion position
            System.arraycopy( getKeys(), 0, leftLeaf.getKeys(), 0, pos );
            System.arraycopy( values, 0, leftLeaf.values, 0, pos );

            // Add the new element
            leftLeaf.setKey( pos, new KeyHolder<K>( key ) );
            leftLeaf.values[pos] = valueHolder;

            // And copy the remaining elements
            System.arraycopy( getKeys(), pos, leftLeaf.getKeys(), pos + 1, middle - pos );
            System.arraycopy( values, pos, leftLeaf.values, pos + 1, middle - pos );

            // Now, create the right page
            rightLeaf = new InMemoryLeaf<K, V>( btree, revision, middle );

            // Copy the keys and the values in the right page
            System.arraycopy( getKeys(), middle, rightLeaf.getKeys(), 0, middle );
            System.arraycopy( values, middle, rightLeaf.values, 0, middle );
        }
        else
        {
            // Create the left page
            leftLeaf = new InMemoryLeaf<K, V>( btree, revision, middle );

            // Copy all the element into the left page
            System.arraycopy( getKeys(), 0, leftLeaf.getKeys(), 0, middle );
            System.arraycopy( values, 0, leftLeaf.values, 0, middle );

            // Now, create the right page
            rightLeaf = new InMemoryLeaf<K, V>( btree, revision, middle + 1 );

            int rightPos = pos - middle;

            // Copy the keys and the values up to the insertion position
            System.arraycopy( getKeys(), middle, rightLeaf.getKeys(), 0, rightPos );
            System.arraycopy( values, middle, rightLeaf.values, 0, rightPos );

            // Add the new element
            rightLeaf.setKey( rightPos, new KeyHolder<K>( key ) );
            rightLeaf.values[rightPos] = valueHolder;

            // And copy the remaining elements
            System.arraycopy( getKeys(), pos, rightLeaf.getKeys(), rightPos + 1, nbElems - pos );
            System.arraycopy( values, pos, rightLeaf.values, rightPos + 1, nbElems - pos );
        }

        // Get the pivot
        K pivot = rightLeaf.getKey( 0 );

        // Create the result
        InsertResult<K, V> result = new SplitResult<K, V>( pivot, leftLeaf, rightLeaf );

        return result;
    }


    /**
     * {@inheritDoc}
     */
    public Tuple<K, V> findLeftMost() throws IOException
    {
        V val = null;

        val = values[0].getCursor().next();

        return new Tuple<K, V>( getKey( 0 ), val );
    }


    /**
     * {@inheritDoc}
     */
    public Tuple<K, V> findRightMost() throws EndOfFileExceededException, IOException
    {
        V val = null;

        ValueCursor<V> valueCursor = values[nbElems - 1].getCursor();
        valueCursor.afterLast();
        val = valueCursor.prev();

        return new Tuple<K, V>( getKey( nbElems - 1 ), val );
    }


    /**
     * {@inheritDoc}
     */
    public boolean isLeaf()
    {
        return true;
    }


    /**
     * {@inheritDoc}
     */
    public boolean isNode()
    {
        return false;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Leaf[" );
        sb.append( super.toString() );

        sb.append( "] -> {" );

        if ( nbElems > 0 )
        {
            boolean isFirst = true;

            for ( int i = 0; i < nbElems; i++ )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }

                sb.append( "<" ).append( getKey( i ) ).append( "," ).append( values[i] ).append( ">" );
            }
        }

        sb.append( "}" );

        return sb.toString();
    }


    /**
     * {@inheritDoc}
     */
    public String dumpPage( String tabs )
    {
        StringBuilder sb = new StringBuilder();

        sb.append( tabs );

        if ( nbElems > 0 )
        {
            boolean isFirst = true;

            for ( int i = 0; i < nbElems; i++ )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }

                sb.append( "<" ).append( getKey( i ) ).append( "," ).append( values[i] ).append( ">" );
            }
        }

        sb.append( "\n" );

        return sb.toString();
    }
}
