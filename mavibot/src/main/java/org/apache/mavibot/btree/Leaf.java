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

import java.util.LinkedList;

/**
 * A MVCC Leaf. It stores the keys and values. It does not have any children.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class Leaf<K, V> extends AbstractPage<K, V>
{
    /** Values associated with keys */
    protected V[] values;
    
    
    /**
     * Empty constructor
     */
    /* No qualifier */ Leaf( BTree<K, V> btree )
    {
        super( btree );
    }
    
    
    /**
     * Internal constructor used to create Page instance used when a page is being copied or overflow
     */
    @SuppressWarnings("unchecked") // Cannot create an array of generic objects
    private Leaf( BTree<K, V> btree, long revision, int nbElems )
    {
        super( btree, revision, nbElems );

        this.values = (V[])new Object[nbElems];
    }
    
    
    /**
     * {@inheritDoc}
     */
    public InsertResult<K, V> insert( long revision, K key, V value )
    {
        // Find the key into this leaf
        int pos = findPos( key );

        if ( pos < 0 )
        {
            // We already have the key in the page : replace the value
            // into a copy of this page, unless the page has already be copied
            int index = - ( pos + 1 );
            
            // Replace the existing value in a copy of the current page
            InsertResult<K, V> result = replaceElement( revision, key, value, index );
            
            return result;
        }
        
        // The key is not present in the leaf. We have to add it in the page
        if ( nbElems < btree.pageSize )
        {
            // The current page is not full, it can contain the added element.
            // We insert it into a copied page and return the result
            Page<K, V> modifiedPage = addElement( revision, key, value, pos );
            
            InsertResult<K, V> result = new ModifyResult<K, V>( modifiedPage, null );
                
            return result;
        }
        else
        {
            // The Page is already full : we split it and return the overflow element,
            // after having created two pages.
            InsertResult<K, V> result = addAndSplit( revision, key, value, pos );
            
            return result;
        }
    }
    
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public DeleteResult<K, V> delete( long revision, K key, Page<K, V> parent, int parentPos )
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
        
        int index = -( pos + 1 );

        // If the parent is null, then this page is the root page.
        if ( parent == null )
        {
            // Just remove the entry if it's present
            DeleteResult<K, V> result = removeElement( revision, index );
            
            return result;
        }
        else
        {
            // The current page is not the root. Check if the leaf has more than N/2
            // elements
            int halfSize = btree.pageSize/2;
            
            if ( nbElems == halfSize )
            {
                // We have to find a sibling now, and either borrow an entry from it
                // if it has more than N/2 elements, or to merge the two pages.
                // Check in both next and previous page, if they have the same parent
                // and select the biggest page with the same parent to borrow an element.
                int siblingPos = selectSibling( (Node<K, V>)parent, parentPos );
                
                Leaf<K, V> sibling = (Leaf<K, V>)((Node<K, V>)parent).children[siblingPos];
                
                if ( sibling.getNbElems() == halfSize )
                {
                    // We will merge the current page with its sibling
                    DeleteResult<K, V> result = mergeWithSibling( revision, sibling, ( siblingPos < index), index );
                    
                    return result;
                }
                else
                {
                    // We can borrow the element from the sibling
                    if ( siblingPos < parentPos )
                    {
                        DeleteResult<K, V> result = borrowFromLeft( revision, sibling, index );
                        
                        return result;
                    }
                    else
                    {
                        // Borrow from the right
                        DeleteResult<K, V> result = borrowFromRight( revision, sibling, index );
                        
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
                DeleteResult<K, V> result = removeElement( revision, index );
                
                return result;
            }
        }
    }
    
    
    /**
     * Merge the sibling with the current leaf, after having removed the element in the page.
     * 
     * @param revision The new revision
     * @param sibling The sibling we will merge with
     * @param isLeft Tells if the sibling is on the left or on the right
     * @param pos The position of the removed element
     * @return The new created leaf containing the sibling and the old page.
     */
    private DeleteResult<K, V> mergeWithSibling( long revision, Leaf<K, V> sibling, boolean isLeft, int pos )
    {
        // Create the new page. It will contain N - 1 elements (the maximum number)
        // as we merge two pages that contain N/2 elements minus the one we remove
        Leaf<K, V> newLeaf = new Leaf<K, V>( btree, revision, btree.pageSize - 1 );
        Tuple<K, V> removedElement = new Tuple<K, V>( keys[pos], values[pos] );
        
        if ( isLeft )
        {
            // The sibling is on the left
            // Copy all the elements from the sibling first
            System.arraycopy( sibling.keys, 0, newLeaf.keys, 0, sibling.nbElems );
            System.arraycopy( sibling.values, 0, newLeaf.values, 0, sibling.nbElems );

            // Copy all the elements from the page up to the deletion position
            System.arraycopy( keys, 0, newLeaf.keys, sibling.nbElems, pos );
            System.arraycopy( values, 0, newLeaf.values, sibling.nbElems, pos );
            
            // And copy the remaining elements after the deletion point
            System.arraycopy( keys, pos + 1, newLeaf.keys, sibling.nbElems + pos, nbElems - pos - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, sibling.nbElems + pos, nbElems - pos - 1 );
        }
        else
        {
            // The sibling is on the right
            // Copy all the elements from the page up to the deletion position
            System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
            System.arraycopy( values, 0, newLeaf.values, 0, pos );

            // Then copy the remaining elements after the deletion point
            System.arraycopy( keys, pos + 1, newLeaf.keys, pos, nbElems - pos - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, pos, nbElems - pos - 1 );

            // And copy all the elements from the sibling
            System.arraycopy( sibling.keys, 0, newLeaf.keys, nbElems - 1, sibling.nbElems );
            System.arraycopy( sibling.values, 0, newLeaf.values, nbElems - 1, sibling.nbElems );
        }

        // And create the result
        DeleteResult<K, V> result = new MergedWithSiblingResult<K, V>( newLeaf, removedElement, newLeaf.keys[0] );
        
        return result;
    }
    
    
    /**
     * Borrow an element from the left sibling, creating a new sibling with one
     * less element and creating a new page where the element to remove has been
     * deleted and the borrowed element added on the left.
     * 
     * @param revision The new revision for all the pages
     * @param sibling The left sibling
     * @param pos The position of the element to remove
     * @return The resulting pages
     */
    private DeleteResult<K, V> borrowFromLeft( long revision, Leaf<K, V> sibling, int pos )
    {
        // The sibling is on the left, borrow the rightmost element
        K siblingKey = sibling.keys[sibling.getNbElems() - 1];
        V siblingValue = sibling.values[sibling.getNbElems() - 1 ];
        
        // Create the new sibling, with one less element at the end
        Leaf<K, V> newSibling = (Leaf<K, V>)sibling.copy( revision, sibling.getNbElems() - 1 );

        // Create the new page and add the new element at the beginning
        // First copy the current page, with the same size
        Leaf<K, V> newLeaf = new Leaf<K, V>( btree, revision, nbElems );
        
        // Insert the borrowed element
        newLeaf.keys[0] = siblingKey;
        newLeaf.values[0] = siblingValue;
        
        // Copy the keys and the values up to the insertion position,
        System.arraycopy( keys, 0, newLeaf.keys, 1, pos );
        System.arraycopy( values, 0, newLeaf.values, 1, pos );
        
        // And copy the remaining elements
        System.arraycopy( keys, pos + 1, newLeaf.keys, pos + 1, keys.length - pos - 1 );
        System.arraycopy( values, pos + 1, newLeaf.values, pos + 1, values.length - pos - 1 );
        
        // Create the result
        Tuple<K, V> removedElement = new Tuple<K, V>( keys[pos], values[pos] );

        DeleteResult<K, V> result = new BorrowedFromLeftResult<K, V>( newLeaf, newSibling, removedElement, siblingKey );
        
        return result;
    }
    
    
    /**
     * Borrow an element from the right sibling, creating a new sibling with one
     * less element and creating a new page where the element to remove has been
     * deleted and the borrowed element added on the right.
     * 
     * @param revision The new revision for all the pages
     * @param sibling The right sibling
     * @param pos The position of the element to remove
     * @return The resulting pages
     */
    private DeleteResult<K, V> borrowFromRight( long revision, Leaf<K, V> sibling, int pos )
    {
        // The sibling is on the left, borrow the rightmost element
        K siblingKey = sibling.keys[0];
        V siblingValue = sibling.values[0];
        
        // Create the new sibling
        Leaf<K, V> newSibling = new Leaf<K, V>( btree, revision, sibling.getNbElems() - 1 );

        // Copy the keys and the values from 1 to N in the new sibling
        System.arraycopy( sibling.keys, 1, newSibling.keys, 0, sibling.nbElems - 1 );
        System.arraycopy( sibling.values, 1, newSibling.values, 0, sibling.nbElems - 1 );

        // Create the new page and add the new element at the end
        // First copy the current page, with the same size
        Leaf<K, V> newLeaf = new Leaf<K, V>( btree, revision, nbElems );
        
        // Insert the borrowed element at the end
        newLeaf.keys[nbElems - 1] = siblingKey;
        newLeaf.values[nbElems - 1] = siblingValue;
        
        // Copy the keys and the values up to the deletion position,
        System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
        System.arraycopy( values, 0, newLeaf.values, 0, pos );
        
        // And copy the remaining elements
        System.arraycopy( keys, pos + 1, newLeaf.keys, pos, keys.length - pos - 1 );
        System.arraycopy( values, pos + 1, newLeaf.values, pos, values.length - pos - 1 );
        
        // Create the result
        Tuple<K, V> removedElement = new Tuple<K, V>( keys[pos], values[pos] );

        DeleteResult<K, V> result = new BorrowedFromRightResult<K, V>( newLeaf, newSibling, removedElement, newSibling.keys[0] );
        
        return result;
    }
    

    /**
     * Select the sibling (the prev or next page with the same parent) which has
     * the more element assuming it's above N/2
     * 
     * @param parent The parent of the current page
     * @param The position of the current page reference in its parent
     * @return The position of the sibling, or -1 if we hav'nt found any sibling
     */
    private int selectSibling( Node<K, V> parent, int parentPos )
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
            return -1;
        }
        
        Page<K, V> prevPage = parent.children[parentPos - 1];
        Page<K, V> nextPage = parent.children[parentPos + 1];

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
     * Remove the element at a given position. The
     * 
     * @param revision The revision of the modified page
     * @param pos The position into the page of the element to remove
     * @return The modified page with the <K,V> element added
     */
    private DeleteResult<K, V> removeElement( long revision,int pos )
    {
        // First copy the current page, but remove one element in the copied page
        Leaf<K, V> newLeaf = new Leaf<K, V>( btree, revision, nbElems - 1 );
        
        // Get the removed element
        Tuple<K, V> removedElement = new Tuple<K, V>( keys[pos], values[pos] );
        
        K newLeftMost = null;
        
        // Deal with the special case of an page with only one element by skipping
        // the copy, as we won't have any remaining  element in the page
        if ( nbElems > 1 )
        {
            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
            System.arraycopy( values, 0, newLeaf.values, 0, pos );
            
            // And copy the elements after the position
            System.arraycopy( keys, pos + 1, newLeaf.keys, pos, keys.length - pos  - 1 );
            System.arraycopy( values, pos + 1, newLeaf.values, pos, values.length - pos - 1 );
            
            if ( pos == 0 )
            {
                newLeftMost = newLeaf.keys[0];
            }
        }
        
        // Create the result
        DeleteResult<K, V> result = new RemoveResult<K, V>( newLeaf, removedElement, newLeftMost );
        
        return result;
    }


    /**
     * {@inheritDoc}
     */
    public V find( K key )
    {
        int pos = findPos( key );
        
        if ( pos < 0 )
        {
            return values[- ( pos + 1 ) ];
        }
        else
        {
            return null;
        }
    }
    
    
    /**
     * {@inheritDoc}
     */
    public Cursor<K, V> browse( K key, Transaction<K, V> transaction, LinkedList<ParentPos<K, V>> stack )
    {
        int pos = findPos( key );
        Cursor<K, V> cursor = null;
        
        if ( pos < 0 )
        {
            int index = - ( pos + 1 );
            
            // The first element has been found. Create the cursor
            stack.push( new ParentPos<K, V>( this, index ) );

            cursor = new Cursor<K, V>( transaction, stack );
        }
        else
        {
            // The key has not been found. Select the value just above, if we have one
            if ( pos < nbElems )
            {
                stack.push( new ParentPos<K, V>( this, pos ) );

                cursor = new Cursor<K, V>( transaction, stack );
            }
            else
            {
                // Not found : return a null cursor
                stack.push( new ParentPos<K, V>( this, -1 ) );
                
                return new Cursor<K, V>( transaction, stack );
            }
        }
        
        return cursor;
    }
    
    
    /**
     * {@inheritDoc}
     */
    public Cursor<K, V> browse( Transaction<K, V> transaction, LinkedList<ParentPos<K, V>> stack  )
    {
        int pos = 0;
        Cursor<K, V> cursor = null;
        
        if ( nbElems == 0 )
        {
            // The tree is empty, it's the root, we have nothing to return
            stack.push( new ParentPos<K, V>( null, -1 ) );
            
            return new Cursor<K, V>( transaction, stack );
        }
        else
        {
            // Start at the beginning of the page
            stack.push( new ParentPos<K, V>( this, pos ) );
            
            cursor = new Cursor<K, V>( transaction, stack );
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
        Leaf<K, V> newLeaf = new Leaf<K, V>( btree, revision, nbElems );

        // Copy the keys and the values
        System.arraycopy( keys, 0, newLeaf.keys, 0, nbElems );
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
     */
    private InsertResult<K, V> replaceElement( long revision, K key, V value, int pos )
    {
        Leaf<K, V> newLeaf = this;
        
        if ( this.revision != revision )
        {
            // The page hasn't been modified yet, we need to copy it first
            newLeaf = (Leaf<K, V>)copy( revision, nbElems );
        }
        
        // Now we can inject the value
        V oldValue = newLeaf.values[pos];
        newLeaf.values[pos] = value;
        
        // Create the result
        InsertResult<K, V> result = new ModifyResult<K, V>( newLeaf, oldValue );
        
        return result;
    }
    
    
    /**
     * Add a new <K, V> into a copy of the current page at a given position. We return the
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
        Leaf<K, V> newLeaf = new Leaf<K, V>( btree, revision, nbElems + 1 );
        
        // Deal with the special case of an empty page
        if ( nbElems == 0 )
        {
            newLeaf.keys[0] = key;
            newLeaf.values[0] = value;
        }
        else
        {
            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, 0, newLeaf.keys, 0, pos );
            System.arraycopy( values, 0, newLeaf.values, 0, pos );
            
            // Add the new element
            newLeaf.keys[pos] = key;
            newLeaf.values[pos] = value;
            
            // And copy the remaining elements
            System.arraycopy( keys, pos, newLeaf.keys, pos + 1, keys.length - pos );
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
        int middle = btree.pageSize >> 1;
        Leaf<K, V> leftLeaf = null;
        Leaf<K, V> rightLeaf = null;
        
        // Determinate where to store the new value
        if ( pos <= middle )
        {
            // The left page will contain the new value
            leftLeaf = new Leaf<K, V>( btree, revision, middle + 1 );

            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, 0, leftLeaf.keys, 0, pos );
            System.arraycopy( values, 0, leftLeaf.values, 0, pos );
            
            // Add the new element
            leftLeaf.keys[pos] = key;
            leftLeaf.values[pos] = value;
            
            // And copy the remaining elements
            System.arraycopy( keys, pos, leftLeaf.keys, pos + 1, middle - pos );
            System.arraycopy( values, pos, leftLeaf.values, pos + 1, middle - pos );

            // Now, create the right page
            rightLeaf = new Leaf<K, V>( btree, revision, middle );

            // Copy the keys and the values in the right page
            System.arraycopy( keys, middle, rightLeaf.keys, 0, middle );
            System.arraycopy( values, middle, rightLeaf.values, 0, middle );
        }
        else
        {
            // Create the left page
            leftLeaf = new Leaf<K, V>( btree, revision, middle );

            // Copy all the element into the left page
            System.arraycopy( keys, 0, leftLeaf.keys, 0, middle );
            System.arraycopy( values, 0, leftLeaf.values, 0, middle );

            // Now, create the right page
            rightLeaf = new Leaf<K, V>( btree, revision, middle + 1 );
            
            int rightPos = pos - middle;

            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, middle, rightLeaf.keys, 0, rightPos );
            System.arraycopy( values, middle, rightLeaf.values, 0, rightPos );
            
            // Add the new element
            rightLeaf.keys[rightPos] = key;
            rightLeaf.values[rightPos] = value;
            
            // And copy the remaining elements
            System.arraycopy( keys, pos, rightLeaf.keys, rightPos + 1, nbElems - pos );
            System.arraycopy( values, pos, rightLeaf.values, rightPos + 1, nbElems -pos );
        }
        
        // Get the pivot
        K pivot = rightLeaf.keys[0];
        
        // Create the result
        InsertResult<K, V> result = new SplitResult<K, V>( pivot, leftLeaf, rightLeaf );
        
        return result;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "Leaf[" );
        sb.append( super.toString() );

        sb.append ( "] -> {" );
        
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
                
                sb.append( "<" ).append( keys[i] ).append( "," ).append( values[i] ).append( ">" );
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
                
                sb.append( "<" ).append( keys[i] ).append( "," ).append( values[i] ).append( ">" );
            }
        }
        
        sb.append( "\n" );
        
        return sb.toString();
    }
}
