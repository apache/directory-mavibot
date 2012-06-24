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
    
    /** The page that comes next to this one */
    protected Leaf<K, V> nextPage;
    
    /** The page that comes previous to this one */
    protected Leaf<K, V> prevPage;
    
    
    /**
     * Empty constructor
     */
    public Leaf( BTree<K, V> btree )
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
            InsertResult<K, V> result = addElement( revision, key, value, pos );
            
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
    public DeleteResult<K, V> delete( long revision, K key, Page<K, V> parent )
    {
        // Find the key in the page
        int pos = findPos( key );

        // If the parent is null, then this page is the root page.
        if ( parent == null )
        {
            // Just remove the entry if it's present
            if ( pos < 0 )
            {
                DeleteResult<K, V> result = removeElementFromRoot( revision, -( pos + 1 ) );
                
                return result;
            }
            else
            {
                // Not found : retur the not present result.
                return NotPresentResult.NOT_PRESENT;
            }
        }
        else
        {
            // The current page is not the root
            return null;
        }
    }
    
    
    /**
     * Remove the element at a given position. The
     * 
     * @param revision The revision of the modified page
     * @param key The key to insert
     * @param value The value to insert
     * @param pos The position into the page
     * @return The modified page with the <K,V> element added
     */
    private DeleteResult<K, V> removeElementFromRoot( long revision,int pos )
    {
        // First copy the current page, but remove one element in the copied page
        Leaf<K, V> newRoot = new Leaf<K, V>( btree, revision, nbElems - 1 );
        
        // Get the removed element
        Tuple<K, V> removedElement = new Tuple<K, V>();
        removedElement.setKey( keys[pos] );
        removedElement.setValue( values[pos] );
        
        // Deal with the special case of an page with only one element by skipping
        // the copy, as we won't have any remaining  element in the page
        if ( nbElems > 1 )
        {
            // Copy the keys and the values up to the insertion position
            System.arraycopy( keys, 0, newRoot.keys, 0, pos );
            System.arraycopy( values, 0, newRoot.values, 0, pos );
            
            // And copy the elements after the position
            System.arraycopy( keys, pos + 1, newRoot.keys, pos, keys.length - pos  - 1 );
            System.arraycopy( values, pos + 1, newRoot.values, pos, values.length - pos - 1 );
        }
        
        // Create the result
        DeleteResult<K, V> result = new RemoveResult<K, V>( newRoot, removedElement );
        
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
    public Cursor<K, V> browse( K key, Transaction<K, V> transaction )
    {
        int pos = findPos( key );
        Cursor<K, V> cursor = null;
        
        if ( pos < 0 )
        {
            // The first element has been found. Create the cursor
            cursor = new Cursor<K, V>( transaction, this, - ( pos + 1 ) );
        }
        else
        {
            // The key has not been found. Select the value just above, if we have one
            if ( pos < nbElems )
            {
                cursor = new Cursor<K, V>( transaction, this, pos );
            }
            else
            {
                if ( nextPage != null )
                {
                    cursor = new Cursor<K, V>( transaction, nextPage, 0 );
                }
                else
                {
                    cursor = new Cursor<K, V>( transaction, null, -1 );
                }
            }
        }
        
        return cursor;
    }
    
    
    /**
     * {@inheritDoc}
     */
    public Cursor<K, V> browse( Transaction<K, V> transaction )
    {
        int pos = 0;
        Cursor<K, V> cursor = null;
        
        if ( nbElems == 0 )
        {
            // The tree is empty, we have nothing to return
            return new Cursor<K, V>( transaction, null, -1 );
        }
        else
        {
            // Start at the beginning of the page
            cursor = new Cursor<K, V>( transaction, this, pos );
        }
        
        return cursor;
    }
    
    
    /**
     * Copy the current page and all of the keys, values and children, if it's not a leaf.
     * 
     * @param revision The new revision
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
     * @param pos The position of the key in the page
     * @param value the new value
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
        
        // and update the prev/next references
        newLeaf.prevPage = this.prevPage;
        newLeaf.nextPage = this.nextPage;
        
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
    private InsertResult<K, V> addElement( long revision, K key, V value, int pos )
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
        
        // Update the prev/next references
        newLeaf.prevPage = this.prevPage;
        newLeaf.nextPage = this.nextPage;

        // Create the result
        InsertResult<K, V> result = new ModifyResult<K, V>( newLeaf, null );
        
        return result;
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
        
        // and update the prev/next references
        leftLeaf.prevPage = this.prevPage;
        leftLeaf.nextPage = rightLeaf;
        rightLeaf.prevPage = leftLeaf;
        rightLeaf.nextPage = this.nextPage;
        
        // Get the pivot
        K pivot = rightLeaf.keys[0];
        
        // Prepare the result
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
        sb.append( ", prev:" );
        
        if ( prevPage == null )
        {
            sb.append( "null" );
        }
        else
        {
            sb.append( prevPage.revision );
        }
        
        sb.append( ", next:" );
        
        if ( nextPage == null )
        {
            sb.append( "null" );
        }
        else
        {
            sb.append( nextPage.revision );
        }

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
