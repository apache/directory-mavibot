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
    private V[] values;
    
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
        int index = findKeyInPage( key );

        if ( index < 0 )
        {
            // We already have the key in the page : replace the value
            // into a copy of this page, unless the page has already be copied
            index = - ( index++ );
            
            // Replace the existing value in a copy of the current page
            InsertResult<K, V> result = replaceValue( revision, index, value );
            
            return result;
        }
        
        // The key is not present in the leaf. We have to add it in the page
        if ( nbElems < btree.pageSize )
        {
            // The current page is not full, it can contain the added element.
            // We insert it into a copied page and return the result
            InsertResult<K, V> result = addElement( revision, index, key, value );
            
            return result;
        }
        else
        {
            // The Page is already full : we split it and return the overflow element,
            // after having created two pages.
            InsertResult<K, V> result = addAndSplit( revision, key, value, null, null, index );
            
            return result;
        }
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
     * @param index The position of the key in the page
     * @param value the new value
     * @return The copied page
     */
    private InsertResult<K, V> replaceValue( long revision, int index, V value )
    {
        Leaf<K, V> newPage = this;
        
        if ( this.revision != revision )
        {
            // The page hasn't been modified yet, we need to copy it first
            newPage = (Leaf<K, V>)copy( revision, nbElems );
        }
        
        // Now we can inject the value and get back the previous one
        V oldValue = newPage.values[index];
        newPage.values[index] = value;
        
        // Create the result
        InsertResult<K, V> result = new ModifyResult<K, V>( newPage, oldValue );
        
        return result;
    }
    
    
    /**
     * Add a new <K, V> into a copy of the current page at a given position. We return the
     * modified page. The new page will have one more element than the current page.
     * 
     * @param revision The revision of the modified page
     * @param index The position into the page
     * @param key The key to insert
     * @param value The value to insert
     * @return The modified page with the <K,V> element added
     */
    private InsertResult<K, V> addElement( long revision, int index, K key, V value )
    {
        return null;
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
     * @param index The position of the insertion of the new element
     * @param key The key to add
     * @param value The value to add
     * @param left The left child of the added element
     * @param right The right child of the added element
     * @return An OverflowPage containing the pivor, and the new left and right pages
     */
    private InsertResult<K, V> addAndSplit( long revision, K key, V value, AbstractPage<K, V> left, AbstractPage<K, V> right, int index )
    {
        return null;
    }
}
