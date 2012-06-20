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
 * A MVCC Node. It stores the keys and references to its children page. It does not
 * contain any value.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class Node<K, V> extends AbstractPage<K, V>
{
    /** Children pages associated with keys. */
    protected Page<K, V>[] children;
    
    
    /**
     * Create a new Node which will contain only one key, with references to
     * a left and right page. This is a specific constructor used by the btree
     * when the root was full when we added a new value.
     * 
     * @param btree the parent BTree
     * @param revision the Node revision
     * @param key The new key
     * @param leftPage The left page
     * @param rightPage The right page
     */
    /* No qualifier */ Node( BTree<K, V> btree, long revision, int nbElems )
    {
        super( btree, revision, nbElems );
        
        // Create the children array
        children = new Page[nbElems + 1];
    }
    
    
    /**
     * Create a new Node which will contain only one key, with references to
     * a left and right page. This is a specific constructor used by the btree
     * when the root was full when we added a new value.
     * 
     * @param btree the parent BTree
     * @param revision the Node revision
     * @param key The new key
     * @param leftPage The left page
     * @param rightPage The right page
     */
    /* No qualifier */ Node( BTree<K, V> btree, long revision, K key, Page<K, V> leftPage, Page<K, V> rightPage )
    {
        super( btree, revision, 1 );
        
        // Create the children array, and store the left and right children
        children = new Page[btree.getPageSize()];
        children[0] = leftPage;
        children[1] = rightPage;
        
        // Create the keys array and store the pivot into it
        keys = (K[])new Object[btree.getPageSize()];
        keys[0] = key;
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
            // The key has been found in the page. As it's a Node, that means
            // we must go down in the right child to insert the value
            pos = - ( pos++ );
        }
            
        // Get the child page into which we will insert the <K, V> tuple
        Page<K, V> child = children[pos];
        
        // and insert the <K, V> into this child
        InsertResult<K, V> result = child.insert( revision, key, value );

        // Ok, now, we have injected the <K, V> tuple down the tree. Let's check
        // the result to see if we have to split the current page
        if ( result instanceof ModifyResult )
        {
            // The child has been modified.
            ModifyResult<K, V> modifyResult = (ModifyResult<K, V>)result;
            
            // Just copy the current page and update its revision
            Page<K, V> newPage = copy( revision );
            
            // Last, we update the children table of the newly created page
            // to point on the modified child
            ((Node<K, V>)newPage).children[pos] = modifyResult.modifiedPage;
            
            // We can return the result, where we update the modifiedPage,
            // to avoid the creation of a new object
            modifyResult.modifiedPage = newPage;
            
            return modifyResult;
        }
        else
        {
            // The child has been split. We have to insert the new pivot in the
            // current page, and to reference the two new pages
            SplitResult<K, V> splitResult = (SplitResult<K, V>)result;
            K pivot = splitResult.getPivot();
            Page<K, V> leftPage = splitResult.getLeftPage();
            Page<K, V> rightPage = splitResult.getRightPage();
            
            // We have to deal with the two cases :
            // - the current page is full, we have to split it
            // - the current page is not full, we insert the new pivot
            if ( nbElems == btree.getPageSize() )
            {
                // The page is full
                result = addAndSplit( revision, pivot, leftPage, rightPage, pos );
            }
            else
            {
                // The page can contain the new pivot, let's insert it
                result = addElement( revision, pivot, leftPage, rightPage, pos );
            }
            
            return result;
        }
    }


    /**
     * {@inheritDoc}
     */
    public V find( K key )
    {
        int pos = findPos( key );
        
        if ( pos < 0 )
        {
            // Here, if we have found the key in the node, then we must go down into
            // the right child, not the left one
            return children[- pos ].find( key );
        }
        else
        {
            return children[pos].find( key );
        }
    }
    
    
    /**
     * Add a new key into a copy of the current page at a given position. We return the
     * modified page. The new page will have one more key than the current page.
     * 
     * @param revision The revision of the modified page
     * @param key The key to insert
     * @param leftPage The left child
     * @param rightPage The right child
     * @param pos The position into the page
     * @return The modified page with the <K,V> element added
     */
    private InsertResult<K, V> addElement( long revision, K key, Page<K, V> leftPage, Page<K, V> rightPage, int pos )
    {
        // First copy the current page, but add one element in the copied page
        Node<K, V> newNode = new Node<K, V>( btree, revision, nbElems + 1 );
        
        // Deal with the special case of an empty page
        if ( nbElems == 0 )
        {
            newNode.keys[0] = key;
            newNode.children[0] = leftPage;
            newNode.children[1] = rightPage;
        }
        else
        {
            // Copy the keys and the children up to the insertion position
            System.arraycopy( keys, 0, newNode.keys, 0, pos );
            System.arraycopy( children, 0, newNode.children, 0, pos );
            
            // Add the new key and children
            newNode.keys[pos] = key;
            newNode.children[pos] = leftPage;
            newNode.children[pos + 1] = rightPage;
            
            // And copy the remaining keys and children
            System.arraycopy( keys, pos, newNode.keys, pos + 1, keys.length - pos );
            System.arraycopy( children, pos + 1, newNode.children, pos + 2, children.length - pos - 1 );
        }
        
        // Create the result
        InsertResult<K, V> result = new ModifyResult<K, V>( newNode, null );
        
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
     * @param leftPage The left child
     * @param rightPage The right child
     * @param pos The position of the insertion of the new element
     * @return An OverflowPage containing the pivot, and the new left and right pages
     */
    private InsertResult<K, V> addAndSplit( long revision, K pivot, Page<K, V> leftPage, Page<K, V> rightPage, int pos )
    {
        int middle = btree.pageSize >> 1;
        
        // Create two new pages
        Node<K, V> newLeftPage = new Node<K, V>( btree, revision, middle );
        Node<K, V> newRightPage = new Node<K, V>( btree, revision, middle );
        
        // Determinate where to store the new value
        // If it's before the middle, insert the value on the left,
        // the key in the middle will become the new pivot
        if ( pos < middle )
        {
            // Copy the keys and the children up to the insertion position
            System.arraycopy( keys, 0, newLeftPage.keys, 0, pos );
            System.arraycopy( children, 0, newLeftPage.children, 0, pos );
            
            // Add the new element
            newLeftPage.keys[pos] = pivot;
            newLeftPage.children[pos] = leftPage;
            newLeftPage.children[pos+1] = rightPage;
            
            // And copy the remaining elements minus the new pivot
            System.arraycopy( keys, pos, newLeftPage.keys, pos + 1, middle - pos - 1 );
            System.arraycopy( children, pos + 1, newLeftPage.children, pos + 2, middle - pos - 1 );

            // Copy the keys and the values in the right page
            System.arraycopy( keys, middle, newRightPage.keys, 0, middle );
            System.arraycopy( children, middle, newRightPage.children, 0, middle + 1 );
            
            // Create the result
            InsertResult<K, V> result = new SplitResult<K, V>( keys[middle - 1], newLeftPage, newRightPage );
            
            return result;
        }
        else if ( pos == middle )
        {
            // A special case : the pivot will be propagated up in the tree
            // The left and right pages will be spread on the two new pages
            // Copy the keys and the children up to the insertion position
            System.arraycopy( keys, 0, newLeftPage.keys, 0, middle );
            System.arraycopy( children, 0, newLeftPage.children, 0, middle );
            newLeftPage.children[middle] = leftPage;
            
            // And process the right page now
            System.arraycopy( keys, middle, newRightPage.keys, 0, middle );
            System.arraycopy( children, middle + 1, newRightPage.children, 1, middle );
            newRightPage.children[0] = rightPage;
            
            // Create the result
            InsertResult<K, V> result = new SplitResult<K, V>( pivot, newLeftPage, newRightPage );
            
            return result;
        }
        else
        {
            // Copy the keys and the children up to the middle
            System.arraycopy( keys, 0, newLeftPage.keys, 0, middle );
            System.arraycopy( children, 0, newLeftPage.children, 0, middle + 1 );
            
            // Copy the keys and the values in the right page up to the pos
            System.arraycopy( keys, middle + 1, newRightPage.keys, 0, pos - middle - 1 );
            System.arraycopy( children, middle + 1, newRightPage.children, 0, pos - middle - 1 );
            
            // Add the new element
            newRightPage.keys[pos - middle - 1] = pivot;
            newRightPage.children[pos - middle - 1] = leftPage;
            newRightPage.children[pos - middle] = rightPage;
            
            // And copy the remaining elements minus the new pivot
            System.arraycopy( keys, pos, newRightPage.keys, pos - middle, nbElems - pos );
            System.arraycopy( children, pos + 1, newRightPage.children, pos + 1 - middle, nbElems - pos );

            // Create the result
            InsertResult<K, V> result = new SplitResult<K, V>( keys[middle], newLeftPage, newRightPage );
            
            return result;
        }
    }
    
    
    /**
     * Copy the current page and all its keys, with a new revision.
     * 
     * @param revision The new revision
     * @return The copied page
     */
    protected Page<K, V> copy( long revision )
    {
        Page<K, V> newPage = new Node<K, V>( btree, revision, nbElems );

        // Copy the keys
        System.arraycopy( keys, 0, ((Node<K, V>)newPage).keys, 0, nbElems );

        // Copy the children
        System.arraycopy( children, 0, ((Node<K, V>)newPage).children, 0, nbElems + 1);

        return newPage;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "Node[" );
        sb.append( super.toString() );
        sb.append ( "] -> {" );
        
        if ( nbElems > 0 )
        {
            // Start with the first child
            sb.append( children[0].getId() ).append( "-r" ).append( children[0].getRevision() );
            sb.append( "{" ).append( children[0] ).append( "}" );
            
            for ( int i = 0; i < nbElems; i++ )
            {
                sb.append( "|<" ).append( keys[i] ).append( ">|" ).
                    append( children[i + 1].getId() ).append( "-r" ).append( children[i + 1].getRevision() );
                sb.append( "{" ).append( children[i + 1] ).append( "}" );
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
        
        if ( nbElems > 0 )
        {
            // Start with the first child
            sb.append( children[0].dumpPage( tabs + "    " ) );
            
            for ( int i = 0; i < nbElems; i++ )
            {
                sb.append( tabs );
                sb.append ( "<" );
                sb.append( keys[i] ).append( ">\n" );
                sb.append( children[i + 1].dumpPage( tabs + "    " ) );
            }
        }
        
        return sb.toString();
    }
}
