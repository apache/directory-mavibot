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
        
        // Create the children array, and store the left and right children
        children = (Page<K, V>[])new Object[btree.getPageSize()];
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
        children = (Page<K, V>[])new Object[btree.getPageSize()];
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
        int index = findPos( key );

        if ( index < 0 )
        {
            // The key has been found in the page. As it's a Node, that means
            // we must go down in the right child to insert the value
            index = - ( index++ );
        }
            
        // Get the child page into which we will insert the <K, V> tuple
        Page<K, V> child = children[index];
        
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
            ((Node<K, V>)newPage).children[index] = modifyResult.modifiedPage;
            
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
            }
            else
            {
                // The page can contain the new pivot
            }
            
            return null;
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

        // Copy the children
        System.arraycopy( children, 0, ((Node<K, V>)newPage).children, 0, nbElems );

        return newPage;
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
                
                sb.append( "<" ).append( keys[i] ).append( ",r" ).append( children[i].getRevision() ).append( ">" );
            }
        }
        
        sb.append( "}" );
        
        return sb.toString();
    }
}
