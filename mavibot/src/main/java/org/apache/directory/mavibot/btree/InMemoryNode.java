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
import java.util.List;


/**
 * A MVCC Node. It stores the keys and references to its children page. It does not
 * contain any value.
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier */class InMemoryNode<K, V> extends AbstractPage<K, V>
{
    /**
     * Creates a new Node which will contain only one key, with references to
     * a left and right page. This is a specific constructor used by the btree
     * when the root was full when we added a new value.
     *
     * @param btree the parent BTree
     * @param revision the Node revision
     * @param nbElems The number of elements in this Node
     */
    @SuppressWarnings("unchecked")
    InMemoryNode( BTree<K, V> btree, long revision, int nbElems )
    {
        super( btree, revision, nbElems );

        // Create the children array
        children = ( PageHolder<K, V>[] ) Array.newInstance( PageHolder.class, nbElems + 1 );
    }


    /**
     * Creates a new Node which will contain only one key, with references to
     * a left and right page. This is a specific constructor used by the btree
     * when the root was full when we added a new value.
     *
     * @param btree the parent BTree
     * @param revision the Node revision
     * @param key The new key
     * @param leftPage The left page
     * @param rightPage The right page
     */
    @SuppressWarnings("unchecked")
    InMemoryNode( BTree<K, V> btree, long revision, K key, Page<K, V> leftPage, Page<K, V> rightPage )
    {
        super( btree, revision, 1 );

        // Create the children array, and store the left and right children
        children = ( PageHolder[] ) Array.newInstance( PageHolder.class, btree.getPageSize() + 1 );

        children[0] = new PageHolder<K, V>( btree, leftPage );
        children[1] = new PageHolder<K, V>( btree, rightPage );

        // Create the keys array and store the pivot into it
        // We get the type of array to create from the btree
        // Yes, this is an hack...
        setKeys( ( KeyHolder<K>[] ) Array.newInstance( KeyHolder.class, btree.getPageSize() ) );

        setKey( 0, new KeyHolder<K>( key ) );
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
            // The key has been found in the page. As it's a Node, that means
            // we must go down in the right child to insert the value
            pos = -( pos++ );
        }

        // Get the child page into which we will insert the <K, V> tuple
        Page<K, V> child = children[pos].getValue();

        // and insert the <K, V> into this child
        InsertResult<K, V> result = child.insert( key, value, revision );

        // Ok, now, we have injected the <K, V> tuple down the tree. Let's check
        // the result to see if we have to split the current page
        if ( result instanceof ModifyResult )
        {
            // The child has been modified.
            return replaceChild( revision, ( ModifyResult<K, V> ) result, pos );
        }
        else
        {
            // The child has been split. We have to insert the new pivot in the
            // current page, and to reference the two new pages
            SplitResult<K, V> splitResult = ( SplitResult<K, V> ) result;
            K pivot = splitResult.getPivot();
            Page<K, V> leftPage = splitResult.getLeftPage();
            Page<K, V> rightPage = splitResult.getRightPage();

            // We have to deal with the two cases :
            // - the current page is full, we have to split it
            // - the current page is not full, we insert the new pivot
            if ( nbElems == btree.getPageSize() )
            {
                // The page is full
                result = addAndSplit( splitResult.getCopiedPages(), revision, pivot, leftPage, rightPage, pos );
            }
            else
            {
                // The page can contain the new pivot, let's insert it
                result = insertChild( splitResult.getCopiedPages(), revision, pivot, leftPage, rightPage, pos );
            }

            return result;
        }
    }


    /**
     * Modifies the current node after a remove has been done in one of its children.
     * The node won't be merged with another node.
     *
     * @param removeResult The result of a remove operation
     * @param index the position of the key, not transformed
     * @param pos The position of the key, as a positive value
     * @param found If the key has been found in the page
     * @return The new result
     * @throws IOException If we have an error while trying to access the page
     */
    private RemoveResult<K, V> handleRemoveResult( RemoveResult<K, V> removeResult, int index, int pos, boolean found )
        throws IOException
    {
        // Simplest case : the element has been removed from the underlying page,
        // we just have to copy the current page an modify the reference to link to
        // the modified page.
        InMemoryNode<K, V> newPage = copy( revision );

        Page<K, V> modifiedPage = removeResult.getModifiedPage();

        if ( found )
        {
            newPage.children[index + 1] = new PageHolder<K, V>( btree, modifiedPage );
        }
        else
        {
            newPage.children[index] = new PageHolder<K, V>( btree, modifiedPage );
        }

        if ( pos < 0 )
        {
            newPage.setKey( index, new KeyHolder<K>( removeResult.getModifiedPage().getLeftMostKey() ) );
        }

        // Modify the result and return
        removeResult.setModifiedPage( newPage );
        removeResult.addCopiedPage( this );

        return removeResult;
    }


    /**
     * Handles the removal of an element from the root page, when two of its children
     * have been merged.
     *
     * @param mergedResult The merge result
     * @param pos The position in the current root
     * @param found Tells if the removed key is present in the root page
     * @return The resulting root page
     * @throws IOException If we have an error while trying to access the page
     */
    private RemoveResult<K, V> handleRootRemove( MergedWithSiblingResult<K, V> mergedResult, int pos, boolean found )
        throws IOException
    {
        RemoveResult<K, V> removeResult = null;

        // If the current node contains only one key, then the merged result will be
        // the new root. Deal with this case
        if ( nbElems == 1 )
        {
            removeResult = new RemoveResult<K, V>( mergedResult.getCopiedPages(), mergedResult.getModifiedPage(),
                mergedResult.getRemovedElement() );

            removeResult.addCopiedPage( this );
        }
        else
        {
            // Remove the element and update the reference to the changed pages
            removeResult = removeKey( mergedResult, revision, pos );
        }

        return removeResult;
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
    private DeleteResult<K, V> borrowFromRight( long revision, MergedWithSiblingResult<K, V> mergedResult,
        InMemoryNode<K, V> sibling, int pos ) throws IOException
    {
        // Create the new sibling, with one less element at the beginning
        InMemoryNode<K, V> newSibling = new InMemoryNode<K, V>( btree, revision, sibling.getNbElems() - 1 );

        K siblingKey = sibling.children[0].getValue().getLeftMostKey();

        // Copy the keys and children of the old sibling in the new sibling
        System.arraycopy( sibling.getKeys(), 1, newSibling.getKeys(), 0, newSibling.getNbElems() );
        System.arraycopy( sibling.children, 1, newSibling.children, 0, newSibling.getNbElems() + 1 );

        // Create the new page and add the new element at the end
        // First copy the current node, with the same size
        InMemoryNode<K, V> newNode = new InMemoryNode<K, V>( btree, revision, nbElems );

        // Copy the keys and the values up to the insertion position
        int index = Math.abs( pos );

        // Copy the key and children from sibling
        newNode.setKey( nbElems - 1, new KeyHolder<K>( siblingKey ) ); // 1
        newNode.children[nbElems] = sibling.children[0]; // 8

        if ( index < 2 )
        {
            // Copy the keys
            System.arraycopy( getKeys(), 1, newNode.getKeys(), 0, nbElems - 1 );

            // Inject the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[0] = new PageHolder<K, V>( btree, modifiedPage );

            // Copy the children
            System.arraycopy( children, 2, newNode.children, 1, nbElems - 1 );
        }
        else
        {
            if ( index > 2 )
            {
                // Copy the keys before the deletion point
                System.arraycopy( getKeys(), 0, newNode.getKeys(), 0, index - 2 ); // 4
            }

            // Inject the new modified page key
            newNode.setKey( index - 2, new KeyHolder<K>( mergedResult.getModifiedPage().getLeftMostKey() ) ); // 2

            if ( index < nbElems )
            {
                // Copy the remaining keys after the deletion point
                System.arraycopy( getKeys(), index, newNode.getKeys(), index - 1, nbElems - index ); // 3

                // Copy the remaining children after the deletion point
                System.arraycopy( children, index + 1, newNode.children, index, nbElems - index ); // 7
            }

            // Copy the children before the deletion point
            System.arraycopy( children, 0, newNode.children, 0, index - 1 ); // 5

            // Inject the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[index - 1] = new PageHolder<K, V>( btree, modifiedPage ); // 6
        }

        // Create the result
        DeleteResult<K, V> result = new BorrowedFromRightResult<K, V>( mergedResult.getCopiedPages(), newNode,
            newSibling, mergedResult.getRemovedElement() );

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
    private DeleteResult<K, V> borrowFromLeft( long revision, MergedWithSiblingResult<K, V> mergedResult,
        InMemoryNode<K, V> sibling, int pos ) throws IOException
    {
        // The sibling is on the left, borrow the rightmost element
        Page<K, V> siblingChild = sibling.children[sibling.nbElems].getValue();

        // Create the new sibling, with one less element at the end
        InMemoryNode<K, V> newSibling = new InMemoryNode<K, V>( btree, revision, sibling.getNbElems() - 1 );

        // Copy the keys and children of the old sibling in the new sibling
        System.arraycopy( sibling.getKeys(), 0, newSibling.getKeys(), 0, newSibling.getNbElems() );
        System.arraycopy( sibling.children, 0, newSibling.children, 0, newSibling.getNbElems() + 1 );

        // Create the new page and add the new element at the beginning
        // First copy the current node, with the same size
        InMemoryNode<K, V> newNode = new InMemoryNode<K, V>( btree, revision, nbElems );

        // Sets the first children
        newNode.children[0] = new PageHolder<K, V>( btree, siblingChild ); //1

        int index = Math.abs( pos );

        if ( index < 2 )
        {
            newNode.setKey( 0, new KeyHolder<K>( mergedResult.getModifiedPage().getLeftMostKey() ) );
            System.arraycopy( getKeys(), 1, newNode.getKeys(), 1, nbElems - 1 );

            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[1] = new PageHolder<K, V>( btree, modifiedPage );
            System.arraycopy( children, 2, newNode.children, 2, nbElems - 1 );
        }
        else
        {
            // Set the first key
            newNode.setKey( 0, new KeyHolder<K>( children[0].getValue().getLeftMostKey() ) ); //2

            if ( index > 2 )
            {
                // Copy the keys before the deletion point
                System.arraycopy( getKeys(), 0, newNode.getKeys(), 1, index - 2 ); // 4
            }

            // Inject the modified key
            newNode.setKey( index - 1, new KeyHolder<K>( mergedResult.getModifiedPage().getLeftMostKey() ) ); // 3

            if ( index < nbElems )
            {
                // Add copy the remaining keys after the deletion point
                System.arraycopy( getKeys(), index, newNode.getKeys(), index, nbElems - index ); // 5

                // Copy the remaining children after the insertion point
                System.arraycopy( children, index + 1, newNode.children, index + 1, nbElems - index ); // 8
            }

            // Copy the children before the insertion point
            System.arraycopy( children, 0, newNode.children, 1, index - 1 ); // 6

            // Insert the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[index] = new PageHolder<K, V>( btree, modifiedPage ); // 7
        }

        // Create the result
        DeleteResult<K, V> result = new BorrowedFromLeftResult<K, V>( mergedResult.getCopiedPages(), newNode,
            newSibling,
            mergedResult.getRemovedElement() );

        result.addCopiedPage( this );
        result.addCopiedPage( sibling );

        return result;
    }


    /**
     * We have to merge the node with its sibling, both have N/2 elements before the element
     * removal.
     *
     * @param revision The revision
     * @param mergedResult The result of the merge
     * @param sibling The Page we will merge the current page with
     * @param isLeft Tells if the sibling is on the left
     * @param pos The position of the key that has been removed
     * @return The page resulting of the merge
     * @throws IOException If we have an error while trying to access the page
     */
    private DeleteResult<K, V> mergeWithSibling( long revision, MergedWithSiblingResult<K, V> mergedResult,
        InMemoryNode<K, V> sibling, boolean isLeft, int pos ) throws IOException
    {
        // Create the new node. It will contain N - 1 elements (the maximum number)
        // as we merge two nodes that contain N/2 elements minus the one we remove
        InMemoryNode<K, V> newNode = new InMemoryNode<K, V>( btree, revision, btree.getPageSize() );
        Tuple<K, V> removedElement = mergedResult.getRemovedElement();
        int half = btree.getPageSize() / 2;
        int index = Math.abs( pos );

        if ( isLeft )
        {
            // The sibling is on the left. Copy all of its elements in the new node first
            System.arraycopy( sibling.getKeys(), 0, newNode.getKeys(), 0, half ); //1
            System.arraycopy( sibling.children, 0, newNode.children, 0, half + 1 ); //2

            // Then copy all the elements up to the deletion point
            if ( index < 2 )
            {
                newNode.setKey( half, new KeyHolder<K>( mergedResult.getModifiedPage().getLeftMostKey() ) );
                System.arraycopy( getKeys(), 1, newNode.getKeys(), half + 1, half - 1 );

                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                newNode.children[half + 1] = new PageHolder<K, V>( btree, modifiedPage );
                System.arraycopy( children, 2, newNode.children, half + 2, half - 1 );
            }
            else
            {
                // Copy the left part of the node keys up to the deletion point
                // Insert the new key
                newNode.setKey( half, new KeyHolder<K>( children[0].getValue().getLeftMostKey() ) ); // 3

                if ( index > 2 )
                {
                    System.arraycopy( getKeys(), 0, newNode.getKeys(), half + 1, index - 2 ); //4
                }

                // Inject the new merged key
                newNode.setKey( half + index - 1, new KeyHolder<K>( mergedResult.getModifiedPage().getLeftMostKey() ) ); //5

                if ( index < half )
                {
                    System.arraycopy( getKeys(), index, newNode.getKeys(), half + index, half - index ); //6
                    System.arraycopy( children, index + 1, newNode.children, half + index + 1, half - index ); //9
                }

                // Copy the children before the deletion point
                System.arraycopy( children, 0, newNode.children, half + 1, index - 1 ); // 7

                // Inject the new merged child
                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                newNode.children[half + index] = new PageHolder<K, V>( btree, modifiedPage ); //8
            }
        }
        else
        {
            // The sibling is on the right.
            if ( index < 2 )
            {
                // Copy the keys
                System.arraycopy( getKeys(), 1, newNode.getKeys(), 0, half - 1 );

                // Insert the first child
                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                newNode.children[0] = new PageHolder<K, V>( btree, modifiedPage );

                // Copy the node children
                System.arraycopy( children, 2, newNode.children, 1, half - 1 );
            }
            else
            {
                // Copy the keys and children before the deletion point
                if ( index > 2 )
                {
                    // Copy the first keys
                    System.arraycopy( getKeys(), 0, newNode.getKeys(), 0, index - 2 ); //1
                }

                // Copy the first children
                System.arraycopy( children, 0, newNode.children, 0, index - 1 ); //6

                // Inject the modified key
                newNode.setKey( index - 2, new KeyHolder<K>( mergedResult.getModifiedPage().getLeftMostKey() ) ); //2

                // Inject the modified children
                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                newNode.children[index - 1] = new PageHolder<K, V>( btree, modifiedPage ); // 7

                // Add the remaining node's key if needed
                if ( index < half )
                {
                    System.arraycopy( getKeys(), index, newNode.getKeys(), index - 1, half - index ); //5

                    // Add the remining children if below half
                    System.arraycopy( children, index + 1, newNode.children, index, half - index ); // 8
                }
            }

            // Inject the new key from sibling
            newNode.setKey( half - 1, new KeyHolder<K>( sibling.findLeftMost().getKey() ) ); //3

            // Copy the sibling keys
            System.arraycopy( sibling.getKeys(), 0, newNode.getKeys(), half, half );

            // Add the sibling children
            System.arraycopy( sibling.children, 0, newNode.children, half, half + 1 ); // 9
        }

        // And create the result
        DeleteResult<K, V> result = new MergedWithSiblingResult<K, V>( mergedResult.getCopiedPages(), newNode,
            removedElement );

        result.addCopiedPage( this );
        result.addCopiedPage( sibling );

        return result;
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */ DeleteResult<K, V> delete( K key, V value, long revision, Page<K, V> parent, int parentPos )
        throws IOException
    {
        // We first try to delete the element from the child it belongs to
        // Find the key in the page
        int pos = findPos( key );
        boolean found = pos < 0;
        int index = pos;
        Page<K, V> child = null;
        DeleteResult<K, V> deleteResult = null;

        if ( found )
        {
            index = -( pos + 1 );
            child = children[-pos].getValue();
            deleteResult = ((AbstractPage<K, V>)child).delete( key, value, revision, this, -pos );
        }
        else
        {
            child = children[pos].getValue();
            deleteResult = ((AbstractPage<K, V>)child).delete( key, value, revision, this, pos );
        }

        // If the key is not present in the tree, we simply return
        if ( deleteResult instanceof NotPresentResult )
        {
            // Nothing to do...
            return deleteResult;
        }

        // If we just modified the child, return a modified page
        if ( deleteResult instanceof RemoveResult )
        {
            RemoveResult<K, V> removeResult = handleRemoveResult( ( RemoveResult<K, V> ) deleteResult, index, pos,
                found );

            return removeResult;
        }

        // If we had to borrow an element in the child, then have to update
        // the current page
        if ( deleteResult instanceof BorrowedFromSiblingResult )
        {
            RemoveResult<K, V> removeResult = handleBorrowedResult( ( BorrowedFromSiblingResult<K, V> ) deleteResult,
                pos );

            return removeResult;
        }

        // Last, not least, we have merged two child pages. We now have to remove
        // an element from the local page, and to deal with the result.
        if ( deleteResult instanceof MergedWithSiblingResult )
        {
            MergedWithSiblingResult<K, V> mergedResult = ( MergedWithSiblingResult<K, V> ) deleteResult;

            // If the parent is null, then this page is the root page.
            if ( parent == null )
            {
                RemoveResult<K, V> result = handleRootRemove( mergedResult, pos, found );

                return result;
            }

            // We have some parent. Check if the current page is not half full
            int halfSize = btree.getPageSize() / 2;

            if ( nbElems > halfSize )
            {
                // The page has more than N/2 elements.
                // We simply remove the element from the page, and if it was the leftmost,
                // we return the new pivot (it will replace any instance of the removed
                // key in its parents)
                RemoveResult<K, V> result = removeKey( mergedResult, revision, pos );

                return result;
            }
            else
            {
                // We will remove one element from a page that will have less than N/2 elements,
                // which will lead to some reorganization : either we can borrow an element from
                // a sibling, or we will have to merge two pages
                int siblingPos = selectSibling( parent, parentPos );

                InMemoryNode<K, V> sibling = ( InMemoryNode<K, V> ) ( ( ( InMemoryNode<K, V> ) parent ).children[siblingPos]
                    .getValue() );

                if ( sibling.getNbElems() > halfSize )
                {
                    // The sibling contains enough elements
                    // We can borrow the element from the sibling
                    if ( siblingPos < parentPos )
                    {
                        DeleteResult<K, V> result = borrowFromLeft( revision, mergedResult, sibling, pos );

                        return result;
                    }
                    else
                    {
                        // Borrow from the right
                        DeleteResult<K, V> result = borrowFromRight( revision, mergedResult, sibling, pos );

                        return result;
                    }
                }
                else
                {
                    // We need to merge the sibling with the current page
                    DeleteResult<K, V> result = mergeWithSibling( revision, mergedResult, sibling,
                        ( siblingPos < parentPos ), pos );

                    return result;
                }
            }
        }

        // We should never reach this point
        return null;
    }


    /**
     * The deletion in a children has moved an element from one of its sibling. The key
     * is present in the current node.
     * @param borrowedResult The result of the deletion from the children
     * @param pos The position the key was found in the current node
     * @return The result
     * @throws IOException If we have an error while trying to access the page
     */
    private RemoveResult<K, V> handleBorrowedResult( BorrowedFromSiblingResult<K, V> borrowedResult, int pos )
        throws IOException
    {
        Page<K, V> modifiedPage = borrowedResult.getModifiedPage();
        Page<K, V> modifiedSibling = borrowedResult.getModifiedSibling();

        InMemoryNode<K, V> newPage = copy( revision );

        if ( pos < 0 )
        {
            pos = -( pos + 1 );

            if ( borrowedResult.isFromRight() )
            {
                // Update the keys
                newPage.setKey( pos, new KeyHolder<K>( modifiedPage.findLeftMost().getKey() ) );
                newPage.setKey( pos + 1, new KeyHolder<K>( modifiedSibling.findLeftMost().getKey() ) );

                // Update the children
                newPage.children[pos + 1] = new PageHolder<K, V>( btree, modifiedPage );
                newPage.children[pos + 2] = new PageHolder<K, V>( btree, modifiedSibling );
            }
            else
            {
                // Update the keys
                newPage.setKey( pos, new KeyHolder<K>( modifiedPage.findLeftMost().getKey() ) );

                // Update the children
                newPage.children[pos] = new PageHolder<K, V>( btree, modifiedSibling );
                newPage.children[pos + 1] = new PageHolder<K, V>( btree, modifiedPage );
            }
        }
        else
        {
            if ( borrowedResult.isFromRight() )
            {
                // Update the keys
                newPage.setKey( pos, new KeyHolder<K>( modifiedSibling.findLeftMost().getKey() ) );

                // Update the children
                newPage.children[pos] = new PageHolder<K, V>( btree, modifiedPage );
                newPage.children[pos + 1] = new PageHolder<K, V>( btree, modifiedSibling );
            }
            else
            {
                // Update the keys
                newPage.setKey( pos - 1, new KeyHolder<K>( modifiedPage.findLeftMost().getKey() ) );

                // Update the children
                newPage.children[pos - 1] = new PageHolder<K, V>( btree, modifiedSibling );
                newPage.children[pos] = new PageHolder<K, V>( btree, modifiedPage );
            }
        }

        // Modify the result and return
        RemoveResult<K, V> removeResult = new RemoveResult<K, V>( borrowedResult.getCopiedPages(), newPage,
            borrowedResult.getRemovedElement() );

        removeResult.addCopiedPage( this );

        return removeResult;
    }


    /**
     * Remove the key at a given position.
     *
     * @param mergedResult The page we will remove a key from
     * @param revision The revision of the modified page
     * @param pos The position into the page of the element to remove
     * @return The modified page with the <K,V> element added
     * @throws IOException If we have an error while trying to access the page
     */
    private RemoveResult<K, V> removeKey( MergedWithSiblingResult<K, V> mergedResult, long revision, int pos )
        throws IOException
    {
        // First copy the current page, but remove one element in the copied page
        InMemoryNode<K, V> newNode = new InMemoryNode<K, V>( btree, revision, nbElems - 1 );

        int index = Math.abs( pos ) - 2;

        //
        if ( index < 0 )
        {
            // Copy the keys and the children
            System.arraycopy( getKeys(), 1, newNode.getKeys(), 0, newNode.nbElems );
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[0] = new PageHolder<K, V>( btree, modifiedPage );
            System.arraycopy( children, 2, newNode.children, 1, nbElems - 1 );
        }
        else
        {
            // Copy the keys
            if ( index > 0 )
            {
                System.arraycopy( getKeys(), 0, newNode.getKeys(), 0, index );
            }

            newNode.setKey( index, new KeyHolder<K>( mergedResult.getModifiedPage().findLeftMost().getKey() ) );

            if ( index < nbElems - 2 )
            {
                System.arraycopy( getKeys(), index + 2, newNode.getKeys(), index + 1, nbElems - index - 2 );
            }

            // Copy the children
            System.arraycopy( children, 0, newNode.children, 0, index + 1 );

            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[index + 1] = new PageHolder<K, V>( btree, modifiedPage );

            if ( index < nbElems - 2 )
            {
                System.arraycopy( children, index + 3, newNode.children, index + 2, nbElems - index - 2 );
            }
        }

        // Create the result
        RemoveResult<K, V> result = new RemoveResult<K, V>( mergedResult.getCopiedPages(), newNode,
            mergedResult.getRemovedElement() );

        result.addCopiedPage( this );

        return result;
    }


    /**
     * Set the value at a give position
     *
     * @param pos The position in the values array
     * @param value the value to inject
     */
    /* no qualifier */void setValue( int pos, Page<K, V> value )
    {
        children[pos] = new PageHolder<K, V>( btree, value );
    }


    /**
     * This method is used when we have to replace a child in a page when we have
     * found the key in the tree (the value will be changed, so we have made
     * copies of the existing pages).
     *
     * @param revision The current revision
     * @param result The modified page
     * @param pos The position of the found key
     * @return A modified page
     * @throws IOException If we have an error while trying to access the page
     */
    private InsertResult<K, V> replaceChild( long revision, ModifyResult<K, V> result, int pos ) throws IOException
    {
        // Just copy the current page and update its revision
        Page<K, V> newPage = copy( revision );

        // Last, we update the children table of the newly created page
        // to point on the modified child
        Page<K, V> modifiedPage = result.getModifiedPage();

        ( ( InMemoryNode<K, V> ) newPage ).children[pos] = new PageHolder<K, V>( btree, modifiedPage );

        // We can return the result, where we update the modifiedPage,
        // to avoid the creation of a new object
        result.setModifiedPage( newPage );

        result.addCopiedPage( this );

        return result;
    }


    /**
     * Adds a new key into a copy of the current page at a given position. We return the
     * modified page. The new page will have one more key than the current page.
     *
     * @param copiedPages the list of copied pages
     * @param revision The revision of the modified page
     * @param key The key to insert
     * @param leftPage The left child
     * @param rightPage The right child
     * @param pos The position into the page
     * @return The modified page with the <K,V> element added
     * @throws IOException If we have an error while trying to access the page
     */
    private InsertResult<K, V> insertChild( List<Page<K, V>> copiedPages, long revision, K key, Page<K, V> leftPage,
        Page<K, V> rightPage, int pos )
        throws IOException
    {
        // First copy the current page, but add one element in the copied page
        InMemoryNode<K, V> newNode = new InMemoryNode<K, V>( btree, revision, nbElems + 1 );

        // Copy the keys and the children up to the insertion position
        if ( nbElems > 0 )
        {
            System.arraycopy( getKeys(), 0, newNode.getKeys(), 0, pos );
            System.arraycopy( children, 0, newNode.children, 0, pos );
        }

        // Add the new key and children
        newNode.setKey( pos, new KeyHolder<K>( key ) );

        // If the BTree is managed, we now have to write the modified page on disk
        // and to add this page to the list of modified pages
        newNode.children[pos] = new PageHolder<K, V>( btree, leftPage );
        newNode.children[pos + 1] = new PageHolder<K, V>( btree, rightPage );

        // And copy the remaining keys and children
        if ( nbElems > 0 )
        {
            System.arraycopy( getKeys(), pos, newNode.getKeys(), pos + 1, getKeys().length - pos );
            System.arraycopy( children, pos + 1, newNode.children, pos + 2, children.length - pos - 1 );
        }

        // Create the result
        InsertResult<K, V> result = new ModifyResult<K, V>( copiedPages, newNode, null );
        result.addCopiedPage( this );

        return result;
    }


    /**
     * Splits a full page into two new pages, a left, a right and a pivot element. The new pages will
     * each contains half of the original elements. <br/>
     * The pivot will be computed, depending on the place
     * we will inject the newly added element. <br/>
     * If the newly added element is in the middle, we will use it
     * as a pivot. Otherwise, we will use either the last element in the left page if the element is added
     * on the left, or the first element in the right page if it's added on the right.
     *
     * @param copiedPages the list of copied pages
     * @param revision The new revision for all the created pages
     * @param pivot The key that will be move up after the split
     * @param leftPage The left child
     * @param rightPage The right child
     * @param pos The position of the insertion of the new element
     * @return An OverflowPage containing the pivot, and the new left and right pages
     * @throws IOException If we have an error while trying to access the page
     */
    private InsertResult<K, V> addAndSplit( List<Page<K, V>> copiedPages, long revision, K pivot, Page<K, V> leftPage,
        Page<K, V> rightPage, int pos ) throws IOException
    {
        int middle = btree.getPageSize() >> 1;

        // Create two new pages
        InMemoryNode<K, V> newLeftPage = new InMemoryNode<K, V>( btree, revision, middle );
        InMemoryNode<K, V> newRightPage = new InMemoryNode<K, V>( btree, revision, middle );

        // Determinate where to store the new value
        // If it's before the middle, insert the value on the left,
        // the key in the middle will become the new pivot
        if ( pos < middle )
        {
            // Copy the keys and the children up to the insertion position
            System.arraycopy( getKeys(), 0, newLeftPage.getKeys(), 0, pos );
            System.arraycopy( children, 0, newLeftPage.children, 0, pos );

            // Add the new element
            newLeftPage.setKey( pos, new KeyHolder<K>( pivot ) );
            newLeftPage.children[pos] = new PageHolder<K, V>( btree, leftPage );
            newLeftPage.children[pos + 1] = new PageHolder<K, V>( btree, rightPage );

            // And copy the remaining elements minus the new pivot
            System.arraycopy( getKeys(), pos, newLeftPage.getKeys(), pos + 1, middle - pos - 1 );
            System.arraycopy( children, pos + 1, newLeftPage.children, pos + 2, middle - pos - 1 );

            // Copy the keys and the children in the right page
            System.arraycopy( getKeys(), middle, newRightPage.getKeys(), 0, middle );
            System.arraycopy( children, middle, newRightPage.children, 0, middle + 1 );

            // Create the result
            InsertResult<K, V> result = new SplitResult<K, V>( copiedPages, getKey( middle - 1 ), newLeftPage,
                newRightPage );
            result.addCopiedPage( this );

            return result;
        }
        else if ( pos == middle )
        {
            // A special case : the pivot will be propagated up in the tree
            // The left and right pages will be spread on the two new pages
            // Copy the keys and the children up to the insertion position (here, middle)
            System.arraycopy( getKeys(), 0, newLeftPage.getKeys(), 0, middle );
            System.arraycopy( children, 0, newLeftPage.children, 0, middle );
            newLeftPage.children[middle] = new PageHolder<K, V>( btree, leftPage );

            // And process the right page now
            System.arraycopy( getKeys(), middle, newRightPage.getKeys(), 0, middle );
            System.arraycopy( children, middle + 1, newRightPage.children, 1, middle );
            newRightPage.children[0] = new PageHolder<K, V>( btree, rightPage );

            // Create the result
            InsertResult<K, V> result = new SplitResult<K, V>( copiedPages, pivot, newLeftPage, newRightPage );
            result.addCopiedPage( this );

            return result;
        }
        else
        {
            // Copy the keys and the children up to the middle
            System.arraycopy( getKeys(), 0, newLeftPage.getKeys(), 0, middle );
            System.arraycopy( children, 0, newLeftPage.children, 0, middle + 1 );

            // Copy the keys and the children in the right page up to the pos
            System.arraycopy( getKeys(), middle + 1, newRightPage.getKeys(), 0, pos - middle - 1 );
            System.arraycopy( children, middle + 1, newRightPage.children, 0, pos - middle - 1 );

            // Add the new element
            newRightPage.setKey( pos - middle - 1, new KeyHolder<K>( pivot ) );
            newRightPage.children[pos - middle - 1] = new PageHolder<K, V>( btree, leftPage );
            newRightPage.children[pos - middle] = new PageHolder<K, V>( btree, rightPage );

            // And copy the remaining elements minus the new pivot
            System.arraycopy( getKeys(), pos, newRightPage.getKeys(), pos - middle, nbElems - pos );
            System.arraycopy( children, pos + 1, newRightPage.children, pos + 1 - middle, nbElems - pos );

            // Create the result
            InsertResult<K, V> result = new SplitResult<K, V>( copiedPages, getKey( middle ), newLeftPage, newRightPage );
            result.addCopiedPage( this );

            return result;
        }
    }


    /**
     * Copies the current page and all its keys, with a new revision.
     *
     * @param revision The new revision
     * @return The copied page
     */
    protected InMemoryNode<K, V> copy( long revision )
    {
        InMemoryNode<K, V> newPage = new InMemoryNode<K, V>( btree, revision, nbElems );

        // Copy the keys
        System.arraycopy( getKeys(), 0, newPage.getKeys(), 0, nbElems );

        // Copy the children
        System.arraycopy( children, 0, newPage.children, 0, nbElems + 1 );

        return newPage;
    }


    /**
     * {@inheritDoc}
     */
    public K getLeftMostKey()
    {
        return children[0].getValue().getLeftMostKey();
    }


    /**
     * {@inheritDoc}
     */
    public K getRightMostKey()
    {
        int index = ( nbElems + 1 ) - 1;

        if ( children[index] != null )
        {
            return children[index].getValue().getRightMostKey();
        }

        return children[nbElems - 1].getValue().getRightMostKey();
    }


    /**
     * {@inheritDoc}
     */
    public boolean isLeaf()
    {
        return false;
    }


    /**
     * {@inheritDoc}
     */
    public boolean isNode()
    {
        return true;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Node[" );
        sb.append( super.toString() );
        sb.append( "] -> {" );

        if ( nbElems > 0 )
        {
            // Start with the first child
            if ( children[0] == null )
            {
                sb.append( "null" );
            }
            else
            {
                sb.append( 'r' ).append( children[0].getValue().getRevision() );
            }

            for ( int i = 0; i < nbElems; i++ )
            {
                sb.append( "|<" ).append( getKey( i ) ).append( ">|" );

                if ( children[i + 1] == null )
                {
                    sb.append( "null" );
                }
                else
                {
                    sb.append( 'r' ).append( children[i + 1].getValue().getRevision() );
                }
            }
        }

        sb.append( "}" );

        return sb.toString();
    }
}
