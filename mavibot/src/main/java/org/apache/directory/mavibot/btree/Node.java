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
import java.util.ArrayList;
import java.util.List;

import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;


/**
 * A MVCC Node. It stores the keys and references to its children page. It does not
 * contain any value; only keys and references on the underlaying pages.
 * 
 * A Node has N keys and N+1 references.
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier */class Node<K, V> extends AbstractPage<K, V>
{
    /**
     * Creates a new Node which will contain only one key, with references to
     * a left and right page. This is a specific constructor used by the btree
     * when the root was full when we added a new value.
     *
     * @param btree the parent BTree
     * @param revision the Node revision
     * @param nbPageElems The number of elements in this Node
     */
    @SuppressWarnings("unchecked")
    Node( BTree<K, V> btree, long revision, int nbPageElems )
    {
        super( btree, revision, nbPageElems );

        // Create the children array
        children = ( PageHolder<K, V>[] ) Array.newInstance( PageHolder.class, nbPageElems + 1 );
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
    Node( BTree<K, V> btree, long revision, K key, Page<K, V> leftPage, Page<K, V> rightPage )
    {
        super( btree, revision, 1 );

        // Create the children array, and store the left and right children
        children = ( PageHolder<K, V>[] ) Array.newInstance( PageHolder.class,
            btree.getPageNbElem() + 1 );

        children[0] = new PageHolder<K, V>( btree, leftPage );
        children[1] = new PageHolder<K, V>( btree, rightPage );

        // Feed the key array
        keys[0] = new KeyHolder<K>( btree.getKeySerializer(), key );
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
    Node( BTree<K, V> btree, long revision, K key, PageHolder<K, V> leftPage, PageHolder<K, V> rightPage )
    {
        super( btree, revision, 1 );

        // Create the children array, and store the left and right children
        children = ( PageHolder<K, V>[] ) Array.newInstance( PageHolder.class,
            btree.getPageNbElem() + 1 );

        children[0] = leftPage;
        children[1] = rightPage;

        // Feed the key array
        keys[0] = new KeyHolder<K>( btree.getKeySerializer(), key );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public InsertResult<K, V> insert( WriteTransaction transaction, K key, V value ) throws IOException
    {
        // Find the key into this leaf
        int pos = findPos( key );

        // Negative position are for Nodes.
        if ( pos < 0 )
        {
            // The key has been found in the page. As it's a Node, that means
            // we must go down in the right child to insert the value
            pos = -( pos + 1 );
        }

        // Get the child page into which we will insert the <K, V> tuple
        Page<K, V> child = children[pos].getValue();

        // and insert the <K, V> into this child
        InsertResult<K, V> result = child.insert( transaction, key, value );

        // Ok, now, we have injected the <K, V> tuple down the tree. Let's check
        // the result to see if we have to split the current page
        if ( result instanceof ExistsResult )
        {
            // Nothing to do
            return result;
        }
        
        if ( result instanceof ModifyResult )
        {
            // The child has been modified, we must modify this node too
            return replaceChild( transaction, ( ModifyResult<K, V> ) result, pos );
        }
        else
        {
            // The child has been split. We have to insert the new pivot in the
            // current page, and to reference the two new pages. This might forces
            // a split if the current Node is already full.
            // We have to deal with the two cases :
            // - the current page is full, we have to split it
            // - the current page is not full, we insert the new pivot
            if ( nbPageElems == btree.getPageNbElem() )
            {
                // The page is full
                result = addAndSplit( transaction, ( SplitResult<K, V> ) result, pos );
            }
            else
            {
                // The page can contain the new pivot, let's insert it
                result = insertChild( transaction, ( SplitResult<K, V> ) result, pos );
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
    private RemoveResult<K, V> handleRemoveResult( WriteTransaction transaction, RemoveResult<K, V> removeResult, 
        int index, int pos, boolean found ) throws IOException
    {
        // Simplest case : the element has been removed from the underlying page,
        // we just have to copy the current page an modify the reference to link to
        // the modified page.
        Node<K, V> newPage = copy( transaction );

        Page<K, V> modifiedPage = removeResult.getModifiedPage();

        if ( found )
        {
            newPage.children[index + 1] = createHolder( modifiedPage );
        }
        else
        {
            newPage.children[index] = createHolder( modifiedPage );
        }

        if ( pos < 0 )
        {
            newPage.keys[index].setKey( removeResult.getModifiedPage().getLeftMostKey() );
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
    private RemoveResult<K, V> handleRootRemove( MergedWithSiblingResult<K, V> mergedResult, int pos )
        throws IOException
    {
        RemoveResult<K, V> removeResult;

        // If the current node contains only one key, then the merged result will be
        // the new root. Deal with this case
        if ( nbPageElems == 1 )
        {
            removeResult = new RemoveResult<>( mergedResult.getCopiedPages(), mergedResult.getModifiedPage(),
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
    private DeleteResult<K, V> borrowFromRight( WriteTransaction transaction, MergedWithSiblingResult<K, V> mergedResult,
        Node<K, V> sibling, int pos ) throws IOException
    {
        // Create the new sibling, with one less element at the beginning
        Node<K, V> newSibling = new Node<>( btree, revision, sibling.nbPageElems - 1 );

        K siblingKey = sibling.children[0].getValue().getLeftMostKey();

        // Copy the keys and children of the old sibling in the new sibling
        System.arraycopy( sibling.keys, 1, newSibling.keys, 0, newSibling.nbPageElems );
        System.arraycopy( sibling.children, 1, newSibling.children, 0, newSibling.nbPageElems + 1 );

        // Create the new page and add the new element at the end
        // First copy the current node, with the same size
        Node<K, V> newNode = new Node<>( btree, revision, nbPageElems );

        // Copy the keys and the values up to the insertion position
        int index = Math.abs( pos );

        // Copy the key and children from sibling
        newNode.keys[nbPageElems - 1] = new KeyHolder<>( btree.getKeySerializer(), siblingKey ); // 1
        newNode.children[nbPageElems] = sibling.children[0]; // 8

        if ( index < 2 )
        {
            // Copy the keys
            System.arraycopy( keys, 1, newNode.keys, 0, nbPageElems - 1 );

            // Inject the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[0] = createHolder( modifiedPage );

            // Copy the children
            System.arraycopy( children, 2, newNode.children, 1, nbPageElems - 1 );
        }
        else
        {
            if ( index > 2 )
            {
                // Copy the keys before the deletion point
                System.arraycopy( keys, 0, newNode.keys, 0, index - 2 ); // 4
            }

            // Inject the new modified page key
            newNode.keys[index - 2] = new KeyHolder<K>( btree.getKeySerializer(), mergedResult
                .getModifiedPage()
                .getLeftMostKey() ); // 2

            if ( index < nbPageElems )
            {
                // Copy the remaining keys after the deletion point
                System.arraycopy( keys, index, newNode.keys, index - 1, nbPageElems - index ); // 3

                // Copy the remaining children after the deletion point
                System.arraycopy( children, index + 1, newNode.children, index, nbPageElems - index ); // 7
            }

            // Copy the children before the deletion point
            System.arraycopy( children, 0, newNode.children, 0, index - 1 ); // 5

            // Inject the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[index - 1] = createHolder( modifiedPage ); // 6
        }

        // Create the result
        DeleteResult<K, V> result = new BorrowedFromRightResult<>( mergedResult.getCopiedPages(), newNode,
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
    private DeleteResult<K, V> borrowFromLeft( WriteTransaction transaction, MergedWithSiblingResult<K, V> mergedResult,
        Node<K, V> sibling, int pos ) throws IOException
    {
        // The sibling is on the left, borrow the rightmost element
        Page<K, V> siblingChild = sibling.children[sibling.nbPageElems].getValue();

        // Create the new sibling, with one less element at the end
        Node<K, V> newSibling = new Node<>( btree, revision, sibling.nbPageElems - 1 );

        // Copy the keys and children of the old sibling in the new sibling
        System.arraycopy( sibling.keys, 0, newSibling.keys, 0, newSibling.nbPageElems );
        System.arraycopy( sibling.children, 0, newSibling.children, 0, newSibling.nbPageElems + 1 );

        // Create the new page and add the new element at the beginning
        // First copy the current node, with the same size
        Node<K, V> newNode = new Node<>( btree, revision, nbPageElems );

        // Sets the first children
        newNode.children[0] = createHolder( siblingChild ); //1

        int index = Math.abs( pos );

        if ( index < 2 )
        {
            newNode.keys[0] = new KeyHolder<K>( btree.getKeySerializer(), mergedResult.getModifiedPage()
                .getLeftMostKey() );
            System.arraycopy( keys, 1, newNode.keys, 1, nbPageElems - 1 );

            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[1] = createHolder( modifiedPage );
            System.arraycopy( children, 2, newNode.children, 2, nbPageElems - 1 );
        }
        else
        {
            // Set the first key
            newNode.keys[0] = new KeyHolder<K>( btree.getKeySerializer(), children[0].getValue()
                .getLeftMostKey() ); //2

            if ( index > 2 )
            {
                // Copy the keys before the deletion point
                System.arraycopy( keys, 0, newNode.keys, 1, index - 2 ); // 4
            }

            // Inject the modified key
            newNode.keys[index - 1] = new KeyHolder<K>( btree.getKeySerializer(), mergedResult
                .getModifiedPage()
                .getLeftMostKey() ); // 3

            if ( index < nbPageElems )
            {
                // Add copy the remaining keys after the deletion point
                System.arraycopy( keys, index, newNode.keys, index, nbPageElems - index ); // 5

                // Copy the remaining children after the insertion point
                System.arraycopy( children, index + 1, newNode.children, index + 1, nbPageElems - index ); // 8
            }

            // Copy the children before the insertion point
            System.arraycopy( children, 0, newNode.children, 1, index - 1 ); // 6

            // Insert the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[index] = createHolder( modifiedPage ); // 7
        }

        // Create the result
        DeleteResult<K, V> result = new BorrowedFromLeftResult<>( mergedResult.getCopiedPages(), newNode,
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
    private DeleteResult<K, V> mergeWithSibling( WriteTransaction transaction, MergedWithSiblingResult<K, V> mergedResult,
        Node<K, V> sibling, boolean isLeft, int pos ) throws IOException
    {
        // Create the new node. It will contain N - 1 elements (the maximum number)
        // as we merge two nodes that contain N/2 elements minus the one we remove
        Node<K, V> newNode = new Node<>( btree, revision, btree.getPageNbElem() );
        Tuple<K, V> removedElement = mergedResult.getRemovedElement();
        int half = btree.getPageNbElem() / 2;
        int index = Math.abs( pos );

        if ( isLeft )
        {
            // The sibling is on the left. Copy all of its elements in the new node first
            System.arraycopy( sibling.keys, 0, newNode.keys, 0, half ); //1
            System.arraycopy( sibling.children, 0, newNode.children, 0, half + 1 ); //2

            // Then copy all the elements up to the deletion point
            if ( index < 2 )
            {
                newNode.keys[half] = new KeyHolder<K>( btree.getKeySerializer(), mergedResult
                    .getModifiedPage()
                    .getLeftMostKey() );
                System.arraycopy( keys, 1, newNode.keys, half + 1, half - 1 );

                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                newNode.children[half + 1] = createHolder( modifiedPage );
                System.arraycopy( children, 2, newNode.children, half + 2, half - 1 );
            }
            else
            {
                // Copy the left part of the node keys up to the deletion point
                // Insert the new key
                newNode.keys[half] = new KeyHolder<K>( btree.getKeySerializer(), children[0].getValue()
                    .getLeftMostKey() ); // 3

                if ( index > 2 )
                {
                    System.arraycopy( keys, 0, newNode.keys, half + 1, index - 2 ); //4
                }

                // Inject the new merged key
                newNode.keys[half + index - 1] = new KeyHolder<K>( btree.getKeySerializer(), mergedResult
                    .getModifiedPage().getLeftMostKey() ); //5

                if ( index < half )
                {
                    System.arraycopy( keys, index, newNode.keys, half + index, half - index ); //6
                    System.arraycopy( children, index + 1, newNode.children, half + index + 1, half - index ); //9
                }

                // Copy the children before the deletion point
                System.arraycopy( children, 0, newNode.children, half + 1, index - 1 ); // 7

                // Inject the new merged child
                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                newNode.children[half + index] = createHolder( modifiedPage ); //8
            }
        }
        else
        {
            // The sibling is on the right.
            if ( index < 2 )
            {
                // Copy the keys
                System.arraycopy( keys, 1, newNode.keys, 0, half - 1 );

                // Insert the first child
                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                newNode.children[0] = createHolder( modifiedPage );

                // Copy the node children
                System.arraycopy( children, 2, newNode.children, 1, half - 1 );
            }
            else
            {
                // Copy the keys and children before the deletion point
                if ( index > 2 )
                {
                    // Copy the first keys
                    System.arraycopy( keys, 0, newNode.keys, 0, index - 2 ); //1
                }

                // Copy the first children
                System.arraycopy( children, 0, newNode.children, 0, index - 1 ); //6

                // Inject the modified key
                newNode.keys[index - 2] = new KeyHolder<K>( btree.getKeySerializer(), mergedResult
                    .getModifiedPage()
                    .getLeftMostKey() ); //2

                // Inject the modified children
                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                newNode.children[index - 1] = createHolder( modifiedPage ); // 7

                // Add the remaining node's key if needed
                if ( index < half )
                {
                    System.arraycopy( keys, index, newNode.keys, index - 1, half - index ); //5

                    // Add the remaining children if below half
                    System.arraycopy( children, index + 1, newNode.children, index, half - index ); // 8
                }
            }

            // Inject the new key from sibling
            newNode.keys[half - 1] = new KeyHolder<>( btree.getKeySerializer(), sibling.findLeftMost()
                .getKey() ); //3

            // Copy the sibling keys
            System.arraycopy( sibling.keys, 0, newNode.keys, half, half );

            // Add the sibling children
            System.arraycopy( sibling.children, 0, newNode.children, half, half + 1 ); // 9
        }

        // And create the result
        DeleteResult<K, V> result = new MergedWithSiblingResult<>( mergedResult.getCopiedPages(), newNode,
            removedElement );

        result.addCopiedPage( this );
        result.addCopiedPage( sibling );

        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    /* no qualifier */ DeleteResult<K, V> delete( WriteTransaction transaction, K key, Page<K, V> parent, int parentPos )
        throws IOException
    {
        // We first try to delete the element from the child it belongs to
        // Find the key in the page
        int pos = findPos( key );
        boolean found = pos < 0;
        int index = pos;
        Page<K, V> child;
        DeleteResult<K, V> deleteResult;

        if ( found )
        {
            index = -( pos + 1 );
            child = children[-pos].getValue();
            deleteResult = ((AbstractPage<K, V>)child).delete( transaction, key, this, -pos );
        }
        else
        {
            child = children[pos].getValue();
            deleteResult = ((AbstractPage<K, V>)child).delete( transaction, key, this, pos );
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
            return handleRemoveResult( transaction, ( RemoveResult<K, V> ) deleteResult, index, pos, found );
        }

        // If we had to borrow an element in the child, then have to update
        // the current page
        if ( deleteResult instanceof BorrowedFromSiblingResult )
        {
            return handleBorrowedResult( transaction, ( BorrowedFromSiblingResult<K, V> ) deleteResult, pos );
        }

        // Last, not least, we have merged two child pages. We now have to remove
        // an element from the local page, and to deal with the result.
        if ( deleteResult instanceof MergedWithSiblingResult )
        {
            MergedWithSiblingResult<K, V> mergedResult = ( MergedWithSiblingResult<K, V> ) deleteResult;

            // If the parent is null, then this page is the root page.
            if ( parent == null )
            {
                return handleRootRemove( mergedResult, pos );
            }

            // We have some parent. Check if the current page is not half full
            int halfSize = btree.getPageNbElem() / 2;

            if ( nbPageElems > halfSize )
            {
                // The page has more than N/2 elements.
                // We simply remove the element from the page, and if it was the leftmost,
                // we return the new pivot (it will replace any instance of the removed
                // key in its parents)
                return removeKey( mergedResult, revision, pos );
            }
            else
            {
                // We will remove one element from a page that will have less than N/2 elements,
                // which will lead to some reorganization : either we can borrow an element from
                // a sibling, or we will have to merge two pages
                int siblingPos = selectSibling( transaction, parent, parentPos );

                Node<K, V> sibling = ( Node<K, V> ) ( ( ( Node<K, V> ) parent ).children[siblingPos]
                    .getValue() );

                if ( sibling.nbPageElems > halfSize )
                {
                    // The sibling contains enough elements
                    // We can borrow the element from the sibling
                    if ( siblingPos < parentPos )
                    {
                        return borrowFromLeft( transaction, mergedResult, sibling, pos );
                    }
                    else
                    {
                        // Borrow from the right
                        return borrowFromRight( transaction, mergedResult, sibling, pos );
                    }
                }
                else
                {
                    // We need to merge the sibling with the current page
                    return mergeWithSibling( transaction, mergedResult, sibling,
                        siblingPos < parentPos, pos );
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
    private RemoveResult<K, V> handleBorrowedResult( WriteTransaction transaction, 
        BorrowedFromSiblingResult<K, V> borrowedResult, int pos ) throws IOException
    {
        Page<K, V> modifiedPage = borrowedResult.getModifiedPage();
        Page<K, V> modifiedSibling = borrowedResult.getModifiedSibling();

        Node<K, V> newPage = copy( transaction );

        if ( pos < 0 )
        {
            int newPos = -( pos + 1 );

            if ( borrowedResult.isFromRight() )
            {
                // Update the keys
                newPage.keys[newPos] = new KeyHolder<K>( btree.getKeySerializer(), modifiedPage.findLeftMost()
                    .getKey() );
                newPage.keys[newPos + 1] = new KeyHolder<K>( btree.getKeySerializer(), modifiedSibling
                    .findLeftMost()
                    .getKey() );

                // Update the children
                newPage.children[newPos + 1] = createHolder( modifiedPage );
                newPage.children[newPos + 2] = createHolder( modifiedSibling );
            }
            else
            {
                // Update the keys
                newPage.keys[newPos] = new KeyHolder<K>( btree.getKeySerializer(), modifiedPage.findLeftMost()
                    .getKey() );

                // Update the children
                newPage.children[newPos] = createHolder( modifiedSibling );
                newPage.children[newPos + 1] = createHolder( modifiedPage );
            }
        }
        else
        {
            if ( borrowedResult.isFromRight() )
            {
                // Update the keys
                newPage.keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), modifiedSibling.findLeftMost()
                    .getKey() );

                // Update the children
                newPage.children[pos] = createHolder( modifiedPage );
                newPage.children[pos + 1] = createHolder( modifiedSibling );
            }
            else
            {
                // Update the keys
                newPage.keys[pos - 1] = new KeyHolder<>( btree.getKeySerializer(), modifiedPage
                    .findLeftMost()
                    .getKey() );

                // Update the children
                newPage.children[pos - 1] = createHolder( modifiedSibling );
                newPage.children[pos] = createHolder( modifiedPage );
            }
        }

        // Modify the result and return
        RemoveResult<K, V> removeResult = new RemoveResult<>( borrowedResult.getCopiedPages(), newPage,
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
        Node<K, V> newNode = new Node<>( btree, revision, nbPageElems - 1 );

        int index = Math.abs( pos ) - 2;

        //
        if ( index < 0 )
        {
            // Copy the keys and the children
            System.arraycopy( keys, 1, newNode.keys, 0, newNode.nbPageElems );
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[0] = createHolder( modifiedPage );
            System.arraycopy( children, 2, newNode.children, 1, nbPageElems - 1 );
        }
        else
        {
            // Copy the keys
            if ( index > 0 )
            {
                System.arraycopy( keys, 0, newNode.keys, 0, index );
            }

            newNode.keys[index] = new KeyHolder<K>( btree.getKeySerializer(), mergedResult.getModifiedPage()
                .findLeftMost().getKey() );

            if ( index < nbPageElems - 2 )
            {
                System.arraycopy( keys, index + 2, newNode.keys, index + 1, nbPageElems - index - 2 );
            }

            // Copy the children
            System.arraycopy( children, 0, newNode.children, 0, index + 1 );

            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            newNode.children[index + 1] = createHolder( modifiedPage );

            if ( index < nbPageElems - 2 )
            {
                System.arraycopy( children, index + 3, newNode.children, index + 2, nbPageElems - index - 2 );
            }
        }

        // Create the result
        RemoveResult<K, V> result = new RemoveResult<>( mergedResult.getCopiedPages(), newNode,
            mergedResult.getRemovedElement() );

        result.addCopiedPage( this );

        return result;
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
     * Set the value at a give position
     *
     * @param pos The position in the values array
     * @param value the value to inject
     */
    /* no qualifier */void setValue( int pos, PageHolder<K, V> value )
    {
        children[pos] = value;
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
    private InsertResult<K, V> replaceChild( WriteTransaction transaction, ModifyResult<K, V> result, int pos ) throws IOException
    {
        // Just copy the current page and update its revision
        Page<K, V> newPage = this;
        
        if ( revision != transaction.getRevision() )
        {
            newPage = copy( transaction );
        }

        // Last, we update the children table of the newly created page
        // to point on the modified child
        Page<K, V> modifiedPage = result.getModifiedPage();

        ( ( Node<K, V> ) newPage ).children[pos] = createHolder( modifiedPage );

        // We can return the result, where we update the modifiedPage,
        // to avoid the creation of a new object
        result.setModifiedPage( newPage );
        
        if ( revision != transaction.getRevision() )
        {
            transaction.addWALObject( newPage );
            
            if ( btree.getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
            {
                transaction.addCopiedWALObject( this );
            }
        }
        else
        {
            // Move the page up in the WAL stack
            transaction.removeWALObject( id );
            transaction.addWALObject( this );
        }

        result.addCopiedPage( this );

        return result;
    }


    /**
     * Creates a new holder containing a reference to a Page
     *
     * @param page The page we will refer to
     * @return A holder containing a reference to the child page
     * @throws IOException If we have an error while trying to access the page
     */
    private PageHolder<K, V> createHolder( Page<K, V> page ) throws IOException
    {
        return new PageHolder<>( btree, page );
    }


    /**
     * Adds a new key into a copy of the current page at a given position. We return the
     * modified page. The new page will have one more key than the current page.
     *
     * @param transaction The {@WriteTransaction} we are running in
     * @param slitResult the result of the child's split
     * @param pos The position into the page
     * @return The modified page with the <K,V> element added
     * @throws IOException If we have an error while trying to access the page
     */
    private InsertResult<K, V> insertChild( WriteTransaction transaction, SplitResult<K, V> splitResult, int pos ) throws IOException
    {
        K pivot = splitResult.getPivot();
        Page<K, V> leftPage = splitResult.getLeftPage();
        Page<K, V> rightPage = splitResult.getRightPage();

        // First copy the current page if needed, but add one element in the copied page
        Node<K, V> newNode = this;
        
        if ( transaction.getRevision() != revision )
        {
            newNode = new Node<>( btree, transaction.getRevision(), nbPageElems + 1 );
            newNode.setId( id );

            // Copy the keys and the children up to the insertion position
            if ( nbPageElems > 0 )
            {
                System.arraycopy( keys, 0, newNode.keys, 0, pos );
                System.arraycopy( children, 0, newNode.children, 0, pos );
            }

            // Add the new key and children
            newNode.keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), pivot );
            newNode.children[pos] = createHolder( leftPage );
            newNode.children[pos + 1] = createHolder( rightPage );

            // And copy the remaining keys and children
            if ( nbPageElems - pos > 0 )
            {
                System.arraycopy( keys, pos, newNode.keys, pos + 1, nbPageElems - pos );
                System.arraycopy( children, pos + 1, newNode.children, pos + 2, children.length - pos - 1 );
            }
        }
        else
        {
            KeyHolder[] newKeys = ( KeyHolder[] ) Array.newInstance( KeyHolder.class, nbPageElems + 1);
            PageHolder[] newChildren = ( PageHolder<K, V>[] ) Array.newInstance( PageHolder.class, nbPageElems + 2 );
            
            // Copy the keys and the children up to the insertion position
            System.arraycopy( keys, 0, newKeys, 0, pos );
            System.arraycopy( children, 0, newChildren, 0, pos );

            // Add the new key and children
            newKeys[pos] = new KeyHolder<K>( btree.getKeySerializer(), pivot );
            newChildren[pos] = createHolder( leftPage );
            newChildren[pos + 1] = createHolder( rightPage );
            
            // And copy the remaining keys and children
            if ( nbPageElems - pos > 0 )
            {
                System.arraycopy( keys, pos, newKeys, pos + 1, nbPageElems - pos );
                System.arraycopy( children, pos + 1, newChildren, pos + 2, children.length - pos - 1 );
            }
            
            keys = newKeys;
            children = newChildren;
        }
        
        nbPageElems++;

        // Create the result
        ModifyResult<K, V> result = new ModifyResult<>( newNode, null );
        
        // Remove the old Node from the transaction
        transaction.removeWALObject( id );
        
        // and add the new Node into the transaction Pages map
        transaction.addWALObject( newNode );

        // And the split node in the CopiedPages B-tree if we are not already processing it
        if ( btree.getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
        {
            transaction.addCopiedWALObject( this );
        }

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
     * @param transaction the on going transaction
     * @param splitResult the result of the child's insertion
     * @param pos The position of the insertion of the new element
     * @return An {@InsertResult} instance containing the pivot, and the new left and right pages
     * @throws IOException If we have an error while trying to access the page
     */
    private InsertResult<K, V> addAndSplit( WriteTransaction transaction, SplitResult<K, V> splitResult, int pos ) throws IOException
    {
        int middle = btree.getPageNbElem() >> 1;
        long revision = transaction.getRevision();
        InsertResult<K, V> result;

        // Create two new pages
        Node<K, V> newLeftPage = new Node<>( btree, revision, middle );
        newLeftPage.initId( transaction.getRecordManagerHeader() );
        
        Node<K, V> newRightPage = new Node<>( btree, revision, middle );
        newRightPage.initId( transaction.getRecordManagerHeader() );

        // Determinate where to store the new value
        // If it's before the middle, insert the value on the left,
        // the key in the middle will become the new pivot
        if ( pos < middle )
        {
            // Copy the keys and the children up to the insertion position
            System.arraycopy( keys, 0, newLeftPage.keys, 0, pos );
            System.arraycopy( children, 0, newLeftPage.children, 0, pos );

            // Add the new element
            newLeftPage.keys[pos] = new KeyHolder<K>( btree.getKeySerializer(), splitResult.getPivot() );
            newLeftPage.children[pos] = createHolder( splitResult.getLeftPage() );
            newLeftPage.children[pos + 1] = createHolder( splitResult.getRightPage() );

            // And copy the remaining elements minus the new pivot
            System.arraycopy( keys, pos, newLeftPage.keys, pos + 1, middle - pos - 1 );
            System.arraycopy( children, pos + 1, newLeftPage.children, pos + 2, middle - pos - 1 );

            // Copy the keys and the children in the right page
            System.arraycopy( keys, middle, newRightPage.keys, 0, middle );
            System.arraycopy( children, middle, newRightPage.children, 0, middle + 1 );

            // Create the result
            K newPivot = keys[middle - 1].getKey();

            if ( newPivot == null )
            {
                newPivot = keys[middle - 1].getKey();
            }

            result = new SplitResult<>( null, newPivot, newLeftPage, newRightPage );
        }
        else if ( pos == middle )
        {
            // A special case : the pivot will be propagated up in the tree
            // The left and right pages will be spread on the two new pages
            // Copy the keys and the children up to the insertion position (here, middle)
            System.arraycopy( keys, 0, newLeftPage.keys, 0, middle );
            System.arraycopy( children, 0, newLeftPage.children, 0, middle );
            newLeftPage.children[middle] = createHolder( splitResult.getLeftPage() );

            // And process the right page now
            System.arraycopy( keys, middle, newRightPage.keys, 0, middle );
            System.arraycopy( children, middle + 1, newRightPage.children, 1, middle );
            newRightPage.children[0] = createHolder( splitResult.getRightPage() );

            // Create the result
            result = new SplitResult<>( null, splitResult.getPivot(), newLeftPage, newRightPage );
        }
        else
        {
            // Copy the keys and the children up to the middle
            System.arraycopy( keys, 0, newLeftPage.keys, 0, middle );
            System.arraycopy( children, 0, newLeftPage.children, 0, middle + 1 );

            // Copy the keys and the children in the right page up to the pos
            System.arraycopy( keys, middle + 1, newRightPage.keys, 0, pos - middle - 1 );
            System.arraycopy( children, middle + 1, newRightPage.children, 0, pos - middle - 1 );

            // Add the new element
            newRightPage.keys[pos - middle - 1] = new KeyHolder<K>( btree.getKeySerializer(), splitResult.getPivot() );
            newRightPage.children[pos - middle - 1] = createHolder( splitResult.getLeftPage() );
            newRightPage.children[pos - middle] = createHolder( splitResult.getRightPage() );

            // And copy the remaining elements minus the new pivot
            System.arraycopy( keys, pos, newRightPage.keys, pos - middle, nbPageElems - pos );
            System.arraycopy( children, pos + 1, newRightPage.children, pos + 1 - middle, nbPageElems - pos );

            // Create the result
            K newPivot = keys[middle].getKey();

            if ( newPivot == null )
            {
                newPivot = keys[middle].getKey();
            }

            result = new SplitResult<>( null, newPivot, newLeftPage, newRightPage );
        }

        // Add the newly created pages i the transaction Pages map
        transaction.addWALObject( newLeftPage );
        transaction.addWALObject( newRightPage );
        
        // Don't forget to remove the previous page from the transaction Pages map
        transaction.removeWALObject( id );

        // And insert this page into the copied pages map, if it's not a CopiedPages B-tree page. 
        if ( btree.getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
        {
            transaction.addCopiedWALObject( this );
        }

        return result;
    }


    /**
     * Copies the current page and all its keys, with a new revision.
     *
     * @param revision The new revision
     * @return The copied page
     */
    @Override
    public Node<K, V> copy( WriteTransaction transaction )
    {
        long revision = transaction.getRevision();
        Node<K, V> newNode = new Node<>( btree, revision, nbPageElems );
        newNode.setId( id );

        // Copy the keys
        System.arraycopy( keys, 0, newNode.keys, 0, nbPageElems );

        // Copy the children
        System.arraycopy( children, 0, newNode.children, 0, nbPageElems + 1 );

        return newNode;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public K getLeftMostKey()
    {
        return children[0].getValue().getLeftMostKey();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public K getRightMostKey()
    {
        int index = ( nbPageElems + 1 ) - 1;

        if ( children[index] != null )
        {
            return children[index].getValue().getRightMostKey();
        }

        return children[nbPageElems - 1].getValue().getRightMostKey();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeaf()
    {
        return false;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNode()
    {
        return true;
    }


    /**
     * Serialize a new Page. It will contain the following data :<br/>
     * <ul>
     * <li>the revision : a long</li>
     * <li>the number of elements : an int (if <= 0, it's a Node, otherwise it's a Leaf)</li>
     * <li>the keys : an array of serialized keys</li>
     * <li>the values : an array of references to the children pageIO offset (stored as long)
     * if it's a Node, or a list of values if it's a Leaf</li>
     * <li></li>
     * </ul>
     * Note that keys and values are stored alternatively :
     * <pre> 
     * V[0], K[0], V[1], K[1], ... V[N], K[N], V[N+1]
     * </pre>
     * 
     * (keep in mind we have one more value than the number of keys).
     * 
     * @param transaction The on-going transaction
     * @return An array of {@PageIO}s containing the serialized node
     * @throws IOException If we had some issue di-uring the serialization
     */
    @Override
    public PageIO[] serialize( WriteTransaction transaction ) throws IOException
    {
        RecordManager recordManager = transaction.getRecordManager();
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();

        // Prepare a list of byte[] that will contain the serialized page
        int nbBuffers = 1 + 1 + 1 + nbPageElems * 2 + 1;
        int dataSize = 0;
        int serializedSize = 0;

        // Now, we can create the list with the right size
        List<byte[]> serializedData = new ArrayList<>( nbBuffers );

        // The page ID
        byte[] buffer = LongSerializer.serialize( id );
        serializedData.add( buffer );
        serializedSize += buffer.length;

        // The revision
        buffer = LongSerializer.serialize( revision );
        serializedData.add( buffer );
        serializedSize += buffer.length;

        // The number of elements
        // Make it a negative value if it's a Node
        buffer = IntSerializer.serialize( -nbPageElems );
        serializedData.add( buffer );
        serializedSize += buffer.length;

        // Iterate on the keys and values. We first serialize the value, then the key
        // until we are done with all of them. If we are serializing a page, we have
        // to serialize one more value
        for ( int pos = 0; pos < nbPageElems; pos++ )
        {
            // Start with the value
            dataSize += serializeNodeValue( pos, serializedData );
            dataSize += serializeNodeKey( pos, serializedData );
        }
        
        // Nodes have one more value to serialize
        dataSize += serializeNodeValue( nbPageElems, serializedData );

        // Store the data size
        buffer = IntSerializer.serialize( dataSize );
        serializedData.add( 3, buffer );
        serializedSize += buffer.length;

        serializedSize += dataSize;

        // We are done. Allocate the pages we need to store the data
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


    /**
     * Serialize a Node's key
     */
    private int serializeNodeKey( int pos, List<byte[]> serializedData )
    {
        KeyHolder<K> holder = getKeyHolder( pos );
        byte[] buffer = holder.getRaw();

        // We have to store the serialized key length
        serializedData.add( buffer );

        return buffer.length;
    }


    /**
     * Serialize a Node's Value. We store the two offsets of the child page.
     */
    private int serializeNodeValue( int pos, List<byte[]> serializedData )
        throws IOException
    {
        // For a node, we just store the children's offsets
        Page<K, V> child = getReference( pos );

        // The first offset
        byte[] buffer = LongSerializer.serialize( ( ( AbstractPage<K, V> ) child ).getOffset() );
        serializedData.add( buffer );

        return buffer.length;
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public String prettyPrint()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "{Node(" ).append( id ).append( ")@" );
        
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
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Node[" );
        sb.append( super.toString() );
        sb.append( "] -> {" );

        if ( nbPageElems > 0 )
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
            
            sb.append( ':' ).append( String.format( "0x%04X", children[0].getOffset() ) );

            for ( int i = 0; i < nbPageElems; i++ )
            {
                sb.append( "|<" ).append( keys[i] ).append( ">|" );

                if ( children[i + 1] == null )
                {
                    sb.append( "null" );
                }
                else
                {
                    sb.append( 'r' ).append( children[i + 1].getValue().getRevision() );
                    sb.append( ':' ).append( String.format( "0x%04X", children[i+1].getOffset() ) );
                }
            }
        }

        sb.append( "}" );

        return sb.toString();
    }
}
