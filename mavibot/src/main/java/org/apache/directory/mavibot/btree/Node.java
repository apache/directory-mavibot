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
    /** Children pages associated with keys. */
    protected long[] children;

    /**
     * Creates a new Node which will contain only one key, with references to
     * a left and right page. This is a specific constructor used by the btree
     * when the root was full when we added a new value.
     *
     * @param btree the parent BTree
     * @param revision the Node revision
     * @param pageNbElems The number of elements in this Node
     */
    Node( BTreeInfo<K, V> btreeInfo, long revision, int pageNbElems )
    {
        super( btreeInfo, revision, pageNbElems );

        // Create the children array
        children = ( long[] ) Array.newInstance( long.class, pageNbElems + 1 );
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
    Node( BTreeInfo<K, V> btreeInfo, long revision, K key, long leftPage, long rightPage )
    {
        super( btreeInfo, revision, 1 );

        // Create the children array, and store the left and right children
        children = new long[btreeInfo.getPageNbElem() + 1];

        children[0] = leftPage;
        children[1] = rightPage;

        // Feed the key array
        keys[0] = new KeyHolder<K>( btreeInfo.getKeySerializer(), key );
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
        Page<K, V> child = transaction.getPage( btreeInfo, children[pos] );

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
            if ( pageNbElems == btreeInfo.getPageNbElem() )
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
     */
    private RemoveResult<K, V> handleRemoveResult( WriteTransaction transaction, RemoveResult<K, V> removeResult, 
        int index, int pos, boolean found ) throws IOException
    {
        // Simplest case : the element has been removed from the underlying page,
        // we just have to copy the current page an modify the reference to link to
        // the modified page.
        Node<K, V> newNode = copy( transaction );

        Page<K, V> modifiedPage = removeResult.getModifiedPage();

        if ( found )
        {
            setChild( newNode, index + 1, modifiedPage );
        }
        else
        {
            setChild( newNode, index, modifiedPage );
        }

        if ( pos < 0 )
        {
            newNode.keys[index].setKey( removeResult.getModifiedPage().getLeftMostKey( transaction ) );
        }

        // Modify the result and return
        removeResult.setModifiedPage( newNode );
        
        // Store the new page in the WAL
        transaction.removeWALObject( id );
        transaction.updateWAL( revision, this, newNode );

        return removeResult;
    }


    /**
     * Handles the removal of an element from the root page, when two of its children
     * have been merged.
     */
    private RemoveResult<K, V> handleRootRemove( WriteTransaction transaction, MergedWithSiblingResult<K, V> mergedResult, int pos )
        throws IOException
    {
        RemoveResult<K, V> removeResult;

        // If the current node contains only one key, then the merged result will be
        // the new root. Deal with this case
        if ( pageNbElems == 1 )
        {
            removeResult = new RemoveResult<>( mergedResult.getModifiedPage(), mergedResult.getRemovedElement(), pos );
            
            // Add the current root to the copied pages, as we won't use it anymore
            transaction.addCopiedWALObject( this );
            transaction.removeWALObject( id );
        }
        else
        {
            // Remove the element and update the reference to the changed pages
            removeResult = removeKey( transaction, mergedResult, pos );
            transaction.removeWALObject( id );
            transaction.updateWAL( revision, this, removeResult.getModifiedPage() );
        }

        return removeResult;
    }


    /**
     * Borrows an element from the right sibling, creating a new sibling with one
     * less element and creating a new page where the element to remove has been
     * deleted and the borrowed element added on the right.
     */
    private DeleteResult<K, V> borrowFromRight( WriteTransaction transaction, MergedWithSiblingResult<K, V> mergedResult, Node<K, V> sibling, int pos ) throws IOException
    {
        // Create the new sibling, with one less element at the beginning
        Node<K, V> newSibling = transaction.newNode( btreeInfo, sibling.getPageNbElems() - 1 );

        K siblingKey = transaction.getPage( btreeInfo, sibling.children[0] ).getLeftMostKey( transaction );

        // Copy the keys and children of the old sibling in the new sibling
        System.arraycopy( sibling.keys, 1, newSibling.keys, 0, newSibling.getPageNbElems() );
        System.arraycopy( sibling.children, 1, newSibling.children, 0, newSibling.getPageNbElems() + 1 );
        
        // Create the new page and add the new element at the end
        // First copy the current node, with the same size
        Node<K, V> newNode = transaction.newNode( btreeInfo, pageNbElems );

        // Copy the keys and the values up to the insertion position
        int index = Math.abs( pos );

        // Copy the key and children from sibling
        newNode.keys[pageNbElems - 1] = new KeyHolder<>( btreeInfo.getKeySerializer(), siblingKey ); // 1
        newNode.children[pageNbElems] = sibling.children[0]; // 8

        if ( index < 2 )
        {
            // Copy the keys
            System.arraycopy( keys, 1, newNode.keys, 0, pageNbElems - 1 );

            // Inject the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            setChild( newNode, 0, modifiedPage );

            // Copy the children
            System.arraycopy( children, 2, newNode.children, 1, pageNbElems - 1 );
        }
        else
        {
            if ( index > 2 )
            {
                // Copy the keys before the deletion point
                System.arraycopy( keys, 0, newNode.keys, 0, index - 2 ); // 4
            }

            // Inject the new modified page key
            newNode.keys[index - 2] = new KeyHolder<>( btreeInfo.getKeySerializer(), mergedResult.getModifiedPage().getLeftMostKey( transaction ) ); // 2

            if ( index < pageNbElems )
            {
                // Copy the remaining keys after the deletion point
                System.arraycopy( keys, index, newNode.keys, index - 1, pageNbElems - index ); // 3

                // Copy the remaining children after the deletion point
                System.arraycopy( children, index + 1, newNode.children, index, pageNbElems - index ); // 7
            }

            // Copy the children before the deletion point
            System.arraycopy( children, 0, newNode.children, 0, index - 1 ); // 5

            // Inject the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            setChild( newNode, index - 1, modifiedPage );
        }

        // Store the updated pages in the WAL
        transaction.removeWALObject( id );
        transaction.updateWAL( revision, this, newNode );
        transaction.removeWALObject( sibling.id );
        transaction.updateWAL( sibling.getRevision(), sibling, newSibling );

        // Create the result
        return new BorrowedFromRightResult<>( mergedResult.getCopiedPages(), newNode,
            newSibling, mergedResult.getRemovedElement() );
    }


    /**
     * Borrows an element from the left sibling, creating a new sibling with one
     * less element and creating a new page where the element to remove has been
     * deleted and the borrowed element added on the left.
     */
    private DeleteResult<K, V> borrowFromLeft( WriteTransaction transaction, MergedWithSiblingResult<K, V> mergedResult, Node<K, V> sibling, int pos ) throws IOException
    {
        // The sibling is on the left, borrow the rightmost element
        Page<K, V> siblingChild = transaction.getPage( btreeInfo, sibling.children[sibling.pageNbElems] );

        // Create the new sibling, with one less element at the end
        Node<K, V> newSibling = transaction.newNode( btreeInfo, sibling.getPageNbElems() - 1 );

        // Copy the keys and children of the old sibling in the new sibling
        System.arraycopy( sibling.keys, 0, newSibling.keys, 0, newSibling.getPageNbElems() );
        System.arraycopy( sibling.children, 0, newSibling.children, 0, newSibling.getPageNbElems() + 1 );

        // Create the new page and add the new element at the beginning
        // First copy the current node, with the same size
        Node<K, V> newNode = transaction.newNode( btreeInfo, pageNbElems );

        // Sets the first children
        setChild( newNode, 0, siblingChild );

        int index = Math.abs( pos );
        
        if ( index < 2 )
        {
            newNode.keys[0] = new KeyHolder<>( btreeInfo.getKeySerializer(), mergedResult.getModifiedPage().getLeftMostKey( transaction) );
            System.arraycopy( keys, 1, newNode.keys, 1, pageNbElems - 1 );

            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            setChild( newNode, 1, modifiedPage );
            System.arraycopy( children, 2, newNode.children, 2, pageNbElems - 1 );
        }
        else
        {
            // Set the first key
            K leftMostKey = transaction.getPage( btreeInfo, children[0] ).getLeftMostKey( transaction );
            newNode.keys[0] = new KeyHolder<>( btreeInfo.getKeySerializer(), leftMostKey ); //2

            if ( index > 2 )
            {
                // Copy the keys before the deletion point
                System.arraycopy( keys, 0, newNode.keys, 1, index - 2 ); // 4
            }

            // Inject the modified key
            newNode.keys[index - 1] = new KeyHolder<>( btreeInfo.getKeySerializer(), mergedResult.getModifiedPage().getLeftMostKey( transaction ) ); // 3

            if ( index < pageNbElems)
            {
                // Add copy the remaining keys after the deletion point
                System.arraycopy( keys, index, newNode.keys, index, pageNbElems - index ); // 5

                // Copy the remaining children after the insertion point
                System.arraycopy( children, index + 1, newNode.children, index + 1, pageNbElems - index ); // 8
            }

            // Copy the children before the insertion point
            System.arraycopy( children, 0, newNode.children, 1, index - 1 ); // 6

            // Insert the modified page
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            setChild( newNode, index, modifiedPage );
        }
        
        // Store the updated pages in the WAL
        transaction.removeWALObject( id );
        transaction.updateWAL( revision, this, newNode );
        transaction.removeWALObject( sibling.id );
        transaction.updateWAL( sibling.getRevision(), sibling, newSibling );

        // Create the result
        return new BorrowedFromLeftResult<>( mergedResult.getCopiedPages(), newNode,
            newSibling,
            mergedResult.getRemovedElement() );
    }


    /**
     * We have to merge the node with its sibling, both have N/2 elements before the element
     * removal.
     * The trick is that the new Node will have N keys, not N-1. The reason is that when two children
     * get merged, the two Node we will merge will refer to N/2 + 1 and N/2 children, N + 1 children,
     * and we need N keys in the node for that.
     * 
     * <pre>
     * Before :
     *                 +---+---+                           +---+---+
     *                 | 3 | 7 |                           | 12| 15|
     *                 +---+---+                           +---+---+
     *                 |   |   |                           |   |   |
     *       +---------+   |   +-------+           +-------+   |   +-------+
     *       |             |           |           |           |           |
     *       v             v           v           v           v           v
     * +---+---+---+   +---+---+   +---+---+   +---+---+   +---+---+   +---+---+
     * | 0 | 1 | 2 |   | 3 | 5 |   | 7 | 8 |   | 10| 11|   | 12| 14|   | 15| 16|
     * +---+---+---+   +---+---+   +---+---+   +---+---+   +---+---+   +---+---+
     *                                           ^
     *                                           |
     *                                           +--- Deleted element
     *                                           
     * Leaves [10,11] and [12,14] are merged, leaving the node [12,15] with only one key. We
     * have to merge the [3,7] node with the [12, 15] node :
     * 
     *                         +---+---+---+---+
     *                         | 3 | 7 | 11| 15|
     *                         +---+---+---+---+
     *                         |   |   |   |   |
     *       +-----------------+   |   |   |   +-------------------+
     *       |                     |   |   |                       |
     *       |             +-------+   |   +---------+             |
     *       |             |           |             |             |
     *       v             v           v             v             v
     * +---+---+---+   +---+---+   +---+---+   +---+---+---+   +---+---+
     * | 0 | 1 | 2 |   | 3 | 5 |   | 7 | 8 |   | 11| 12| 14|   | 15| 16|
     * +---+---+---+   +---+---+   +---+---+   +---+---+---+   +---+---+
     * 
     * We have 5 children instead of 6, but 4 keys in the merged nodes.
     * </pre>
     */
    private DeleteResult<K, V> mergeWithSibling( WriteTransaction transaction, MergedWithSiblingResult<K, V> mergedResult,
        Node<K, V> sibling, boolean isLeft, int pos ) throws IOException
    {
        // Create the new node. It will contain N keys (the maximum number)
        // as we merge two nodes that contain N/2 elements
        Node<K, V> newNode = transaction.newNode( btreeInfo, btreeInfo.getPageNbElem() );

        Tuple<K, V> removedElement = mergedResult.getRemovedElement();
        int half = btreeInfo.getPageNbElem() / 2;
        int index = Math.abs( pos );

        if ( isLeft )
        {
            // The sibling is on the left. Copy all of its elements in the new node first
            System.arraycopy( sibling.keys, 0, newNode.keys, 0, half ); //1
            System.arraycopy( sibling.children, 0, newNode.children, 0, half + 1 ); //2

            // Then copy all the elements up to the deletion point
            if ( index < 2 )
            {
                newNode.keys[half] = new KeyHolder<K>( btreeInfo.getKeySerializer(), mergedResult.getModifiedPage().getLeftMostKey( transaction ) );
                System.arraycopy( keys, 1, newNode.keys, half + 1, half - 1 );

                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                setChild( newNode, half + 1, modifiedPage );
                System.arraycopy( children, 2, newNode.children, half + 2, half - 1 );
            }
            else
            {
                // Copy the left part of the node keys up to the deletion point
                // Insert the new key
                K leftMostKey = transaction.getPage( btreeInfo, children[0] ).getLeftMostKey( transaction );
                newNode.keys[half] = new KeyHolder<K>( btreeInfo.getKeySerializer(), leftMostKey ); // 3

                if ( index > 2 )
                {
                    System.arraycopy( keys, 0, newNode.keys, half + 1, index - 2 ); //4
                }

                // Inject the new merged key
                newNode.keys[half + index - 1] = new KeyHolder<K>( btreeInfo.getKeySerializer(), mergedResult.getModifiedPage().getLeftMostKey( transaction ) ); //5

                if ( index < half )
                {
                    System.arraycopy( keys, index, newNode.keys, half + index, half - index ); //6
                    System.arraycopy( children, index + 1, newNode.children, half + index + 1, half - index ); //9
                }

                // Copy the children before the deletion point
                System.arraycopy( children, 0, newNode.children, half + 1, index - 1 ); // 7

                // Inject the new merged child
                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                setChild( newNode, half + index, modifiedPage );
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
                setChild( newNode, 0, modifiedPage );

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
                newNode.keys[index - 2] = new KeyHolder<K>( btreeInfo.getKeySerializer(), mergedResult.getModifiedPage().getLeftMostKey( transaction ) ); //2

                // Inject the modified children
                Page<K, V> modifiedPage = mergedResult.getModifiedPage();
                setChild( newNode, index - 1, modifiedPage );

                // Add the remaining node's key if needed
                if ( index < half )
                {
                    System.arraycopy( keys, index, newNode.keys, index - 1, half - index ); //5

                    // Add the remaining children if below half
                    System.arraycopy( children, index + 1, newNode.children, index, half - index ); // 8
                }
            }

            // Inject the new key from sibling
            newNode.keys[half - 1] = new KeyHolder<>( btreeInfo.getKeySerializer(), sibling.findLeftMost( transaction )
                .getKey() ); //3

            // Copy the sibling keys
            System.arraycopy( sibling.keys, 0, newNode.keys, half, half );

            // Add the sibling children
            System.arraycopy( sibling.children, 0, newNode.children, half, half + 1 ); // 9
        }

        // Update the WAL
        transaction.removeWALObject( id );
        transaction.removeWALObject( sibling.id );
        transaction.updateWAL( revision, this, newNode );
        transaction.addCopiedWALObject( sibling );
        
        // And create the result
        return new MergedWithSiblingResult<>( mergedResult.getCopiedPages(), newNode, removedElement );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public DeleteResult<K, V> delete( WriteTransaction transaction, K key, Node<K, V> parent, int parentPos ) throws IOException
    {
        // We first try to delete the element from the child it belongs to
        // Find the key in the page
        int pos = findPos( key );
        int index = pos;
        boolean found = pos < 0;
        DeleteResult<K, V> deleteResult;
        Page<K, V> child;

        // Go down the B-tree
        if ( found )
        {
            index = - ( pos + 1 );
            child = transaction.getPage( btreeInfo, children[-pos] );
            deleteResult = child.delete( transaction, key, this, -pos );
        }
        else
        {
            child = transaction.getPage( btreeInfo, children[pos] );
            deleteResult = child.delete( transaction, key, this, pos );
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
        
        // If we had to borrow an element in the child, then we have to update the current page
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
                return handleRootRemove( transaction, mergedResult, pos );
            }

            // We have some parent. Check if the current page is not half full
            int halfSize = btreeInfo.getPageNbElem() / 2;

            if ( pageNbElems > halfSize )
            {
                // The page has more than N/2 elements.
                // We simply remove the element from the page, and if it was the leftmost,
                // we return the new pivot (it will replace any instance of the removed
                // key in its parents)
                return removeKey( transaction, mergedResult, pos );
            }
            else
            {
                // We will remove one element from a page that will have less than N/2 elements,
                // which will lead to some reorganization : either we can borrow an element from
                // a sibling, or we will have to merge two pages
                int siblingPos = selectSibling( transaction, parent, parentPos );

                Node<K, V> sibling = ( Node<K, V> ) ( transaction.getPage( btreeInfo, parent.children[siblingPos] ) );

                if ( sibling.getPageNbElems() > halfSize )
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
                    return mergeWithSibling( transaction, mergedResult, sibling, ( siblingPos < parentPos ), pos );
                }
            }
        }

        // We should never reach this point
        return null;
    }

    
    /**
     * The deletion in a children has moved an element from one of its sibling. The key
     * is present in the current node.
     */
    private RemoveResult<K, V> handleBorrowedResult( WriteTransaction transaction, 
        BorrowedFromSiblingResult<K, V> borrowedResult, int pos ) throws IOException
    {
        Page<K, V> modifiedPage = borrowedResult.getModifiedPage();
        Page<K, V> modifiedSibling = borrowedResult.getModifiedSibling();

        Node<K, V> newNode = copy( transaction );

        if ( pos < 0 )
        {
            int newPos = -( pos + 1 );

            if ( borrowedResult.isFromRight() )
            {
                // Update the keys
                newNode.keys[newPos] = new KeyHolder<K>( btreeInfo.getKeySerializer(), modifiedPage.findLeftMost( transaction )
                    .getKey() );
                newNode.keys[newPos + 1] = new KeyHolder<K>( btreeInfo.getKeySerializer(), modifiedSibling
                    .findLeftMost( transaction )
                    .getKey() );

                // Update the children
                setChild( newNode, newPos + 1, modifiedPage );
                setChild( newNode, newPos + 2, modifiedSibling );
            }
            else
            {
                // Update the keys
                newNode.keys[newPos] = new KeyHolder<K>( btreeInfo.getKeySerializer(), modifiedPage.findLeftMost( transaction )
                    .getKey() );

                // Update the children
                setChild( newNode, newPos, modifiedSibling );
                setChild( newNode, newPos + 1, modifiedPage );
            }
        }
        else
        {
            if ( borrowedResult.isFromRight() )
            {
                // Update the keys
                newNode.keys[pos] = new KeyHolder<K>( btreeInfo.getKeySerializer(), modifiedSibling.findLeftMost( transaction )
                    .getKey() );

                // Update the children
                setChild( newNode, pos, modifiedPage );
                setChild( newNode, pos + 1, modifiedSibling );
            }
            else
            {
                // Update the keys
                newNode.keys[pos - 1] = new KeyHolder<>( btreeInfo.getKeySerializer(), modifiedPage
                    .findLeftMost( transaction )
                    .getKey() );

                // Update the children
                setChild( newNode, pos - 1, modifiedSibling );
                setChild( newNode, pos, modifiedPage );
            }
        }
        
        transaction.removeWALObject( id );
        transaction.addWALObject( newNode );

        // Modify the result and return
        return new RemoveResult<>( newNode, borrowedResult.getRemovedElement(), pos );
    }


    /**
     * Remove the key at a given position.
     */
    private RemoveResult<K, V> removeKey( WriteTransaction transaction, MergedWithSiblingResult<K, V> mergedResult, int pos )
        throws IOException
    {
        // First copy the current page, but remove one element in the copied page
        Node<K, V> newNode = transaction.newNode( btreeInfo, pageNbElems - 1 );
        
        // Compute the key position.
        int index = Math.abs( pos ) - 2;

        if ( index < 0 )
        {
            // Copy the keys and the children
            System.arraycopy( keys, 1, newNode.keys, 0, newNode.pageNbElems );
            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            setChild( newNode, 0, modifiedPage );
            System.arraycopy( children, 2, newNode.children, 1, pageNbElems - 1 );
        }
        else
        {
            // Copy the keys
            if ( index > 0 )
            {
                System.arraycopy( keys, 0, newNode.keys, 0, index );
            }

            newNode.keys[index] = new KeyHolder<K>( btreeInfo.getKeySerializer(), mergedResult.getModifiedPage()
                .findLeftMost( transaction ).getKey() );

            if ( index < pageNbElems - 2 )
            {
                System.arraycopy( keys, index + 2, newNode.keys, index + 1, pageNbElems - index - 2 );
            }

            // Copy the children
            System.arraycopy( children, 0, newNode.children, 0, index + 1 );

            Page<K, V> modifiedPage = mergedResult.getModifiedPage();
            setChild( newNode, index + 1, modifiedPage );

            if ( index < pageNbElems - 2 )
            {
                System.arraycopy( children, index + 3, newNode.children, index + 2, pageNbElems - index - 2 );
            }
        }
        
        transaction.removeWALObject( id );
        transaction.updateWAL( revision, this, newNode );

        // Create the result
        return new RemoveResult<>( newNode, mergedResult.getRemovedElement(), pos );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V get( Transaction transaction, K key ) throws IOException, KeyNotFoundException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            // Here, if we have found the key in the node, then we must go down into
            // the right child, not the left one
            return transaction.getPage( btreeInfo, children[-pos] ).get( transaction, key );
        }
        else
        {
            return transaction.getPage( btreeInfo, children[pos] ).get( transaction, key );
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
     * Set the value at a give position
     *
     * @param pos The position in the values array
     * @param value the value to inject
     */
    /* no qualifier */void setValue( int pos, long value )
    {
        children[pos] = value;
    }


    /**
     * This method is used when we have to replace a child in a page when we have
     * found the key in the tree (the value will be changed, so we have made
     * copies of the existing pages).
     */
    private ModifyResult<K, V> replaceChild( WriteTransaction transaction, ModifyResult<K, V> result, int pos ) throws IOException
    {
        // Just copy the current page and update its revision
        Node<K, V> newNode = this;
        
        if ( revision != transaction.getRevision() )
        {
            newNode = copy( transaction );
        }

        // Last, we update the children table of the newly created page
        // to point on the modified child
        Page<K, V> modifiedPage = result.getModifiedPage();

        setChild( newNode, pos, modifiedPage );

        // We can return the result, where we update the modifiedPage,
        // to avoid the creation of a new object
        result.setModifiedPage( newNode);
        
        if ( revision != transaction.getRevision() )
        {
            transaction.updateWAL( revision, this, newNode );
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
     * Adds a new key into a copy of the current page at a given position. We return the
     * modified page. The new page will have one more key than the current page.
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
            newNode = transaction.newNode( btreeInfo, pageNbElems + 1 );

            // Copy the keys and the children up to the insertion position
            if ( pageNbElems > 0 )
            {
                System.arraycopy( keys, 0, newNode.keys, 0, pos );
                System.arraycopy( children, 0, newNode.children, 0, pos );
            }

            // Add the new key and children
            newNode.keys[pos] = new KeyHolder<K>( btreeInfo.getKeySerializer(), pivot );
            setChild( newNode, pos, leftPage );
            setChild( newNode, pos + 1, rightPage );

            // And copy the remaining keys and children
            if ( pageNbElems - pos > 0 )
            {
                System.arraycopy( keys, pos, newNode.keys, pos + 1, pageNbElems - pos );
                System.arraycopy( children, pos + 1, newNode.children, pos + 2, children.length - pos - 1 );
            }
        }
        else
        {
            KeyHolder[] newKeys = ( KeyHolder[] ) Array.newInstance( KeyHolder.class, pageNbElems + 1);
            long[] newChildren = ( long[] ) Array.newInstance( long.class, pageNbElems + 2 );
            
            // Copy the keys and the children up to the insertion position
            System.arraycopy( keys, 0, newKeys, 0, pos );
            System.arraycopy( children, 0, newChildren, 0, pos );

            // Add the new key and children
            newKeys[pos] = new KeyHolder<K>( btreeInfo.getKeySerializer(), pivot );
            setChild( newChildren, pos, leftPage );
            setChild( newChildren, pos + 1, rightPage );
            
            // And copy the remaining keys and children
            if ( pageNbElems - pos > 0 )
            {
                System.arraycopy( keys, pos, newKeys, pos + 1, pageNbElems - pos );
                System.arraycopy( children, pos + 1, newChildren, pos + 2, pageNbElems - pos );
            }
            
            keys = newKeys;
            children = newChildren;
            pageNbElems++;
        }

        // Create the result
        ModifyResult<K, V> result = new ModifyResult<>( newNode, pivot, null );
        
        // Remove the old Node from the transaction
        transaction.removeWALObject( id );
        
        // and add the new Node into the transaction Pages map
        transaction.updateWAL( revision, this, newNode );

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
        int middle = btreeInfo.getPageNbElem() >> 1;
        InsertResult<K, V> result;

        // Create two new pages
        Node<K, V> newLeftPage = transaction.newNode( btreeInfo, middle );
        Node<K, V> newRightPage = transaction.newNode( btreeInfo, middle );

        // Determinate where to store the new value
        // If it's before the middle, insert the value on the left,
        // the key in the middle will become the new pivot
        if ( pos < middle )
        {
            // Copy the keys and the children up to the insertion position
            System.arraycopy( keys, 0, newLeftPage.keys, 0, pos );
            System.arraycopy( children, 0, newLeftPage.children, 0, pos );

            // Add the new element
            newLeftPage.keys[pos] = new KeyHolder<K>( btreeInfo.getKeySerializer(), splitResult.getPivot() );
            setChild( newLeftPage, pos, splitResult.getLeftPage() );
            setChild( newLeftPage, pos + 1, splitResult.getRightPage() );

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
            setChild( newLeftPage, middle, splitResult.getLeftPage() );

            // And process the right page now
            System.arraycopy( keys, middle, newRightPage.keys, 0, middle );
            System.arraycopy( children, middle + 1, newRightPage.children, 1, middle );
            setChild( newRightPage, 0, splitResult.getRightPage() );

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
            newRightPage.keys[pos - middle - 1] = new KeyHolder<K>( btreeInfo.getKeySerializer(), splitResult.getPivot() );
            setChild( newRightPage, pos - middle - 1, splitResult.getLeftPage() );
            setChild( newRightPage, pos - middle, splitResult.getRightPage() );

            // And copy the remaining elements minus the new pivot
            System.arraycopy( keys, pos, newRightPage.keys, pos - middle, pageNbElems - pos );
            System.arraycopy( children, pos + 1, newRightPage.children, pos + 1 - middle, pageNbElems - pos );

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
        if ( isBTreeUser() )
        {
            transaction.addCopiedWALObject( this );
        }

        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TupleCursor<K, V> browse( Transaction transaction, ParentPos<K, V>[] stack, int depth ) throws IOException
    {
        stack[depth++] = new ParentPos<>( this, 0 );

        Page<K, V> page = transaction.getPage( btreeInfo, children[0] );

        return page.browse( transaction, stack, depth );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TupleCursor<K, V> browse( Transaction transaction, K key, ParentPos<K, V>[] stack, int depth ) throws IOException
    {
        int pos = findPos( key );

        if ( pos < 0 )
        {
            pos = -pos;
        }

        // We first stack the current page
        stack[depth++] = new ParentPos<>( this, pos );

        Page<K, V> page = transaction.getPage( btreeInfo, children[pos] );

        return page.browse( transaction, key, stack, depth );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public KeyCursor<K> browseKeys( Transaction transaction, ParentPos<K, K>[] stack, int depth ) throws IOException
    {
        stack[depth++] = new ParentPos( this, 0 );

        Page<K, V> page = transaction.getPage( btreeInfo, children[0] );

        return page.browseKeys( transaction, stack, depth );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains( Transaction transaction, K key, V value ) throws IOException
    {
        int pos = findPos( key );
        Page<K, V> child;

        if ( pos < 0 )
        {
            // Here, if we have found the key in the node, then we must go down into
            // the right child, not the left one
            child = transaction.getPage( btreeInfo, children[-pos] );
        }
        else
        {
            child = transaction.getPage( btreeInfo, children[pos] );
        }
        
        return child.contains( transaction, key, value );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> copy( WriteTransaction transaction )
    {
        Node<K, V> newNode = transaction.newNode( btreeInfo, pageNbElems );

        // Copy the keys
        System.arraycopy( keys, 0, newNode.keys, 0, pageNbElems );

        // Copy the children
        System.arraycopy( children, 0, newNode.children, 0, pageNbElems + 1 );

        return newNode;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> findLeftMost( Transaction transaction ) throws IOException
    {
        return transaction.getPage( btreeInfo, children[0] ).findLeftMost( transaction );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public K getLeftMostKey( Transaction transaction ) throws IOException
    {
        return transaction.getPage( btreeInfo, children[0] ).getLeftMostKey( transaction );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> findRightMost( Transaction transaction ) throws IOException
    {
        return transaction.getPage( btreeInfo, children[pageNbElems] ).findRightMost( transaction );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public K getRightMostKey( Transaction transaction ) throws IOException
    {
        return transaction.getPage( btreeInfo, children[pageNbElems - 1] ).getRightMostKey( transaction );
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasKey( Transaction transaction, K key ) throws IOException
    {
        int pos = findPos( key );
        long offset;

        if ( pos < 0 )
        {
            // Here, if we have found the key in the node, then we must go down into
            // the right child, not the left one
            offset = children[-pos];
        }
        else
        {
            offset = children[pos];
        }
        
        Page<K, V> child = transaction.getPage( btreeInfo, offset );
        
        return child.hasKey( transaction, key );
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
     * <li>the page id : a long</li>
     * <li>the revision : a long</li>
     * <li>the number of elements : an negative int</li>
     *   <li>the keys and values, N times :
     *     <ul>
     *       <li>key[n] : a serialized key</li>
     *       <li>value[n] : a serialized value</li>
     *     </ul>
     *     followed by the last value
     *   </li>
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
        int nbBuffers = 1 + 1 + 1 + pageNbElems * 2 + 1;
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
        buffer = IntSerializer.serialize( -pageNbElems );
        serializedData.add( buffer );
        serializedSize += buffer.length;

        // Iterate on the keys and values. We first serialize the value, then the key
        // until we are done with all of them. If we are serializing a page, we have
        // to serialize one more value
        for ( int pos = 0; pos < pageNbElems; pos++ )
        {
            // Start with the value
            dataSize += serializeNodeValue( pos, serializedData );
            dataSize += serializeNodeKey( pos, serializedData );
        }
        
        // Nodes have one more value to serialize
        dataSize += serializeNodeValue( pageNbElems, serializedData );

        // Store the data size at the third position in the list of buffers
        // (ie, just after the number of elements, and just before the keys/values)
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
     * {@inheritDoc}
     */
    /* no qualifier */Page<K, V> getPage( Transaction transaction, int pos ) throws IOException
    {
        if ( ( pos >= 0 ) && ( pos < children.length ) )
        {
            return transaction.getPage( btreeInfo, children[pos] );
        }
        else
        {
            return null;
        }
    }

    
    /**
     * Deserialize a Node. It will contain the following data :<br/>
     * <ul>
     * <li>the keys : an array of serialized keys</li>
     * <li>the values : an array of serialized values</li>
     * </ul>
     * 
     * The three first values have already been deserialized by the caller.
     * {@inheritDoc}
     */
    public Node<K, V> deserialize( ByteBuffer byteBuffer ) throws IOException
    {
        // Iterate on the keys and values. We first serialize the value, then the key
        // until we are done with all of them. If we are serializing a page, we have
        // to serialize one more value
        for ( int pos = 0; pos < pageNbElems; pos++ )
        {
            // Start with the value
            children[pos] = LongSerializer.INSTANCE.deserialize( byteBuffer );

            // Then the key
            K key = btreeInfo.getKeySerializer().deserialize( byteBuffer );
            this.setKey( pos, new KeyHolder<>( btreeInfo.getKeySerializer(), key ) );
        }
        
        // The last value
        children[pageNbElems] = LongSerializer.INSTANCE.deserialize( byteBuffer );

        return this;
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
    {
        // For a node, we just store the children's offsets
        // The first offset
        byte[] buffer = LongSerializer.serialize( children[pos] );
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

        if ( pageNbElems > 0 )
        {
            // Start with the first child
            sb.append( children[0] );

            for ( int i = 0; i < pageNbElems; i++ )
            {
                sb.append( tabs );
                sb.append( "<" );
                sb.append( getKey( i ) ).append( ">\n" );
                
                if ( offset > 0 )
                {
                    sb.append( "0x" ).append( Long.toHexString( offset ) );
                }
                else
                {
                    sb.append( "ID:" ).append( -offset );
                    
                }
                
                sb.append( children[i + 1] );
            }
        }

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

        if ( pageNbElems > 0 )
        {
            // Start with the first child
            if ( children[0] < 0 )
            {
                sb.append( ":ID" ).append( -children[0] );
            }
            else
            {
                sb.append( ':' ).append( String.format( "0x%04X", children[0] ) );
            }

            for ( int i = 0; i < pageNbElems; i++ )
            {
                sb.append( "|<" ).append( keys[i] ).append( ">|" );

                if ( children[i+1] < 0 )
                {
                    sb.append( ":ID" ).append( -children[i+1] );
                }
                else
                {
                    sb.append( ':' ).append( String.format( "0x%04X", children[i+1] ) );
                }
            }
        }

        sb.append( "}" );

        return sb.toString();
    }
}
