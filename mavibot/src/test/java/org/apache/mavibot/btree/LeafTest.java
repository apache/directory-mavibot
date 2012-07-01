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

import java.io.IOException;

import org.apache.mavibot.btree.comparator.LongComparator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

/**
 * A unit test class for Leaf
 * 
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class LeafTest
{
    private BTree<Long, String> btree = null;
    
    /**
     * Create a btree
     */
    @Before
    public void setup() throws IOException
    {
        btree = new BTree<Long, String>( new LongComparator() );
        btree.setPageSize( 8 );
    }

    
    /**
     * A helper method to insert elements in a Leaf
     */
    private Leaf<Long, String> insert( Leaf<Long, String> leaf, long key, String value )
    {
        InsertResult<Long, String> result = leaf.insert( 1L, key, value );
        
        return (Leaf<Long, String>)((ModifyResult)result).modifiedPage;
    }
    

    /**
     * Test that deleting an entry from an empty page returns a NOT_PRESENT result
     * @throws IOException
     */
    @Test
    public void testDeleteFromEmptyLeaf() throws IOException
    {
        Leaf<Long, String> leaf = new Leaf<Long, String>( btree );
        
        DeleteResult<Long, String> result = leaf.delete( 1L, 1L, null, -1 );
        
        assertEquals( NotPresentResult.NOT_PRESENT, result );
    }


    /**
     * Test that deleting an entry which is not present in the leaf works
     * @throws IOException
     */
    @Test
    public void testDeleteNotPresentElementFromRootLeaf() throws IOException
    {
        Leaf<Long, String> leaf = new Leaf<Long, String>( btree );
        leaf = insert( leaf, 1L, "v1" );
        leaf = insert( leaf, 2L, "v2" );
        leaf = insert( leaf, 3L, "v3" );
        leaf = insert( leaf, 4L, "v4" );
        
        DeleteResult<Long, String> result = leaf.delete( 2L, 5L, null, -1 );
        
        assertEquals( NotPresentResult.NOT_PRESENT, result );
    }


    /**
     * Test that deleting an entry which is present in the leaf works
     * @throws IOException
     */
    @Test
    public void testDeletePresentElementFromRootLeaf() throws IOException
    {
        Leaf<Long, String> leaf = new Leaf<Long, String>( btree );
        leaf = insert( leaf, 1L, "v1" );
        leaf = insert( leaf, 2L, "v2" );
        leaf = insert( leaf, 3L, "v3" );
        leaf = insert( leaf, 4L, "v4" );
        
        DeleteResult<Long, String> result = leaf.delete( 4L, 3L, null, -1 );
        
        assertTrue( result instanceof RemoveResult );
        
        Tuple<Long, String> removedElement = ((RemoveResult)result).removedElement;
        Page<Long, String> newLeaf = ((RemoveResult)result).modifiedPage;
        
        assertEquals( Long.valueOf( 3L), removedElement.getKey() );
        assertEquals( "v3", removedElement.getValue() );
        assertEquals( 3, newLeaf.getNbElems() );
        
        assertEquals( "v1", newLeaf.find( 1L ) );
        assertEquals( "v2", newLeaf.find( 2L ) );
        assertNull( newLeaf.find( 3L ) );
        assertEquals( "v4", newLeaf.find( 4L ) );
    }


    /**
     * Test that deleting the first element return the correct result
     * @throws IOException
     */
    @Test
    public void testDeleteFirstElementFromRootLeaf() throws IOException
    {
        Leaf<Long, String> leaf = new Leaf<Long, String>( btree );
        leaf = insert( leaf, 1L, "v1" );
        leaf = insert( leaf, 2L, "v2" );
        leaf = insert( leaf, 3L, "v3" );
        leaf = insert( leaf, 4L, "v4" );
        
        DeleteResult<Long, String> result = leaf.delete( 4L, 1L, null, -1 );
        
        assertTrue( result instanceof RemoveResult );
        
        RemoveResult<Long, String> removeResult = (RemoveResult<Long, String>)result;
        
        Tuple<Long, String> removedElement = removeResult.removedElement;
        Page<Long, String> newLeaf = removeResult.modifiedPage;
        Long leftMost = removeResult.newLeftMost;
        
        assertEquals( Long.valueOf( 2L), leftMost );
        assertEquals( Long.valueOf( 1L), removedElement.getKey() );
        assertEquals( "v1", removedElement.getValue() );
        assertEquals( 3, newLeaf.getNbElems() );

        assertNull( newLeaf.find( 1L ) );
        assertEquals( "v2", newLeaf.find( 2L ) );
        assertEquals( "v3", newLeaf.find( 3L ) );
        assertEquals( "v4", newLeaf.find( 4L ) );
    }
    
    
    /**
     * Check that deleting an element from a leaf with N/2 element works when we borrow
     * an element in a left page with more than N/2 elements
     */
    @Test
    public void testRemoveBorrowingFromLeftSibling()
    {
        Node<Long, String> parent = new Node<Long, String>( btree, 1L, 2 );
        Leaf<Long, String> left = new Leaf<Long, String>( btree );
        Leaf<Long, String> target = new Leaf<Long, String>( btree );
        Leaf<Long, String> right = new Leaf<Long, String>( btree );
        
        // Fill the left page
        left = insert( left, 1L, "v1" );
        left = insert( left, 2L, "v2" );
        left = insert( left, 3L, "v3" );
        left = insert( left, 4L, "v4" );
        left = insert( left, 5L, "v5" );

        // Fill the target page
        target = insert( target, 6L, "v6" );
        target = insert( target, 7L, "v7" );
        target = insert( target, 8L, "v8" );
        target = insert( target, 9L, "v9" );

        // Fill the right page
        right = insert( right, 10L, "v10" );
        right = insert( right, 11L, "v11" );
        right = insert( right, 12L, "v12" );
        right = insert( right, 13L, "v13" );

        parent.children[0] = left;
        parent.children[1] = target;
        parent.children[2] = right;
        
        // Update the parent
        parent.keys[0] = 6L;
        parent.keys[1] = 10L;
        
        // Now, delete the element from the target page
        DeleteResult<Long, String> result = target.delete( 2L, 7L, parent, 1 );
        
        assertTrue( result instanceof BorrowedFromLeftResult );
        
        BorrowedFromLeftResult<Long, String> borrowed = (BorrowedFromLeftResult<Long, String>)result;
        assertEquals( Long.valueOf( 5L ), borrowed.newLeftMost );
        Tuple<Long, String> removedKey = borrowed.getRemovedElement();

        assertEquals( Long.valueOf( 7L ), removedKey.getKey() );
        
        // Check the modified leaf
        Leaf<Long, String> newLeaf = (Leaf<Long, String>)borrowed.getModifiedPage();
        
        assertEquals( 4, newLeaf.nbElems );
        assertEquals( Long.valueOf( 5L ), newLeaf.keys[0] );
        assertEquals( Long.valueOf( 6L ), newLeaf.keys[1] );
        assertEquals( Long.valueOf( 8L ), newLeaf.keys[2] );
        assertEquals( Long.valueOf( 9L ), newLeaf.keys[3] );
        
        // Check the sibling
        Leaf<Long, String> leftSibling = (Leaf<Long, String>)borrowed.getModifiedSibling();
        
        assertEquals( 4, leftSibling.nbElems );
        assertEquals( Long.valueOf( 1L ), leftSibling.keys[0] );
        assertEquals( Long.valueOf( 2L ), leftSibling.keys[1] );
        assertEquals( Long.valueOf( 3L ), leftSibling.keys[2] );
        assertEquals( Long.valueOf( 4L ), leftSibling.keys[3] );
    }
    
    
    /**
     * Check that deleting an element from a leaf with N/2 element works when we borrow
     * an element in a right page with more than N/2 elements
     */
    @Test
    public void testRemoveBorrowingFromRightSibling()
    {
        Node<Long, String> parent = new Node<Long, String>( btree, 1L, 2 );
        Leaf<Long, String> left = new Leaf<Long, String>( btree );
        Leaf<Long, String> target = new Leaf<Long, String>( btree );
        Leaf<Long, String> right = new Leaf<Long, String>( btree );
        
        // Fill the left page
        left = insert( left, 1L, "v1" );
        left = insert( left, 2L, "v2" );
        left = insert( left, 3L, "v3" );
        left = insert( left, 4L, "v4" );

        // Fill the target page
        target = insert( target, 6L, "v6" );
        target = insert( target, 7L, "v7" );
        target = insert( target, 8L, "v8" );
        target = insert( target, 9L, "v9" );

        // Fill the right page
        right = insert( right, 10L, "v10" );
        right = insert( right, 11L, "v11" );
        right = insert( right, 12L, "v12" );
        right = insert( right, 13L, "v13" );
        right = insert( right, 14L, "v14" );

        parent.children[0] = left;
        parent.children[1] = target;
        parent.children[2] = right;
        
        // Update the parent
        parent.keys[0] = 6L;
        parent.keys[1] = 10L;
        
        // Now, delete the element from the target page
        DeleteResult<Long, String> result = target.delete( 2L, 7L, parent, 1 );
        
        assertTrue( result instanceof BorrowedFromRightResult );
        
        BorrowedFromRightResult<Long, String> borrowed = (BorrowedFromRightResult<Long, String>)result;
        assertEquals( Long.valueOf( 11L ), borrowed.newLeftMost );
        Tuple<Long, String> removedKey = borrowed.getRemovedElement();

        assertEquals( Long.valueOf( 7L ), removedKey.getKey() );
        
        // Check the modified leaf
        Leaf<Long, String> newLeaf = (Leaf<Long, String>)borrowed.getModifiedPage();
        
        assertEquals( 4, newLeaf.nbElems );
        assertEquals( Long.valueOf( 6L ), newLeaf.keys[0] );
        assertEquals( Long.valueOf( 8L ), newLeaf.keys[1] );
        assertEquals( Long.valueOf( 9L ), newLeaf.keys[2] );
        assertEquals( Long.valueOf( 10L ), newLeaf.keys[3] );
        
        // Check the sibling
        Leaf<Long, String> rightSibling = (Leaf<Long, String>)borrowed.getModifiedSibling();
        
        assertEquals( 4, rightSibling.nbElems );
        assertEquals( Long.valueOf( 11L ), rightSibling.keys[0] );
        assertEquals( Long.valueOf( 12L ), rightSibling.keys[1] );
        assertEquals( Long.valueOf( 13L ), rightSibling.keys[2] );
        assertEquals( Long.valueOf( 14L ), rightSibling.keys[3] );
    }
    
    
    /**
     * Check that deleting an element from a leaf with N/2 element works when we merge
     * it with one of its sibling, if both has N/2 elements
     */
    @Test
    public void testRemoveMergeWithSibling()
    {
        Node<Long, String> parent = new Node<Long, String>( btree, 1L, 2 );
        Leaf<Long, String> left = new Leaf<Long, String>( btree );
        Leaf<Long, String> target = new Leaf<Long, String>( btree );
        Leaf<Long, String> right = new Leaf<Long, String>( btree );
        
        // Fill the left page
        left = insert( left, 1L, "v1" );
        left = insert( left, 2L, "v2" );
        left = insert( left, 3L, "v3" );
        left = insert( left, 4L, "v4" );

        // Fill the target page
        target = insert( target, 5L, "v5" );
        target = insert( target, 6L, "v6" );
        target = insert( target, 7L, "v7" );
        target = insert( target, 8L, "v8" );

        // Fill the right page
        right = insert( right, 9L, "v9" );
        right = insert( right, 10L, "v10" );
        right = insert( right, 11L, "v11" );
        right = insert( right, 12L, "v12" );
        
        parent.children[0] = left;
        parent.children[1] = target;
        parent.children[2] = right;
        
        // Update the parent
        parent.keys[0] = 5L;
        parent.keys[1] = 9L;
        
        // Now, delete the element from the target page
        DeleteResult<Long, String> result = target.delete( 2L, 7L, parent, 1 );
        
        assertTrue( result instanceof MergedWithSiblingResult );
        
        MergedWithSiblingResult<Long, String> merged = (MergedWithSiblingResult<Long, String>)result;
        assertEquals( Long.valueOf( 1L ), merged.newLeftMost );
        Tuple<Long, String> removedKey = merged.getRemovedElement();

        assertEquals( Long.valueOf( 7L ), removedKey.getKey() );
        
        // Check the modified leaf
        Leaf<Long, String> newLeaf = (Leaf<Long, String>)merged.getModifiedPage();
        
        assertEquals( 7, newLeaf.nbElems );
        assertEquals( Long.valueOf( 1L ), newLeaf.keys[0] );
        assertEquals( Long.valueOf( 2L ), newLeaf.keys[1] );
        assertEquals( Long.valueOf( 3L ), newLeaf.keys[2] );
        assertEquals( Long.valueOf( 4L ), newLeaf.keys[3] );
        assertEquals( Long.valueOf( 5L ), newLeaf.keys[4] );
        assertEquals( Long.valueOf( 6L ), newLeaf.keys[5] );
        assertEquals( Long.valueOf( 8L ), newLeaf.keys[6] );
    }
}
