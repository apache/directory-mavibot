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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * A unit test class for Leaf
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryLeafTest
{
    private BTree<Long, String> btree = null;


    /**
     * Create a btree
     */
    @Before
    public void setup() throws IOException
    {
        btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE );
        btree.setPageSize( 8 );
    }


    @After
    public void shutdown() throws IOException
    {
        btree.close();
    }


    /**
     * A helper method to insert elements in a Leaf
     * @throws IOException
     */
    private InMemoryLeaf<Long, String> insert( InMemoryLeaf<Long, String> leaf, long key, String value )
        throws IOException
    {
        InsertResult<Long, String> result = leaf.insert( key, value, 1L );

        return ( InMemoryLeaf<Long, String> ) ( ( ModifyResult<Long, String> ) result ).getModifiedPage();
    }


    /**
     * Test that deleting an entry from an empty page returns a NOT_PRESENT result
     * @throws IOException
     */
    @Test
    public void testDeleteFromEmptyLeaf() throws IOException
    {
        InMemoryLeaf<Long, String> leaf = new InMemoryLeaf<Long, String>( btree );

        DeleteResult<Long, String> result = leaf.delete( 1L, null, 1L, null, -1 );

        assertEquals( NotPresentResult.NOT_PRESENT, result );
    }


    /**
     * Test that deleting an entry which is not present in the leaf works
     * @throws IOException
     */
    @Test
    public void testDeleteNotPresentElementFromRootLeaf() throws IOException
    {
        InMemoryLeaf<Long, String> leaf = new InMemoryLeaf<Long, String>( btree );
        leaf = insert( leaf, 1L, "v1" );
        leaf = insert( leaf, 2L, "v2" );
        leaf = insert( leaf, 3L, "v3" );
        leaf = insert( leaf, 4L, "v4" );

        DeleteResult<Long, String> result = leaf.delete( 5L, null, 2L, null, -1 );

        assertEquals( NotPresentResult.NOT_PRESENT, result );
    }


    /**
     * Test that deleting an entry which is present in the leaf works
     * @throws IOException
     */
    @Test
    public void testDeletePresentElementFromRootLeaf() throws IOException
    {
        InMemoryLeaf<Long, String> leaf = new InMemoryLeaf<Long, String>( btree );
        leaf = insert( leaf, 1L, "v1" );
        leaf = insert( leaf, 2L, "v2" );
        leaf = insert( leaf, 3L, "v3" );
        leaf = insert( leaf, 4L, "v4" );

        DeleteResult<Long, String> result = leaf.delete( 3L, null, 4L, null, -1 );

        assertTrue( result instanceof RemoveResult );

        Tuple<Long, String> removedElement = ( ( RemoveResult<Long, String> ) result ).getRemovedElement();
        Page<Long, String> newLeaf = ( ( RemoveResult<Long, String> ) result ).getModifiedPage();

        assertEquals( Long.valueOf( 3L ), removedElement.getKey() );
        assertEquals( "v3", removedElement.getValue() );
        assertEquals( 3, newLeaf.getNbElems() );

        try
        {
            assertEquals( "v1", newLeaf.get( 1L ) );
            assertEquals( "v2", newLeaf.get( 2L ) );
            assertEquals( "v4", newLeaf.get( 4L ) );
        }
        catch ( KeyNotFoundException knfe )
        {
            fail();
        }

        try
        {
            newLeaf.get( 3L );
            fail();
        }
        catch ( KeyNotFoundException knfe )
        {
            // Expected
        }
    }


    /**
     * Test that deleting the first element return the correct result
     * @throws IOException
     */
    @Test
    public void testDeleteFirstElementFromRootLeaf() throws IOException
    {
        InMemoryLeaf<Long, String> leaf = new InMemoryLeaf<Long, String>( btree );
        leaf = insert( leaf, 1L, "v1" );
        leaf = insert( leaf, 2L, "v2" );
        leaf = insert( leaf, 3L, "v3" );
        leaf = insert( leaf, 4L, "v4" );

        DeleteResult<Long, String> result = leaf.delete( 1L, null, 4L, null, -1 );

        assertTrue( result instanceof RemoveResult );

        RemoveResult<Long, String> removeResult = ( RemoveResult<Long, String> ) result;

        Tuple<Long, String> removedElement = removeResult.getRemovedElement();
        Page<Long, String> newLeaf = removeResult.getModifiedPage();

        assertEquals( Long.valueOf( 1L ), removedElement.getKey() );
        assertEquals( "v1", removedElement.getValue() );
        assertEquals( 3, newLeaf.getNbElems() );

        try
        {
            newLeaf.get( 1L );
            fail();
        }
        catch ( KeyNotFoundException knfe )
        {
            // expected
        }

        try
        {
            assertEquals( "v2", newLeaf.get( 2L ) );
            assertEquals( "v3", newLeaf.get( 3L ) );
            assertEquals( "v4", newLeaf.get( 4L ) );
        }
        catch ( KeyNotFoundException knfe )
        {
            fail();
        }
    }


    /**
     * Check that deleting an element from a leaf with N/2 element works when we borrow
     * an element in a left page with more than N/2 elements.
     * The BTree contains :
     *            +--[1, 2, 3, 4, 5]
     * [6, 10]-+--[6, 7, 8, 9]
     *            +--[10, 11, 12, 13]
     * @throws IOException
     */
    @Test
    public void testDeleteBorrowingFromLeftSibling() throws IOException
    {
        InMemoryNode<Long, String> parent = new InMemoryNode<Long, String>( btree, 1L, 2 );
        InMemoryLeaf<Long, String> left = new InMemoryLeaf<Long, String>( btree );
        InMemoryLeaf<Long, String> target = new InMemoryLeaf<Long, String>( btree );
        InMemoryLeaf<Long, String> right = new InMemoryLeaf<Long, String>( btree );

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

        parent.setPageHolder( 0, new PageHolder<Long, String>( btree, left ) );
        parent.setPageHolder( 1, new PageHolder<Long, String>( btree, target ) );
        parent.setPageHolder( 2, new PageHolder<Long, String>( btree, right ) );

        // Update the parent
        parent.setKey( 0, new KeyHolder<Long>( 6L ) );
        parent.setKey( 1, new KeyHolder<Long>( 10L ) );

        // Now, delete the element from the target page
        DeleteResult<Long, String> result = target.delete( 7L, null, 2L, parent, 1 );

        assertTrue( result instanceof BorrowedFromLeftResult );

        BorrowedFromLeftResult<Long, String> borrowed = ( BorrowedFromLeftResult<Long, String> ) result;
        Tuple<Long, String> removedKey = borrowed.getRemovedElement();

        assertEquals( Long.valueOf( 7L ), removedKey.getKey() );

        // Check the modified leaf
        InMemoryLeaf<Long, String> newLeaf = ( InMemoryLeaf<Long, String> ) borrowed.getModifiedPage();

        assertEquals( 4, newLeaf.getNbElems() );
        assertEquals( Long.valueOf( 5L ), newLeaf.getKey( 0 ) );
        assertEquals( Long.valueOf( 6L ), newLeaf.getKey( 1 ) );
        assertEquals( Long.valueOf( 8L ), newLeaf.getKey( 2 ) );
        assertEquals( Long.valueOf( 9L ), newLeaf.getKey( 3 ) );

        // Check the sibling
        InMemoryLeaf<Long, String> leftSibling = ( InMemoryLeaf<Long, String> ) borrowed.getModifiedSibling();

        assertEquals( 4, leftSibling.getNbElems() );
        assertEquals( Long.valueOf( 1L ), leftSibling.getKey( 0 ) );
        assertEquals( Long.valueOf( 2L ), leftSibling.getKey( 1 ) );
        assertEquals( Long.valueOf( 3L ), leftSibling.getKey( 2 ) );
        assertEquals( Long.valueOf( 4L ), leftSibling.getKey( 3 ) );
    }


    /**
     * Check that deleting an element from a leaf with N/2 element works when we borrow
     * an element in a right page with more than N/2 elements
     * @throws IOException
     */
    @Test
    public void testDeleteBorrowingFromRightSibling() throws IOException
    {
        InMemoryNode<Long, String> parent = new InMemoryNode<Long, String>( btree, 1L, 2 );
        InMemoryLeaf<Long, String> left = new InMemoryLeaf<Long, String>( btree );
        InMemoryLeaf<Long, String> target = new InMemoryLeaf<Long, String>( btree );
        InMemoryLeaf<Long, String> right = new InMemoryLeaf<Long, String>( btree );

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

        parent.setPageHolder( 0, new PageHolder<Long, String>( btree, left ) );
        parent.setPageHolder( 1, new PageHolder<Long, String>( btree, target ) );
        parent.setPageHolder( 2, new PageHolder<Long, String>( btree, right ) );

        // Update the parent
        parent.setKey( 0, new KeyHolder<Long>( 6L ) );
        parent.setKey( 1, new KeyHolder<Long>( 10L ) );

        // Now, delete the element from the target page
        DeleteResult<Long, String> result = target.delete( 7L, null, 2L, parent, 1 );

        assertTrue( result instanceof BorrowedFromRightResult );

        BorrowedFromRightResult<Long, String> borrowed = ( BorrowedFromRightResult<Long, String> ) result;
        assertEquals( Long.valueOf( 11L ), borrowed.getModifiedSibling().getKey( 0 ) );
        Tuple<Long, String> removedKey = borrowed.getRemovedElement();

        assertEquals( Long.valueOf( 7L ), removedKey.getKey() );

        // Check the modified leaf
        InMemoryLeaf<Long, String> newLeaf = ( InMemoryLeaf<Long, String> ) borrowed.getModifiedPage();

        assertEquals( 4, newLeaf.getNbElems() );
        assertEquals( Long.valueOf( 6L ), newLeaf.getKey( 0 ) );
        assertEquals( Long.valueOf( 8L ), newLeaf.getKey( 1 ) );
        assertEquals( Long.valueOf( 9L ), newLeaf.getKey( 2 ) );
        assertEquals( Long.valueOf( 10L ), newLeaf.getKey( 3 ) );

        // Check the sibling
        InMemoryLeaf<Long, String> rightSibling = ( InMemoryLeaf<Long, String> ) borrowed.getModifiedSibling();

        assertEquals( 4, rightSibling.getNbElems() );
        assertEquals( Long.valueOf( 11L ), rightSibling.getKey( 0 ) );
        assertEquals( Long.valueOf( 12L ), rightSibling.getKey( 1 ) );
        assertEquals( Long.valueOf( 13L ), rightSibling.getKey( 2 ) );
        assertEquals( Long.valueOf( 14L ), rightSibling.getKey( 3 ) );
    }


    /**
     * Check that deleting an element from a leaf with N/2 element works when we merge
     * it with one of its sibling, if both has N/2 elements
     * @throws IOException
     */
    @Test
    public void testDeleteMergeWithSibling() throws IOException
    {
        InMemoryNode<Long, String> parent = new InMemoryNode<Long, String>( btree, 1L, 2 );
        InMemoryLeaf<Long, String> left = new InMemoryLeaf<Long, String>( btree );
        InMemoryLeaf<Long, String> target = new InMemoryLeaf<Long, String>( btree );
        InMemoryLeaf<Long, String> right = new InMemoryLeaf<Long, String>( btree );

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

        parent.setPageHolder( 0, new PageHolder<Long, String>( btree, left ) );
        parent.setPageHolder( 1, new PageHolder<Long, String>( btree, target ) );
        parent.setPageHolder( 2, new PageHolder<Long, String>( btree, right ) );

        // Update the parent
        parent.setKey( 0, new KeyHolder<Long>( 5L ) );
        parent.setKey( 1, new KeyHolder<Long>( 9L ) );

        // Now, delete the element from the target page
        DeleteResult<Long, String> result = target.delete( 7L, null, 2L, parent, 1 );

        assertTrue( result instanceof MergedWithSiblingResult );

        MergedWithSiblingResult<Long, String> merged = ( MergedWithSiblingResult<Long, String> ) result;
        Tuple<Long, String> removedKey = merged.getRemovedElement();

        assertEquals( Long.valueOf( 7L ), removedKey.getKey() );

        // Check the modified leaf
        InMemoryLeaf<Long, String> newLeaf = ( InMemoryLeaf<Long, String> ) merged.getModifiedPage();

        assertEquals( 7, newLeaf.getNbElems() );
        assertEquals( Long.valueOf( 1L ), newLeaf.getKey( 0 ) );
        assertEquals( Long.valueOf( 2L ), newLeaf.getKey( 1 ) );
        assertEquals( Long.valueOf( 3L ), newLeaf.getKey( 2 ) );
        assertEquals( Long.valueOf( 4L ), newLeaf.getKey( 3 ) );
        assertEquals( Long.valueOf( 5L ), newLeaf.getKey( 4 ) );
        assertEquals( Long.valueOf( 6L ), newLeaf.getKey( 5 ) );
        assertEquals( Long.valueOf( 8L ), newLeaf.getKey( 6 ) );
    }


    /**
     * Test the findPos() method
     * @throws Exception
     */
    @Test
    public void testFindPos() throws Exception
    {
        InMemoryLeaf<Long, String> leaf = new InMemoryLeaf<Long, String>( btree );

        // Inject the values
        for ( long i = 0; i < 8; i++ )
        {
            long value = i + i + 1;
            leaf = ( InMemoryLeaf<Long, String> ) ( ( ModifyResult<Long, String> ) leaf.insert( value, "V" + value, 0L ) )
                .getModifiedPage();
        }

        // Check the findPos() method now
        for ( long i = 0; i < 17; i++ )
        {
            if ( i % 2 == 1 )
            {
                assertEquals( -( i / 2 + 1 ), leaf.findPos( i ) );
            }
            else
            {
                assertEquals( i / 2, leaf.findPos( i ) );
            }
        }
    }
}
