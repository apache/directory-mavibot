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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Tests the browse methods on a managed BTree
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PersistedBTreeBrowseTest
{
    private BTree<Long, String> btree = null;

    private RecordManager recordManager1 = null;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dataDir = null;


    /**
     * Create a BTree for this test
     */
    @Before
    public void createBTree() throws IOException
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        openRecordManagerAndBtree();

        try
        {
            // Create a new BTree which allows duplicate values
            btree = recordManager1.addBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE, true );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    @After
    public void cleanup() throws IOException
    {
        dataDir = new File( System.getProperty( "java.io.tmpdir" ) + "/recordman" );

        btree.close();

        if ( dataDir.exists() )
        {
            FileUtils.deleteDirectory( dataDir );
        }

        recordManager1.close();
        assertTrue( recordManager1.isContextOk() );
    }


    /**
     * Reload the BTree into a new record manager
     */
    private void openRecordManagerAndBtree()
    {
        try
        {
            if ( recordManager1 != null )
            {
                recordManager1.close();
            }

            // Now, try to reload the file back
            recordManager1 = new RecordManager( dataDir.getAbsolutePath() );

            // load the last created btree
            if ( btree != null )
            {
                btree = recordManager1.getManagedTree( btree.getName() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    /**
     * Check a tuple
     */
    private void checkTuple( Tuple<Long, String> tuple, long key, String value ) throws EndOfFileExceededException,
        IOException
    {
        assertNotNull( tuple );
        assertEquals( key, ( long ) tuple.getKey() );
        assertEquals( value, tuple.getValue() );
    }


    /**
     * Check a next() call
     */
    private void checkNext( TupleCursor<Long, String> cursor, long key, String value, boolean next, boolean prev )
        throws EndOfFileExceededException, IOException
    {
        Tuple<Long, String> tuple = cursor.next();

        checkTuple( tuple, key, value );
        assertEquals( next, cursor.hasNext() );
        assertEquals( prev, cursor.hasPrev() );
    }


    /**
     * Check a prev() call
     */
    private void checkPrev( TupleCursor<Long, String> cursor, long key, String value, boolean next, boolean prev )
        throws EndOfFileExceededException, IOException
    {
        Tuple<Long, String> tuple = cursor.prev();
        assertNotNull( tuple );
        assertEquals( key, ( long ) tuple.getKey() );
        assertEquals( value, tuple.getValue() );
        assertEquals( next, cursor.hasNext() );
        assertEquals( prev, cursor.hasPrev() );
    }


    /**
     * Construct a String representation of a number padded with 0 on the left
     */
    private String toString( long value, int size )
    {
        String valueStr = Long.toString( value );

        StringBuilder sb = new StringBuilder();

        if ( size > valueStr.length() )
        {
            for ( int i = valueStr.length(); i < size; i++ )
            {
                sb.append( "0" );
            }
        }

        sb.append( valueStr );

        return sb.toString();
    }


    //----------------------------------------------------------------------------------------
    // The Browse tests
    //----------------------------------------------------------------------------------------
    /**
     * Test the browse methods on an empty btree
     * @throws KeyNotFoundException 
     */
    @Test
    public void testBrowseEmptyBTree() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        TupleCursor<Long, String> cursor = btree.browse();

        assertFalse( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );

        try
        {
            cursor.next();
            fail();
        }
        catch ( NoSuchElementException nsee )
        {
            // Expected
        }

        try
        {
            cursor.prev();
            fail();
        }
        catch ( NoSuchElementException nsee )
        {
            // Expected
        }

        assertEquals( 0L, cursor.getRevision() );
    }


    /**
     * Test the browse methods on a btree containing just a leaf
     */
    @Test
    public void testBrowseBTreeLeafNext() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        btree.insert( 1L, "1" );
        btree.insert( 4L, "4" );
        btree.insert( 2L, "2" );
        btree.insert( 3L, "3" );
        btree.insert( 5L, "5" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move forward
        cursor.beforeFirst();

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 1L, "1", true, false );
        checkNext( cursor, 2L, "2", true, true );
        checkNext( cursor, 3L, "3", true, true );
        checkNext( cursor, 4L, "4", true, true );
        checkNext( cursor, 5L, "5", false, true );
    }


    /**
     * Test the browse methods on a btree containing just a leaf
     */
    @Test
    public void testBrowseBTreeLeafPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        btree.insert( 1L, "1" );
        btree.insert( 4L, "4" );
        btree.insert( 2L, "2" );
        btree.insert( 3L, "3" );
        btree.insert( 5L, "5" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move backward
        cursor.afterLast();

        checkPrev( cursor, 5L, "5", false, true );
        checkPrev( cursor, 4L, "4", true, true );
        checkPrev( cursor, 3L, "3", true, true );
        checkPrev( cursor, 2L, "2", true, true );
        checkPrev( cursor, 1L, "1", true, false );
    }


    /**
     * Test the browse methods on a btree containing just a leaf and see if we can
     * move at the end or at the beginning
     */
    @Test
    public void testBrowseBTreeLeafFirstLast() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        btree.insert( 1L, "1" );
        btree.insert( 4L, "4" );
        btree.insert( 2L, "2" );
        btree.insert( 3L, "3" );
        btree.insert( 5L, "5" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // We should not be able to move backward
        try
        {
            cursor.prev();
            fail();
        }
        catch ( NoSuchElementException nsee )
        {
            // Expected
        }

        // Start browsing three elements
        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );
        Tuple<Long, String> tuple = cursor.next();
        tuple = cursor.next();
        tuple = cursor.next();

        // We should be at 3 now
        assertTrue( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );
        assertEquals( 3L, ( long ) tuple.getKey() );
        assertEquals( "3", tuple.getValue() );

        // Move to the end
        cursor.afterLast();

        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );

        // We should not be able to move forward
        try
        {
            cursor.next();
            fail();
        }
        catch ( NoSuchElementException nsee )
        {
            // Expected
        }

        // We should be at 5
        tuple = cursor.prev();
        assertEquals( 5L, ( long ) tuple.getKey() );
        assertEquals( "5", tuple.getValue() );

        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );

        // Move back to the origin
        cursor.beforeFirst();

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        // We should be at 1
        tuple = cursor.next();
        assertEquals( 1L, ( long ) tuple.getKey() );
        assertEquals( "1", tuple.getValue() );

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );
    }


    /**
     * Test the browse methods on a btree containing just a leaf and see if we can
     * move back and forth
     */
    @Test
    public void testBrowseBTreeLeafNextPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        btree.insert( 1L, "1" );
        btree.insert( 4L, "4" );
        btree.insert( 2L, "2" );
        btree.insert( 3L, "3" );
        btree.insert( 5L, "5" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // We should not be able to move backward
        try
        {
            cursor.prev();
            fail();
        }
        catch ( NoSuchElementException nsee )
        {
            // Expected
        }

        // Start browsing three elements
        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );
        Tuple<Long, String> tuple = cursor.next();
        tuple = cursor.next();
        tuple = cursor.next();

        // We should be at 3 now
        assertTrue( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );
        assertEquals( 3L, ( long ) tuple.getKey() );
        assertEquals( "3", tuple.getValue() );

        // Now, move to the prev value
        tuple = cursor.prev();
        assertEquals( 2L, ( long ) tuple.getKey() );
        assertEquals( "2", tuple.getValue() );

        // And to the next value
        tuple = cursor.next();
        assertEquals( 3L, ( long ) tuple.getKey() );
        assertEquals( "3", tuple.getValue() );
    }


    /**
     * Test the browse methods on a btree containing many nodes
     */
    @Test
    public void testBrowseBTreeNodesNext() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        for ( long i = 1; i < 1000L; i++ )
        {
            btree.insert( i, Long.toString( i ) );
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move forward
        cursor.beforeFirst();

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 1L, "1", true, false );

        for ( long i = 2L; i < 999L; i++ )
        {
            checkNext( cursor, i, Long.toString( i ), true, true );
        }

        checkNext( cursor, 999L, "999", false, true );
    }


    /**
     * Test the browse methods on a btree containing many nodes
     */
    @Test
    public void testBrowseBTreeNodesPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        for ( long i = 1; i < 1000L; i++ )
        {
            btree.insert( i, Long.toString( i ) );
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move backward
        cursor.afterLast();

        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );

        checkPrev( cursor, 999L, "999", false, true );

        for ( long i = 998L; i > 1L; i-- )
        {
            checkPrev( cursor, i, Long.toString( i ), true, true );
        }

        checkPrev( cursor, 1L, "1", true, false );
    }


    /**
     * Test the browse methods on a btree containing just a leaf with duplicate values
     */
    @Test
    public void testBrowseBTreeLeafNextDups1() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some duplicate data
        btree.insert( 1L, "1" );
        btree.insert( 1L, "4" );
        btree.insert( 1L, "2" );
        btree.insert( 1L, "3" );
        btree.insert( 1L, "5" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move forward
        cursor.beforeFirst();

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 1L, "1", true, false );
        checkNext( cursor, 1L, "2", true, true );
        checkNext( cursor, 1L, "3", true, true );
        checkNext( cursor, 1L, "4", true, true );
        checkNext( cursor, 1L, "5", false, true );
    }


    /**
     * Test the browse methods on a btree containing just a leaf with duplicate values
     */
    @Test
    public void testBrowseBTreeLeafNextDupsN() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some duplicate data
        btree.insert( 1L, "1" );
        btree.insert( 1L, "4" );
        btree.insert( 1L, "2" );
        btree.insert( 2L, "3" );
        btree.insert( 3L, "5" );
        btree.insert( 3L, "7" );
        btree.insert( 3L, "6" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move forward
        cursor.beforeFirst();

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 1L, "1", true, false );
        checkNext( cursor, 1L, "2", true, true );
        checkNext( cursor, 1L, "4", true, true );
        checkNext( cursor, 2L, "3", true, true );
        checkNext( cursor, 3L, "5", true, true );
        checkNext( cursor, 3L, "6", true, true );
        checkNext( cursor, 3L, "7", false, true );
    }


    /**
     * Test the browse methods on a btree containing just a leaf with duplicate values
     */
    @Test
    public void testBrowseBTreeLeafPrevDups1() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some duplicate data
        btree.insert( 1L, "1" );
        btree.insert( 1L, "4" );
        btree.insert( 1L, "2" );
        btree.insert( 1L, "3" );
        btree.insert( 1L, "5" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move backward
        cursor.afterLast();

        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );

        checkPrev( cursor, 1L, "5", false, true );
        checkPrev( cursor, 1L, "4", true, true );
        checkPrev( cursor, 1L, "3", true, true );
        checkPrev( cursor, 1L, "2", true, true );
        checkPrev( cursor, 1L, "1", true, false );
    }


    /**
     * Test the browse methods on a btree containing just a leaf with duplicate values
     */
    @Test
    public void testBrowseBTreeLeafPrevDupsN() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some duplicate data
        btree.insert( 1L, "1" );
        btree.insert( 1L, "4" );
        btree.insert( 1L, "2" );
        btree.insert( 2L, "3" );
        btree.insert( 3L, "5" );
        btree.insert( 3L, "7" );
        btree.insert( 3L, "6" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move backward
        cursor.afterLast();

        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );

        checkPrev( cursor, 3L, "7", false, true );
        checkPrev( cursor, 3L, "6", true, true );
        checkPrev( cursor, 3L, "5", true, true );
        checkPrev( cursor, 2L, "3", true, true );
        checkPrev( cursor, 1L, "4", true, true );
        checkPrev( cursor, 1L, "2", true, true );
        checkPrev( cursor, 1L, "1", true, false );
    }


    /**
     * Test the browse methods on a btree containing nodes with duplicate values
     */
    @Test
    public void testBrowseBTreeNodesNextDupsN() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        for ( long i = 1; i < 1000L; i++ )
        {
            for ( long j = 1; j < 10; j++ )
            {
                btree.insert( i, Long.toString( j ) );
            }
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move backward
        cursor.beforeFirst();

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );
        boolean next = true;
        boolean prev = false;

        for ( long i = 1L; i < 1000L; i++ )
        {
            for ( long j = 1L; j < 10L; j++ )
            {
                checkNext( cursor, i, Long.toString( j ), next, prev );

                if ( ( i == 1L ) && ( j == 1L ) )
                {
                    prev = true;
                }

                if ( ( i == 999L ) && ( j == 8L ) )
                {
                    next = false;
                }
            }
        }
    }


    /**
     * Test the browse methods on a btree containing nodes with duplicate values
     */
    @Test
    public void testBrowseBTreeNodesPrevDupsN() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        for ( long i = 1; i < 1000L; i++ )
        {
            for ( int j = 1; j < 10; j++ )
            {
                btree.insert( i, Long.toString( j ) );
            }
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move backward
        cursor.afterLast();

        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );
        boolean next = false;
        boolean prev = true;

        for ( long i = 999L; i > 0L; i-- )
        {
            for ( long j = 9L; j > 0L; j-- )
            {
                checkPrev( cursor, i, Long.toString( j ), next, prev );

                if ( ( i == 1L ) && ( j == 2L ) )
                {
                    prev = false;
                }

                if ( ( i == 999L ) && ( j == 9L ) )
                {
                    next = true;
                }
            }
        }
    }


    /**
     * Test the browse methods on a btree containing just a leaf with duplicate values
     * stored into a sub btree
     */
    @Test
    public void testBrowseBTreeLeafNextDupsSubBTree1() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        // Inject some duplicate data which will be stored into a sub btree
        for ( long i = 1L; i < 32L; i++ )
        {
            btree.insert( 1L, toString( i, 2 ) );
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move forward
        cursor.beforeFirst();

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 1L, "01", true, false );

        for ( long i = 2L; i < 31L; i++ )
        {
            checkNext( cursor, 1L, toString( i, 2 ), true, true );
        }

        checkNext( cursor, 1L, "31", false, true );
    }


    /**
     * Test the browse methods on a btree containing just a leaf with duplicate values
     */
    @Test
    public void testBrowseBTreeLeafPrevDupsSubBTree1() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        // Inject some duplicate data which will be stored into a sub btree
        for ( long i = 1L; i < 32L; i++ )
        {
            btree.insert( 1L, toString( i, 2 ) );
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move backward
        cursor.afterLast();

        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );

        checkPrev( cursor, 1L, "31", false, true );

        for ( long i = 30L; i > 1L; i-- )
        {
            checkPrev( cursor, 1L, toString( i, 2 ), true, true );
        }

        checkPrev( cursor, 1L, "01", true, false );
    }


    //----------------------------------------------------------------------------------------
    // The BrowseFrom tests
    //----------------------------------------------------------------------------------------
    /**
     * Test the browseFrom method on an empty tree
     */
    @Test
    public void testBrowseFromEmptyBTree() throws IOException, BTreeAlreadyManagedException
    {
        TupleCursor<Long, String> cursor = btree.browseFrom( 1L );

        assertFalse( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );

        try
        {
            cursor.next();
            fail();
        }
        catch ( NoSuchElementException nsee )
        {
            // Expected
        }

        try
        {
            cursor.prev();
            fail();
        }
        catch ( NoSuchElementException nsee )
        {
            // Expected
        }

        assertEquals( -1L, cursor.getRevision() );
    }


    /**
     * Test the browseFrom methods on a btree containing just a leaf
     */
    @Test
    public void testBrowseFromBTreeLeaf() throws IOException, BTreeAlreadyManagedException
    {
        // Inject some data
        btree.insert( 1L, "1" );
        btree.insert( 7L, "7" );
        btree.insert( 3L, "3" );
        btree.insert( 5L, "5" );
        btree.insert( 9L, "9" );

        // Create the cursor, starting at 5
        TupleCursor<Long, String> cursor = btree.browseFrom( 5L );

        assertTrue( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        // Move forward
        checkNext( cursor, 5L, "5", true, true );
        checkNext( cursor, 7L, "7", true, true );
        checkNext( cursor, 9L, "9", false, true );

        cursor.close();

        // now, start at 5 and move backward
        cursor = btree.browseFrom( 5L );

        assertTrue( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        // Move backward
        checkPrev( cursor, 3L, "3", true, true );
        checkPrev( cursor, 1L, "1", true, false );
        cursor.close();

        // Start at the first key
        cursor = btree.browseFrom( 1L );
        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 1L, "1", true, false );
        checkNext( cursor, 3L, "3", true, true );

        // Start before the first key
        cursor = btree.browseFrom( 0L );
        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 1L, "1", true, false );
        checkNext( cursor, 3L, "3", true, true );

        // Start at the last key
        cursor = btree.browseFrom( 9L );
        assertTrue( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 9L, "9", false, true );
        checkPrev( cursor, 7L, "7", true, true );

        // Start after the last key
        cursor = btree.browseFrom( 10L );
        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );

        checkPrev( cursor, 9L, "9", false, true );
        checkPrev( cursor, 7L, "7", true, true );

        // Start in the middle with a non existent key
        cursor = btree.browseFrom( 4L );
        assertTrue( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        checkNext( cursor, 5L, "5", true, true );

        // Start in the middle with a non existent key
        cursor = btree.browseFrom( 4L );

        checkPrev( cursor, 3L, "3", true, true );
    }


    /**
     * Test the browseFrom method on a btree with a non existing key
     */
    @Test
    public void testBrowseFromBTreeNodesNotExistingKey() throws IOException, BTreeAlreadyManagedException
    {
        // Inject some data
        for ( long i = 0; i <= 1000L; i += 2 )
        {
            btree.insert( i, Long.toString( i ) );
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browseFrom( 1500L );

        assertFalse( cursor.hasNext() );
        assertTrue( cursor.hasPrev() );
        assertEquals( 1000L, cursor.prev().getKey().longValue() );
    }


    /**
     * Test the browseFrom method on a btree containing nodes with duplicate values
     */
    @Test
    public void testBrowseFromBTreeNodesPrevDupsN() throws IOException, BTreeAlreadyManagedException
    {
        // Inject some data
        for ( long i = 1; i < 1000L; i += 2 )
        {
            for ( int j = 1; j < 10; j++ )
            {
                btree.insert( i, Long.toString( j ) );
            }
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browseFrom( 500L );

        // Move forward

        assertTrue( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );
        boolean next = true;
        boolean prev = true;

        for ( long i = 501L; i < 1000L; i += 2 )
        {
            for ( long j = 1L; j < 10L; j++ )
            {
                if ( ( i == 999L ) && ( j == 9L ) )
                {
                    next = false;
                }

                checkNext( cursor, i, Long.toString( j ), next, prev );
            }
        }
    }


    //----------------------------------------------------------------------------------------
    // The TupleCursor.moveToNext/PrevNonDuplicateKey method tests
    //----------------------------------------------------------------------------------------
    /**
      * Test the TupleCursor.nextKey method on a btree containing nodes
      * with duplicate values.
      */
    @Test
    public void testNextKey() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        for ( long i = 1; i < 1000L; i++ )
        {
            for ( long j = 1; j < 10; j++ )
            {
                btree.insert( i, Long.toString( j ) );
            }
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move forward
        cursor.beforeFirst();

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );
        boolean next = true;
        boolean prev = false;

        for ( long i = 1L; i < 999L; i++ )
        {
            Tuple<Long, String> tuple = cursor.nextKey();

            checkTuple( tuple, i, "1" );

            if ( i == 999L )
            {
                next = false;
            }

            assertEquals( next, cursor.hasNext() );
            assertEquals( prev, cursor.hasPrev() );

            if ( i == 1L )
            {
                prev = true;
            }
        }
    }


    /**
     * Test the TupleCursor.nextKey method on a btree containing nodes
     * with duplicate values.
     */
    @Test
    @Ignore
    public void testNextKeyDups() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        //for ( long i = 1; i < 3; i++ )
        {
            for ( long j = 1; j < 9; j++ )
            {
                btree.insert( 1L, Long.toString( j ) );
            }
        }

        btree.insert( 1L, "10" );

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move forward
        cursor.beforeFirst();

        assertFalse( cursor.hasPrevKey() );
        assertTrue( cursor.hasNextKey() );

        Tuple<Long, String> tuple = cursor.nextKey();

        checkTuple( tuple, 1L, "1" );

        cursor.beforeFirst();
        long val = 1L;

        while ( cursor.hasNext() )
        {
            tuple = cursor.next();

            assertEquals( Long.valueOf( 1L ), tuple.getKey() );
            assertEquals( Long.toString( val ), tuple.getValue() );

            val++;
        }

        assertFalse( cursor.hasNextKey() );
        assertFalse( cursor.hasPrevKey() );
    }


    /**
     * Test the TupleCursor.moveToPrevNonDuplicateKey method on a btree containing nodes
     * with duplicate values.
     */
    @Test
    public void testPrevKey() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Inject some data
        for ( long i = 1; i < 1000L; i++ )
        {
            for ( long j = 1; j < 10; j++ )
            {
                btree.insert( i, Long.toString( j ) );
            }
        }

        // Create the cursor
        TupleCursor<Long, String> cursor = btree.browse();

        // Move backward
        cursor.afterLast();

        assertTrue( cursor.hasPrev() );
        assertFalse( cursor.hasNext() );
        boolean next = true;
        boolean prev = true;

        for ( long i = 999L; i > 0L; i-- )
        {
            Tuple<Long, String> tuple = cursor.prevKey();

            if ( i == 1L )
            {
                prev = false;
            }

            checkTuple( tuple, i, "1" );
            assertEquals( next, cursor.hasNext() );
            assertEquals( prev, cursor.hasPrev() );

            if ( i == 999L )
            {
                next = true;
            }
        }
    }


    /**
     * Test the overwriting of elements
     */
    @Test
    public void testOverwrite() throws Exception
    {
        btree.setAllowDuplicates( false );

        // Adding an element with a null value
        btree.insert( 1L, "1" );

        assertTrue( btree.hasKey( 1L ) );

        assertEquals( "1", btree.get( 1L ) );

        btree.insert( 1L, "10" );

        assertTrue( btree.hasKey( 1L ) );
        assertEquals( "10", btree.get( 1L ) );

        btree.close();
    }


    @Ignore("test used for debugging")
    @Test
    public void testAdd20Random() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        long[] values = new long[]
            {
                14, 7, 43, 37, 49, 3, 20, 26, 17, 29,
                40, 33, 21, 18, 9, 30, 45, 36, 12, 8
        };

        btree.setPageSize( 4 );
        // Inject some data
        for ( long value : values )
        {
            btree.insert( value, Long.toString( value ) );
            System.out.println( btree );
        }

        TupleCursor<Long, String> cursor = btree.browse();

        while ( cursor.hasNext() )
        {
            System.out.println( cursor.nextKey() );
        }
    }


    /**
     * Test the browse methods on a btree containing 500 random entries, with multiple values, and 
     * try to browse it.
     */
    @Test
    public void testBrowseBTreeMultipleValues() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        BTree<Long, Long> btreeLong = null;

        try
        {
            btreeLong = recordManager1.addBTree( "testLong", LongSerializer.INSTANCE, LongSerializer.INSTANCE, true );

            // Create a set of 500 values from 0 to 499, in a random order
            // (all the values are there, they are just shuffled)
            int nbKeys = 500;
            List<Long> values = new ArrayList<Long>( nbKeys );
            long[] randomVals = new long[nbKeys];
            Random r = new Random( System.currentTimeMillis() );

            // Create the data to inject into the btree
            for ( long i = 0L; i < nbKeys; i++ )
            {
                values.add( i );
            }

            for ( int i = 0; i < nbKeys; i++ )
            {
                int index = r.nextInt( nbKeys - i );
                randomVals[i] = values.get( index );
                values.remove( index );
            }

            long sum = 0L;

            for ( int i = 0; i < nbKeys; i++ )
            {
                sum += randomVals[i];
            }

            assertEquals( ( nbKeys * ( nbKeys - 1 ) ) / 2, sum );

            int nbValues = 9;

            // Inject the 500 keys, each of them with 10 values
            for ( int i = 0; i < nbKeys; i++ )
            {
                Long value = randomVals[i];

                for ( Long j = 0L; j < nbValues; j++ )
                {
                    btreeLong.insert( randomVals[i], value + j );
                }
            }

            long t0 = System.currentTimeMillis();

            // Now, browse the BTree fully, as many time as we have keys.
            // We always browse from a different position, we should cover all
            // the possible situations.
            for ( Long i = 0L; i < nbKeys; i++ )
            {
                // Create the cursor, positionning it before the key
                TupleCursor<Long, Long> cursor = btreeLong.browseFrom( i );

                assertTrue( cursor.hasNext() );
                Long expected = i;

                while ( cursor.hasNext() )
                {
                    for ( Long j = 0L; j < nbValues; j++ )
                    {
                        Tuple<Long, Long> tuple1 = cursor.next();

                        assertEquals( expected, tuple1.getKey() );
                        assertEquals( ( Long ) ( expected + j ), tuple1.getValue() );
                    }

                    expected++;
                }

                cursor.close();
            }
            long t1 = System.currentTimeMillis();

            System.out.println( "Browse Forward for " + nbValues + " = " + ( t1 - t0 ) );

            long t00 = System.currentTimeMillis();

            // Now, browse the BTree backward
            for ( Long i = nbKeys - 1L; i >= 0; i-- )
            {
                // Create the cursor
                TupleCursor<Long, Long> cursor = btreeLong.browseFrom( i );

                if ( i > 0 )
                {
                    assertTrue( cursor.hasPrev() );
                }

                Long expected = i;

                while ( cursor.hasPrev() )
                {
                    for ( Long j = Long.valueOf( nbValues - 1 ); j >= 0L; j-- )
                    {
                        Tuple<Long, Long> tuple1 = cursor.prev();

                        assertEquals( Long.valueOf( expected - 1L ), tuple1.getKey() );
                        assertEquals( ( Long ) ( expected - 1L + j ), tuple1.getValue() );
                    }

                    expected--;
                }

                cursor.close();
            }
            long t11 = System.currentTimeMillis();

            System.out.println( "Browe backward for " + nbValues + " = " + ( t11 - t00 ) );
        }
        finally
        {
            btreeLong.close();
        }
    }
}