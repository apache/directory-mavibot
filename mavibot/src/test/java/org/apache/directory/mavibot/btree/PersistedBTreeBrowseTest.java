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
import org.apache.directory.mavibot.btree.exception.CursorException;
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

    private RecordManager recordManager = null;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dataDir = null;


    /**
     * Create a BTree for this test
     */
    @Before
    public void startup() throws IOException
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        openRecordManagerAndBtree();

        try
        {
            // Create a new BTree which allows duplicate values
            btree = recordManager.addBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE, true );
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

        recordManager.close();
        assertTrue( recordManager.isContextOk() );
    }


    /**
     * Reload the BTree into a new record manager
     */
    private void openRecordManagerAndBtree()
    {
        try
        {
            if ( recordManager != null )
            {
                recordManager.close();
            }

            // Now, try to reload the file back
            recordManager = new RecordManager( dataDir.getAbsolutePath() );

            // load the last created btree
            if ( btree != null )
            {
                btree = recordManager.getManagedTree( btree.getName() );
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
     * @throws CursorException 
     */
    @Test
    public void testBrowseEmptyBTree() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
    public void testBrowseBTreeLeafNext() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        // Inject some data
        recordManager.beginTransaction();
        btree.insert( 1L, "1" );
        btree.insert( 4L, "4" );
        btree.insert( 2L, "2" );
        btree.insert( 3L, "3" );
        btree.insert( 5L, "5" );
        recordManager.commit();

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
    public void testBrowseBTreeLeafPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
    public void testBrowseBTreeLeafFirstLast() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
    public void testBrowseBTreeLeafNextPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
    public void testBrowseBTreeNodesNext() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
    public void testBrowseBTreeNodesPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
    public void testBrowseBTreeLeafNextDups1() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
    public void testBrowseBTreeLeafNextDupsN() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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


    //----------------------------------------------------------------------------------------
    // The TupleCursor.moveToNext/PrevNonDuplicateKey method tests
    //----------------------------------------------------------------------------------------
    /**
      * Test the TupleCursor.nextKey method on a btree containing nodes
      * with duplicate values.
      */
    @Test
    public void testNextKey() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
            Tuple<Long, String> tuple = cursor.next();

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
     * Test the TupleCursor.moveToPrevNonDuplicateKey method on a btree containing nodes
     * with duplicate values.
     */
    @Test
    public void testPrevKey() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        // Inject some data
        for ( long i = 1; i < 1000L; i++ )
        {
            recordManager.beginTransaction();
            btree.insert( i, Long.toString( i ) );
            recordManager.commit();
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
            Tuple<Long, String> tuple = cursor.prev();

            if ( i == 1L )
            {
                prev = false;
            }

            checkTuple( tuple, i, Long.toString( i ) );
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
    public void testAdd20Random() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
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
            System.out.println( cursor.next() );
        }
    }
}