/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */
package org.apache.directory.mavibot.btree;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.DuplicateValueNotAllowedException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * TODO BTreeDuplicateKeyTest.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PersistedBTreeDuplicateKeyTest
{
    private BTree<Long, String> btree = null;

    private RecordManager recordManager1 = null;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dataDir = null;


    @Before
    public void createBTree() throws IOException
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        openRecordManagerAndBtree();

        try
        {
            // Create a new BTree
            btree = recordManager1.addBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE,
                BTree.ALLOW_DUPLICATES );
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


    @Test
    public void testInsertNullValue() throws IOException, KeyNotFoundException
    {
        btree.insert( 1L, null );

        TupleCursor<Long, String> cursor = btree.browse();
        assertTrue( cursor.hasNext() );

        Tuple<Long, String> t = cursor.next();

        assertEquals( Long.valueOf( 1 ), t.getKey() );
        assertEquals( null, t.getValue() );

        cursor.close();

        btree.close();
    }


    @Test
    public void testBrowseEmptyTree() throws IOException, KeyNotFoundException, BTreeAlreadyManagedException
    {
        IntSerializer serializer = IntSerializer.INSTANCE;

        BTree<Integer, Integer> btree = BTreeFactory.createPersistedBTree( "master", serializer, serializer );
        
        // Inject the newly created BTree into teh recordManager
        recordManager1.manage( btree );

        TupleCursor<Integer, Integer> cursor = btree.browse();
        assertFalse( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );

        try
        {
            cursor.next();
            fail( "Should not reach here" );
        }
        catch ( NoSuchElementException e )
        {
            assertTrue( true );
        }

        try
        {
            cursor.prev();
            fail( "Should not reach here" );
        }
        catch ( NoSuchElementException e )
        {
            assertTrue( true );
        }

        cursor.close();
        btree.close();
    }


    @Test
    public void testDuplicateKey() throws IOException, KeyNotFoundException
    {
        btree.insert( 1L, "1" );
        btree.insert( 1L, "2" );

        TupleCursor<Long, String> cursor = btree.browse();
        assertTrue( cursor.hasNext() );

        Tuple<Long, String> t = cursor.next();

        assertEquals( Long.valueOf( 1 ), t.getKey() );
        assertEquals( "1", t.getValue() );

        assertTrue( cursor.hasNext() );

        t = cursor.next();

        assertEquals( Long.valueOf( 1 ), t.getKey() );
        assertEquals( "2", t.getValue() );

        assertFalse( cursor.hasNext() );

        // test backward move
        assertTrue( cursor.hasPrev() );

        t = cursor.prev();

        assertEquals( Long.valueOf( 1 ), t.getKey() );
        assertEquals( "1", t.getValue() );

        assertFalse( cursor.hasPrev() );

        // again forward
        assertTrue( cursor.hasNext() );

        t = cursor.next();

        assertEquals( Long.valueOf( 1 ), t.getKey() );
        assertEquals( "2", t.getValue() );

        assertFalse( cursor.hasNext() );

        cursor.close();
        btree.close();
    }


    @Test
    public void testGetDuplicateKey() throws Exception
    {
        String retVal = btree.insert( 1L, "1" );
        assertNull( retVal );

        retVal = btree.insert( 1L, "2" );
        assertNull( retVal );

        // check the return value when an existing value is added again
        retVal = btree.insert( 1L, "2" );
        assertEquals( "2", retVal );

        assertEquals( "1", btree.get( 1L ) );
        assertTrue( btree.contains( 1L, "1" ) );
        assertTrue( btree.contains( 1L, "2" ) );

        assertFalse( btree.contains( 1L, "0" ) );
        assertFalse( btree.contains( 0L, "1" ) );
        assertFalse( btree.contains( 0L, "0" ) );
        assertFalse( btree.contains( null, "0" ) );
        assertFalse( btree.contains( 0L, null ) );
        assertFalse( btree.contains( null, null ) );
        btree.close();
    }


    @Test
    public void testRemoveDuplicateKey() throws Exception
    {
        btree.insert( 1L, "1" );
        btree.insert( 1L, "2" );

        assertEquals( 2, btree.getNbElems() );

        Tuple<Long, String> t = btree.delete( 1L, "1" );
        assertEquals( Long.valueOf( 1 ), t.getKey() );
        assertEquals( "1", t.getValue() );

        assertEquals( 1l, btree.getNbElems() );

        t = btree.delete( 1L, "2" );
        assertEquals( Long.valueOf( 1 ), t.getKey() );
        assertEquals( "2", t.getValue() );

        assertEquals( 0l, btree.getNbElems() );

        t = btree.delete( 1L, "2" );
        assertNull( t );
        btree.close();
    }


    @Test
    public void testFullPage() throws Exception
    {
        int i = 7;
        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            for ( int k = 0; k < i; k++ )
            {
                String val = ch + Integer.toString( k );
                btree.insert( Long.valueOf( ch ), val );
            }
        }

        TupleCursor<Long, String> cursor = btree.browse();

        char ch = 'a';
        int k = 0;

        while ( cursor.hasNext() )
        {
            Tuple<Long, String> t = cursor.next();
            assertEquals( Long.valueOf( ch ), t.getKey() );
            k++;

            if ( ( k % i ) == 0 )
            {
                ch++;
            }
        }

        assertEquals( ( 'z' + 1 ), ch );

        ch = 'z';
        cursor.afterLast();

        while ( cursor.hasPrev() )
        {
            Tuple<Long, String> t = cursor.prev();
            assertEquals( Long.valueOf( ch ), t.getKey() );
            k--;

            if ( ( k % i ) == 0 )
            {
                ch--;
            }
        }

        assertEquals( ( 'a' - 1 ), ch );
        cursor.close();
    }


    @Test
    public void testMoveFirst() throws Exception
    {
        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            String val = Character.toString( ch );
            btree.insert( Long.valueOf( ch ), val );
        }

        assertEquals( 26, btree.getNbElems() );

        // add one more value for 'a'
        btree.insert( Long.valueOf( 'a' ), "val" );

        assertEquals( 27, btree.getNbElems() );

        // Start from c : we should have only 24 values
        TupleCursor<Long, String> cursor = btree.browseFrom( Long.valueOf( 'c' ) );

        int i = 0;

        while ( cursor.hasNext() )
        {
            Tuple<Long, String> tuple = cursor.next();
            assertNotNull( tuple );
            i++;
        }

        assertEquals( 24, i );

        // now move the cursor first
        cursor.beforeFirst();
        assertTrue( cursor.hasNext() );
        Tuple<Long, String> tuple = cursor.next();

        // We should be on the first position
        assertEquals( Long.valueOf( 'a' ), tuple.getKey() );

        // Count the number of element after the first one, we should have 26 only
        i = 0;

        while ( cursor.hasNext() )
        {
            tuple = cursor.next();
            assertNotNull( tuple );
            i++;
        }

        assertEquals( 26, i );

        cursor.close();

        // Rebrowse
        cursor = btree.browse();

        i = 0;

        while ( cursor.hasNext() )
        {
            assertNotNull( cursor.next() );
            i++;
        }

        // again, we should see 27 elements
        assertEquals( 27, i );

        // now move the cursor first, but move forward the keys
        cursor.beforeFirst();
        assertTrue( cursor.hasNextKey() );
        assertEquals( Long.valueOf( 'a' ), cursor.nextKey().getKey() );

        i = 0;

        while ( cursor.hasNextKey() )
        {
            tuple = cursor.nextKey();
            long key = tuple.getKey();
            assertNotNull( key );
            i++;
        }

        // We should have 25 keys only, as we just moved forward the first one
        assertEquals( 25, i );
    }


    @Test
    public void testMoveLast() throws Exception
    {
        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            String val = Character.toString( ch );
            btree.insert( Long.valueOf( ch ), val );
        }

        assertEquals( 26, btree.getNbElems() );

        // add one more value for 'z'
        btree.insert( Long.valueOf( 'z' ), "val" );

        assertEquals( 27, btree.getNbElems() );

        // Start from x : we should have only 23 values
        TupleCursor<Long, String> cursor = btree.browseFrom( Long.valueOf( 'x' ) );

        int i = 0;

        while ( cursor.hasPrev() )
        {
            Tuple<Long, String> tuple = cursor.prev();
            assertNotNull( tuple );
            i++;
        }

        assertEquals( 23, i );

        // now move the cursor to the last element
        cursor.afterLast();
        assertTrue( cursor.hasPrev() );
        Tuple<Long, String> tuple = cursor.prev();

        // We should be on the last position
        assertEquals( Long.valueOf( 'z' ), tuple.getKey() );

        // Count the number of element before the last one, we should have 26
        i = 0;

        while ( cursor.hasPrev() )
        {
            tuple = cursor.prev();
            assertNotNull( tuple );
            i++;
        }

        assertEquals( 26, i );

        cursor.close();

        // Rebrowse
        cursor = btree.browse();
        cursor.afterLast();

        i = 0;

        while ( cursor.hasPrev() )
        {
            assertNotNull( cursor.prev() );
            i++;
        }

        // again, we should see 27 elements
        assertEquals( 27, i );

        // now move the cursor first, but move backward the keys
        cursor.afterLast();
        assertTrue( cursor.hasPrevKey() );
        assertEquals( Long.valueOf( 'z' ), cursor.prevKey().getKey() );

        i = 0;

        while ( cursor.hasPrevKey() )
        {
            tuple = cursor.prevKey();
            long key = tuple.getKey();
            assertNotNull( key );
            i++;
        }

        // We should have 25 keys only, as we just moved forward the first one
        assertEquals( 25, i );
    }


    @Test(expected = NoSuchElementException.class)
    public void testMoveLast2() throws Exception
    {
        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            btree.insert( Long.valueOf( ch ), UUID.randomUUID().toString() );
        }

        btree.insert( Long.valueOf( 'z' ), UUID.randomUUID().toString() );

        TupleCursor<Long, String> cursor = btree.browseFrom( Long.valueOf( 'c' ) );
        cursor.afterLast();

        assertFalse( cursor.hasNext() );
        assertTrue( cursor.hasPrev() );
        assertEquals( Long.valueOf( 'z' ), cursor.prev().getKey() );
        // the key, 'z', has two values
        assertEquals( Long.valueOf( 'z' ), cursor.prev().getKey() );
        assertEquals( Long.valueOf( 'y' ), cursor.prev().getKey() );

        cursor.beforeFirst();
        assertEquals( Long.valueOf( 'a' ), cursor.next().getKey() );

        cursor.afterLast();
        assertFalse( cursor.hasNext() );
        // make sure it throws NoSuchElementException
        cursor.next();
    }


    @Test(expected = NoSuchElementException.class)
    public void testNextPrevKey() throws Exception
    {
        int i = 7;

        // Insert keys from a to z with 7 values for each key
        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            for ( int k = 0; k < i; k++ )
            {
                btree.insert( Long.valueOf( ch ), String.valueOf( k ) );
            }
        }

        TupleCursor<Long, String> cursor = btree.browse();

        assertTrue( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );

        for ( int k = 0; k < 2; k++ )
        {
            assertEquals( Long.valueOf( 'a' ), cursor.next().getKey() );
        }

        assertEquals( Long.valueOf( 'a' ), cursor.next().getKey() );

        Tuple<Long, String> tuple = cursor.nextKey();

        assertEquals( Long.valueOf( 'b' ), tuple.getKey() );

        for ( char ch = 'b'; ch < 'z'; ch++ )
        {
            assertEquals( Long.valueOf( ch ), cursor.next().getKey() );
            tuple = cursor.nextKey();
            char t = ch;
            assertEquals( Long.valueOf( ++t ), tuple.getKey() );
        }

        for ( int k = 0; k < i; k++ )
        {
            assertEquals( Long.valueOf( 'z' ), cursor.next().getKey() );
        }

        assertFalse( cursor.hasNextKey() );
        assertTrue( cursor.hasPrevKey() );
        tuple = cursor.prev();
        assertEquals( Long.valueOf( 'z' ), tuple.getKey() );
        assertEquals( "6", tuple.getValue() );

        for ( char ch = 'z'; ch > 'a'; ch-- )
        {
            char t = ch;
            t--;

            assertEquals( Long.valueOf( ch ), cursor.prev().getKey() );

            tuple = cursor.prevKey();

            assertEquals( Long.valueOf( t ), tuple.getKey() );
        }

        for ( int k = 5; k >= 0; k-- )
        {
            tuple = cursor.prev();
            assertEquals( Long.valueOf( 'a' ), tuple.getKey() );
            assertEquals( String.valueOf( k ), tuple.getValue() );
        }

        assertTrue( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );
        tuple = cursor.next();
        assertEquals( Long.valueOf( 'a' ), tuple.getKey() );
        assertEquals( "0", tuple.getValue() );

        cursor.close();

        cursor = btree.browseFrom( Long.valueOf( 'y' ) );
        tuple = cursor.prevKey();
        assertNotNull( tuple );
        assertEquals( Long.valueOf( 'y' ), tuple.getKey() );
        assertEquals( "6", tuple.getValue() );
        cursor.close();

        cursor = btree.browse();
        cursor.beforeFirst();
        assertFalse( cursor.hasPrev() );
        // make sure it throws NoSuchElementException
        cursor.prev();
    }


    /**
     * Test for moving between two leaves. When moveToNextNonDuplicateKey is called
     * and cursor is on the last element of the current leaf.
     *
     * @throws Exception
     */
    @Test
    public void testMoveToNextAndPrevWithPageBoundaries() throws Exception
    {
        int i = 32;
        for ( int k = 0; k < i; k++ )
        {
            btree.insert( ( long ) k, Long.toString( k ) );
        }

        // 15 is the last element of the first leaf
        // Check that we correctly jump to the next page
        TupleCursor<Long, String> cursor = btree.browseFrom( 15L );
        Tuple<Long, String> tuple = cursor.nextKey();

        assertNotNull( tuple );
        assertEquals( Long.valueOf( 16 ), tuple.getKey() );
        assertEquals( "16", tuple.getValue() );
        cursor.close();

        // Do the same check, on the revert side : moving backward
        cursor = btree.browseFrom( 16L );
        tuple = cursor.prevKey();

        assertNotNull( tuple );
        assertEquals( Long.valueOf( 15 ), tuple.getKey() );
        assertEquals( "15", tuple.getValue() );
        cursor.close();

        // Now do a next followed by a prev on the boundary of 2 pages
        cursor = btree.browseFrom( 16L );
        tuple = cursor.prevKey();

        assertNotNull( tuple );
        assertEquals( Long.valueOf( 15 ), tuple.getKey() );
        assertEquals( "15", tuple.getValue() );

        // Move next, we should be back to the initial value
        assertTrue( cursor.hasNext() );
        tuple = cursor.next();
        assertEquals( Long.valueOf( 16 ), tuple.getKey() );
        assertEquals( "16", tuple.getValue() );
        cursor.close();

        // test the extremes of the BTree instead of that of leaves
        cursor = btree.browseFrom( 30L );
        tuple = cursor.nextKey();
        assertFalse( cursor.hasNext() );
        assertTrue( cursor.hasPrev() );

        assertEquals( Long.valueOf( 31 ), tuple.getKey() );
        assertEquals( "31", tuple.getValue() );
        cursor.close();

        cursor = btree.browse();
        assertTrue( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );

        tuple = cursor.nextKey();
        assertEquals( Long.valueOf( 0 ), tuple.getKey() );
        assertEquals( "0", tuple.getValue() );
        cursor.close();
    }


    @Test
    public void testNextAfterPrev() throws Exception
    {
        int i = 32;

        for ( int k = 0; k < i; k++ )
        {
            btree.insert( ( long ) k, String.valueOf( k ) );
        }

        // 15 is the last element of the first leaf
        TupleCursor<Long, String> cursor = btree.browseFrom( 16L );

        assertTrue( cursor.hasNext() );
        Tuple<Long, String> tuple = cursor.next();
        assertEquals( Long.valueOf( 16 ), tuple.getKey() );
        assertEquals( "16", tuple.getValue() );

        assertTrue( cursor.hasPrev() );
        tuple = cursor.prev();
        assertEquals( Long.valueOf( 15 ), tuple.getKey() );
        assertEquals( "15", tuple.getValue() );

        assertTrue( cursor.hasNext() );
        tuple = cursor.next();
        assertEquals( Long.valueOf( 16 ), tuple.getKey() );
        assertEquals( "16", tuple.getValue() );
        cursor.close();

    }


    /**
     * Test for moving after a key and traversing backwards.
     *
     * @throws Exception
     */
    @Test
    public void testMoveToNextAndTraverseBackward() throws Exception
    {
        int i = 5;

        for ( int k = 0; k < i; k++ )
        {
            btree.insert( ( long ) k, Long.toString( k ) );
        }

        // 4 is the last element in the tree
        TupleCursor<Long, String> cursor = btree.browseFrom( 4L );
        cursor.nextKey();

        long currentKey = 4L;

        while ( cursor.hasPrev() )
        {
            assertEquals( Long.valueOf( currentKey ), cursor.prev().getKey() );
            currentKey--;
        }

        cursor.close();
    }


    /**
     * Test for moving after a key and traversing backwards.
     *
     * @throws Exception
     */
    @Test
    public void testMoveToPrevAndTraverseForward() throws Exception
    {
        int i = 5;

        for ( int k = 0; k < i; k++ )
        {
            btree.insert( ( long ) k, Long.toString( k ) );
        }

        // 4 is the last element in the tree
        TupleCursor<Long, String> cursor = btree.browseFrom( 0L );

        long currentKey = 0L;

        while ( cursor.hasNext() )
        {
            assertEquals( Long.valueOf( currentKey ), cursor.next().getKey() );
            currentKey++;
        }

        cursor.close();
    }

    
    @Test
    public void testFindLeftAndRightMosetInSubBTree() throws Exception
    {
        PersistedBTreeConfiguration<Integer, Integer> config = new PersistedBTreeConfiguration<Integer, Integer>();

        config.setName( "test" );
        config.setKeySerializer( IntSerializer.INSTANCE );
        config.setValueSerializer( IntSerializer.INSTANCE );
        config.setAllowDuplicates( false );
        config.setBtreeType( BTreeTypeEnum.PERSISTED_SUB );

        PersistedBTree<Integer, Integer> subBtree = new PersistedBTree<Integer, Integer>( config );
        
        subBtree.setRecordManager( recordManager1 );
        
        subBtree.insert( 1, 1 ); // the values will be discarded in this BTree type
        subBtree.insert( 2, 2 );
        subBtree.insert( 3, 3 );
        subBtree.insert( 4, 4 );
        subBtree.insert( 5, 5 );
        
        Tuple<Integer, Integer> t = subBtree.getRootPage().findLeftMost();
        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        
        t = subBtree.getRootPage().findRightMost();
        assertEquals( Integer.valueOf( 5 ), t.getKey() );
    }

    /**
     * Test that a BTree which forbid duplicate values does not accept them
     */
    @Test(expected = DuplicateValueNotAllowedException.class)
    @Ignore("this condition is removed")
    public void testBTreeForbidDups() throws IOException, BTreeAlreadyManagedException
    {
        BTree<Long, String> singleValueBtree = recordManager1.addBTree( "test2", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE, BTree.FORBID_DUPLICATES );

        for ( long i = 0; i < 64; i++ )
        {
            singleValueBtree.insert( i, Long.toString( i ) );
        }

        try
        {
            singleValueBtree.insert( 18L, "Duplicate" );
            fail();
        }
        finally
        {
            singleValueBtree.close();
        }
    }
}
