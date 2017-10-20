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
public class BTreeBrowseTest
{
    private BTree<Long, String> btree = null;

    private RecordManager recordManager = null;
    
    private Transaction transaction;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dataDir = null;


    /**
     * Create a BTree for this test
     */
    @Before
    public void startup() throws Exception
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        openRecordManagerAndBtree();

        // Create a new BTree
        try ( WriteTransaction transaction = recordManager.beginWriteTransaction() )
        {
            btree = recordManager.addBTree( transaction, "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE );
        }
        catch ( Exception e )
        {
            transaction.abort();
            throw new RuntimeException( e );
        }

        try ( Transaction transaction = recordManager.beginReadTransaction() )
        {
            //MavibotInspector.dumpInfos( recordManager, transaction.getRecordManagerHeader() );
        }
    }


    @After
    public void cleanup() throws IOException
    {
        dataDir = new File( System.getProperty( "java.io.tmpdir" ) + "/recordman" );

        if ( dataDir.exists() )
        {
            FileUtils.deleteDirectory( dataDir );
        }

        recordManager.close();
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

            try ( Transaction transaction = recordManager.beginReadTransaction() )
            {
                //MavibotInspector.dumpInfos( recordManager, transaction.getRecordManagerHeader() );
            }

            // load the last created btree
            try ( Transaction transaction = recordManager.beginReadTransaction() )
            {
                if ( btree != null )
                {
                    btree = transaction.getBTree( btree.getName() );
                }
            }
            catch ( Exception e )
            {
                transaction.abort();
                throw e;
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
     * 
     * @throws KeyNotFoundException 
     * @throws CursorException 
     */
    @Test
    public void testBrowseEmptyBTree() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        try ( Transaction transaction  = recordManager.beginReadTransaction() )
        {
            btree = transaction.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( transaction );
    
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
    
            assertEquals( 2L, cursor.getRevision() );
        }
    }


    /**
     * Test the browse methods on a btree containing just a leaf
     */
    @Test
    public void testBrowseBTreeLeafNext() throws Exception
    {
        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            btree.insert( writeTxn, 1L, "1" );
            btree.insert( writeTxn, 4L, "4" );
            btree.insert( writeTxn, 2L, "2" );
            btree.insert( writeTxn, 3L, "3" );
            btree.insert( writeTxn, 5L, "5" );
        }

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
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
    }


    /**
     * Test the browse methods on a btree containing just a leaf
     */
    @Test
    public void testBrowseBTreeLeafPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            btree.insert( writeTxn, 1L, "1" );
            btree.insert( writeTxn, 4L, "4" );
            btree.insert( writeTxn, 2L, "2" );
            btree.insert( writeTxn, 3L, "3" );
            btree.insert( writeTxn, 5L, "5" );
        }

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
            // Move backward
            cursor.afterLast();
    
            checkPrev( cursor, 5L, "5", false, true );
            checkPrev( cursor, 4L, "4", true, true );
            checkPrev( cursor, 3L, "3", true, true );
            checkPrev( cursor, 2L, "2", true, true );
            checkPrev( cursor, 1L, "1", true, false );
        }
    }


    /**
     * Test the browse methods on a btree containing just a leaf and see if we can
     * move at the end or at the beginning
     */
    @Test
    public void testBrowseBTreeLeafFirstLast() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            btree.insert( writeTxn, 1L, "1" );
            btree.insert( writeTxn, 4L, "4" );
            btree.insert( writeTxn, 2L, "2" );
            btree.insert( writeTxn, 3L, "3" );
            btree.insert( writeTxn, 5L, "5" );
        }

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
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
    }


    /**
     * Test the browse methods on a btree containing just a leaf and see if we can
     * move back and forth
     */
    @Test
    public void testBrowseBTreeLeafNextPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            btree.insert( writeTxn, 1L, "1" );
            btree.insert( writeTxn, 4L, "4" );
            btree.insert( writeTxn, 2L, "2" );
            btree.insert( writeTxn, 3L, "3" );
            btree.insert( writeTxn, 5L, "5" );
        }

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
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
    }
    
    
    /**
     * Shuffle a set of longs
     * @param nbElems
     * @return
     */
    private void shuffle( long[] values )
    {
        Random r = new Random( System.nanoTime() );
        
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = i;
        }
        
        for ( int i = 0; i < values.length*10; i++ )
        {
            int i1 = r.nextInt( values.length ) ;
            int i2 = r.nextInt( values.length ) ;
            
            long tmp = values[i1];
            values[i1] = values[i2];
            values[i2] = tmp;
        }
    }
    
    
    @Test
    @Ignore
    public void testPerf()
    {
        Random r = new Random( System.nanoTime() );
        long[] values = new long[24];
        
        for ( int i = 0; i < 24; )
        {
            long v = r.nextLong();
            boolean found = false;
            
            for ( long old : values )
            {
                if ( old == v )
                {
                    found = true;
                    break;
                }
            }
            
            if ( !found )
            {
                values[i] = v;
                i++;
            }
        }
        
        
        long t0 = System.currentTimeMillis();
        
        for ( int i = 0; i < 50000000; i++ )
        {
            //TreeMap<Long, Long> treeMap = new TreeMap<>();
            List<Long> list = new ArrayList<>();

            for ( long v : values )
            {
                //treeMap.put( v, v );
                if ( !list.contains( v ) )
                {
                    list.add( v );
                }
            }
        }
        
        long t1 = System.currentTimeMillis();
        
        System.out.println( "Delta = " + ( t1 - t0) );
    }


    /**
     * Test the browse methods on a btree containing many nodes
     */
    @Test
    public void testBrowseBTreeNodesNext() throws Exception
    {
        // Inject some data
        long increment = 100L;
        long nbRound = 100_000L;
        long t0 = System.currentTimeMillis();
        
        for ( long i = 0; i < nbRound/increment; i++ )
        {
            //System.out.println( "\nInserting " + i + " in the tree ---->" );
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );
                
                for ( long j = 0; j < increment; j++ )
                {
                    long val = i*increment + j;
                    //System.out.println( "Injecting " + val );
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.insert( writeTxn, val, Long.toString( val ) );
                }
            }
        }
        long t1 = System.currentTimeMillis();
        
        System.out.println( "Delta add    : " + ( t1 - t0 ) );
        System.out.println( "File name    : " + dataDir );
        System.out.println( "Nb cache hits : " + recordManager.nbCacheHits.get() );
        System.out.println( "Nb cache misses : " + recordManager.nbCacheMisses.get() );
        
        //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );

            // Move forward
            cursor.afterLast();
            cursor.beforeFirst();
    
            assertFalse( cursor.hasPrev() );
            assertTrue( cursor.hasNext() );
    
            t0 = System.currentTimeMillis();
            
            for ( int loop = 0; loop < 1_000L; loop++ )
            {
                cursor.beforeFirst();

                //assertFalse( cursor.hasPrev() );
                //assertTrue( cursor.hasNext() );
                //Tuple<Long, String> tuple = cursor.next();
                checkNext( cursor, 0L, "0", true, false );
                
                assertFalse( cursor.hasPrev() );
                assertTrue( cursor.hasNext() );
        
                for ( long i = 1L; i < nbRound - 1; i++ )
                {
                    //assertTrue( cursor.hasNext() );
                    //tuple = cursor.next();
                    //assertTrue( cursor.hasPrev() );
                    checkNext( cursor, i, Long.toString( i ), true, true );
                }
        
                assertTrue( cursor.hasNext() );
                //tuple = cursor.next();
                //assertFalse( cursor.hasNext() );
                checkNext( cursor, nbRound - 1L, Long.toString( nbRound - 1L ), false, true );
            }
            
            t1 = System.currentTimeMillis();
            
            System.out.println( "Delta browse : " + ( t1 - t0 ) );
            System.out.println( "Nb cache hits : " + recordManager.nbCacheHits.get() );
            System.out.println( "Nb cache misses : " + recordManager.nbCacheMisses.get() );
        }
    }
    
    
    @Test
    public void testAddRandom() throws Exception
    {
        // Inject some data
        long increment = 1_000L;
        long nbRound = 100_000L;
        long t0 = System.currentTimeMillis();

        long[] values = new long[(int)nbRound];
        
        for ( long i = 0; i < nbRound; i++ )
        {
            values[(int)i] = i;
        }
        
        shuffle( values );

        for ( long i = 0; i < nbRound/increment; i++ )
        {
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    long val = values[( int )( i * increment + j )];
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.insert( writeTxn, val, Long.toString( val ) );
                }
            }
        }

        long t1 = System.currentTimeMillis();
        
        System.out.println( "Delta for " + nbRound + " : " + ( t1 - t0 ) );
        
        //MavibotInspector.check( recordManager );

        int counter = 0;

        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
            while ( cursor.hasNext() )
            {
                cursor.next();
                counter++;
            }
        }
        
        assertEquals( nbRound, counter );
        
        // Now delete the elements
        shuffle( values );
        
        long tt0 = System.currentTimeMillis();

        increment = 1L;
        
        for ( long i = 0; i < nbRound/increment; i++ )
        {
            //System.out.println( "\nInserting " + i + " in the tree ---->" );
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    int elemNb = ( int )( i * increment + j );
                    long val = values[elemNb];
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.delete( writeTxn, val );
                }
            }
        }

        long tt1 = System.currentTimeMillis();
        
        System.out.println( "Delta for " + nbRound + " : " + ( tt1 - tt0 ) );
    }
    
    
    @Test
    public void testReopen() throws Exception
    {
        // Inject some data
        long increment = 1L;
        long nbRound = 100L;
        long t0 = System.currentTimeMillis();

        long[] values = new long[(int)nbRound];
        
        for ( long i = 0; i < nbRound; i++ )
        {
            values[(int)i] = i;
        }
        
        //shuffle( values );
        //printValues( values );
        values = new long[]
            {
                38, 40, 61, 34, 9, 59, 78, 30, 42, 76, 84, 52, 37, 58, 88, 27, 24, 22, 33, 39, 
                74, 44, 65, 45, 70, 98, 64, 99, 31, 19, 95, 57, 35, 90, 68, 1, 12, 69, 77, 73, 83, 
                6, 96, 80, 7, 23, 43, 85, 36, 48, 32, 66, 53, 87, 16, 10, 15, 13, 5, 91, 54, 71, 
                92, 72, 82, 63, 97, 62, 26, 56, 41, 18, 11, 3, 21, 75, 46, 67, 93, 2, 28, 29, 25, 
                14, 0, 94, 4, 81, 20, 55, 8, 79, 51, 50, 60, 86, 89, 47, 49, 17
            };

        for ( long i = 0; i < nbRound/increment; i++ )
        {
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    int pos = ( int )( i * increment + j );
                    long val = values[pos];
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    System.out.println( "Adding value " + pos );
                    btree.insert( writeTxn, val, Long.toString( val ) );
                }
            }
        }

        long t1 = System.currentTimeMillis();
        
        System.out.println( "Delta for " + nbRound + " : " + ( t1 - t0 ) );
        
        recordManager.close();
        recordManager = new RecordManager( dataDir.getAbsolutePath() );
        
        int counter = 0;

        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
            while ( cursor.hasNext() )
            {
                cursor.next();
                counter++;
            }
        }
        
        assertEquals( nbRound, counter );
        
        // Now delete the elements
        //shuffle( values );
        //printValues( values );
        values = new long[]
            {
                13, 42, 95, 59, 96, 62, 39, 90, 32, 5, 20, 7, 37, 63, 25, 17, 23, 97, 4, 16, 
                53, 69, 89, 9, 80, 71, 19, 22, 31, 33, 12, 29, 34, 65, 6, 57, 15, 18, 24, 93, 38, 
                92, 83, 98, 28, 0, 21, 94, 8, 64, 14, 40, 48, 26, 41, 66, 81, 52, 82, 88, 68, 74, 
                55, 84, 54, 79, 61, 87, 67, 99, 36, 47, 1, 86, 58, 50, 51, 75, 49, 56, 27, 78, 60, 
                45, 30, 77, 46, 73, 35, 43, 91, 85, 70, 44, 11, 10, 3, 72, 2, 76
            };
        
        increment = 1L;
        
        for ( long i = 0; i < nbRound/increment; i++ )
        {
            //System.out.println( "\nInserting " + i + " in the tree ---->" );
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    int elemNb = ( int )( i * increment + j );
                    long val = values[elemNb];
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    System.out.println( "Removing value " + val );
                    btree.delete( writeTxn, val );
                }
            }
        }
    }

    
    private void printValues( long[] values )
    {
        boolean isFirst = true;
        int nbVal = 0;
        StringBuilder sb = new StringBuilder();
        
        for ( long val : values )
        {
            if ( isFirst )
            {
                sb.append( "    long[] values = new long[]\n    {\n        " );
                isFirst = false;
            }
            else
            {
                sb.append( ", " );
            }
            
            if ( nbVal == 20 )
            {
                nbVal = 0;
                sb.append( "\n        " );
            }
            else
            {
                nbVal++;
            }
            
            sb.append( val );
        }
        
        sb.append( "\n    };" );
        
        System.out.println(  sb.toString() );
    }
    
    
    @Test
    public void testDeleteDebug() throws Exception
    {
        btree.setPageNbElem( 4 );

        // Inject some data
        long increment = 20L;
        long nbRound = 1000L;
        long t0 = System.currentTimeMillis();

        long[] values = new long[]
            {
                962, 598, 975, 719, 246, 498, 482, 326, 205, 777, 73, 12, 847, 428, 934, 533, 149, 20, 334, 686, 
                139, 40, 640, 438, 838, 999, 827, 560, 658, 868, 798, 55, 112, 315, 435, 153, 451, 280, 570, 769, 665, 
                821, 86, 780, 587, 557, 900, 59, 884, 657, 240, 324, 448, 287, 92, 693, 169, 439, 959, 677, 254, 23, 
                212, 22, 387, 765, 596, 915, 486, 573, 219, 339, 304, 16, 187, 823, 933, 423, 109, 113, 741, 436, 420, 
                290, 250, 926, 181, 150, 321, 558, 67, 628, 432, 90, 745, 302, 586, 742, 521, 910, 978, 314, 322, 568, 
                295, 221, 178, 316, 360, 552, 747, 309, 578, 588, 8, 503, 244, 468, 261, 269, 846, 170, 367, 815, 459, 
                750, 607, 662, 399, 427, 79, 496, 7, 15, 203, 717, 264, 664, 799, 887, 687, 278, 143, 793, 383, 623, 
                553, 325, 446, 536, 335, 692, 297, 257, 273, 495, 699, 499, 60, 308, 141, 58, 951, 976, 106, 480, 901, 
                947, 104, 224, 861, 146, 694, 466, 547, 229, 525, 133, 262, 748, 487, 801, 283, 878, 505, 718, 906, 230, 
                709, 164, 182, 862, 49, 582, 831, 300, 2, 614, 527, 621, 402, 194, 585, 253, 667, 215, 108, 963, 886, 
                648, 161, 350, 924, 391, 422, 63, 775, 405, 740, 381, 601, 98, 970, 948, 688, 500, 53, 344, 35, 803, 
                771, 61, 235, 708, 892, 0, 42, 701, 208, 863, 356, 713, 802, 158, 920, 888, 24, 974, 540, 368, 794, 
                770, 492, 561, 370, 29, 622, 17, 619, 893, 983, 817, 567, 345, 767, 818, 564, 995, 929, 199, 425, 174, 
                816, 424, 992, 515, 569, 602, 218, 659, 781, 216, 225, 790, 416, 213, 627, 103, 551, 579, 954, 372, 171, 
                398, 768, 75, 909, 833, 33, 822, 969, 679, 882, 695, 167, 649, 592, 964, 263, 788, 864, 41, 227, 876, 
                896, 875, 268, 691, 609, 418, 319, 606, 421, 732, 68, 542, 479, 464, 36, 826, 122, 668, 493, 944, 537, 
                96, 508, 147, 660, 516, 475, 941, 37, 298, 461, 114, 891, 13, 841, 584, 997, 766, 674, 313, 656, 160, 
                787, 384, 172, 689, 65, 559, 762, 286, 785, 837, 100, 931, 867, 120, 949, 942, 129, 4, 115, 359, 755, 
                354, 987, 39, 676, 724, 624, 11, 365, 458, 705, 245, 825, 782, 895, 911, 144, 866, 443, 835, 946, 156, 
                773, 21, 714, 54, 419, 756, 470, 743, 338, 341, 204, 734, 238, 357, 807, 267, 855, 47, 433, 460, 544, 
                604, 829, 548, 943, 550, 532, 990, 353, 671, 131, 471, 871, 340, 201, 501, 51, 452, 562, 510, 196, 851, 
                883, 965, 28, 337, 727, 77, 574, 507, 481, 546, 200, 539, 565, 362, 912, 43, 778, 595, 34, 666, 409, 
                27, 66, 491, 814, 226, 389, 608, 291, 407, 472, 454, 673, 168, 3, 638, 366, 210, 277, 397, 519, 469, 
                95, 758, 956, 563, 889, 123, 738, 902, 198, 710, 981, 955, 760, 97, 166, 819, 260, 850, 957, 351, 549, 
                434, 632, 57, 809, 32, 299, 259, 1, 581, 848, 860, 396, 828, 744, 274, 746, 414, 214, 327, 26, 99, 
                71, 415, 14, 494, 683, 786, 38, 31, 986, 820, 64, 348, 395, 626, 634, 9, 852, 531, 307, 543, 958, 
                455, 132, 806, 594, 330, 994, 935, 752, 968, 761, 504, 430, 288, 796, 897, 647, 282, 918, 706, 233, 730, 
                332, 117, 25, 485, 757, 162, 642, 824, 93, 83, 333, 222, 441, 52, 363, 857, 928, 529, 232, 84, 776, 
                843, 483, 700, 711, 456, 523, 591, 655, 853, 251, 369, 960, 996, 654, 101, 393, 797, 50, 859, 292, 795, 
                406, 590, 932, 85, 530, 352, 145, 725, 922, 927, 388, 275, 937, 961, 175, 513, 284, 885, 749, 124, 753, 
                437, 804, 832, 840, 111, 258, 242, 630, 121, 78, 739, 764, 572, 107, 830, 779, 94, 509, 808, 890, 159, 
                684, 707, 457, 236, 980, 880, 940, 88, 600, 759, 192, 331, 952, 731, 239, 184, 899, 663, 317, 207, 318, 
                979, 392, 234, 462, 189, 580, 105, 763, 478, 18, 939, 467, 364, 904, 643, 938, 620, 858, 998, 811, 417, 
                813, 720, 135, 445, 10, 46, 783, 228, 898, 715, 408, 873, 612, 48, 571, 231, 792, 905, 675, 680, 704, 
                252, 526, 358, 716, 615, 426, 394, 599, 916, 248, 220, 751, 126, 678, 522, 812, 431, 726, 839, 349, 346, 
                157, 134, 87, 844, 62, 74, 894, 512, 490, 404, 528, 281, 69, 712, 836, 834, 279, 82, 474, 800, 271, 
                945, 610, 669, 791, 917, 444, 217, 524, 382, 541, 342, 617, 410, 152, 737, 136, 163, 930, 361, 907, 636, 
                877, 518, 127, 403, 223, 130, 772, 682, 237, 914, 400, 881, 872, 633, 685, 312, 211, 729, 449, 310, 19, 
                936, 188, 272, 266, 118, 625, 611, 465, 810, 142, 125, 151, 735, 256, 670, 593, 650, 328, 440, 984, 870, 
                378, 520, 566, 723, 556, 616, 255, 646, 545, 514, 371, 320, 605, 347, 249, 476, 165, 950, 702, 138, 506, 
                966, 265, 576, 185, 56, 774, 698, 805, 177, 629, 190, 305, 390, 186, 323, 644, 183, 76, 635, 209, 80, 
                583, 869, 923, 991, 789, 375, 577, 155, 639, 116, 988, 502, 128, 672, 645, 270, 285, 180, 343, 971, 385, 
                511, 247, 137, 110, 119, 989, 442, 736, 879, 429, 603, 294, 697, 589, 450, 6, 690, 179, 45, 72, 473, 
                453, 311, 243, 463, 754, 661, 874, 30, 534, 303, 919, 631, 681, 376, 722, 488, 972, 993, 489, 925, 447, 
                651, 355, 842, 154, 81, 575, 733, 921, 903, 703, 148, 484, 653, 379, 70, 554, 967, 652, 973, 301, 721, 
                597, 276, 306, 380, 977, 477, 641, 197, 206, 377, 535, 497, 696, 293, 373, 908, 845, 191, 618, 555, 538, 
                728, 784, 411, 982, 140, 289, 5, 176, 413, 517, 296, 102, 173, 44, 613, 386, 856, 91, 89, 241, 913, 
                849, 953, 865, 637, 193, 985, 401, 412, 202, 195, 336, 374, 329, 854
            };
        
        for ( long i = 0; i < nbRound; i++ )
        {
            values[(int)i] = i;
        }
        
        //shuffle( values );

        //printValues( values );

        for ( long i = 0; i < nbRound/increment; i++ )
        {
            //System.out.println( "\nInserting " + i + " in the tree ---->" );
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    long val = values[( int )( i * increment + j )];
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.insert( writeTxn, val, Long.toString( val ) );
                }
            }
        }

        long t1 = System.currentTimeMillis();
        
        System.out.println( "Delta for " + nbRound + " : " + ( t1 - t0 ) );
        int counter = 0;

        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
            while ( cursor.hasNext() )
            {
                cursor.next();
                counter++;
            }
        }
        
        assertEquals( nbRound, counter );

        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
        }

        // Now delete the elements
        //shuffle( values );
        
        //printValues( values );
        values = new long[]
            {
                477, 459, 542, 462, 119, 743, 714, 881, 24, 561, 550, 583, 38, 562, 417, 954, 893, 421, 173, 25, 
                766, 70, 928, 685, 797, 839, 313, 599, 885, 386, 573, 246, 763, 936, 430, 380, 327, 169, 318, 631, 677, 
                332, 123, 596, 312, 449, 105, 129, 533, 435, 985, 465, 978, 3, 315, 911, 463, 632, 520, 323, 142, 19, 
                937, 609, 755, 216, 614, 329, 617, 628, 87, 440, 745, 222, 616, 842, 657, 856, 776, 405, 373, 841, 292, 
                389, 107, 309, 947, 460, 930, 43, 636, 961, 886, 703, 57, 718, 431, 39, 643, 224, 383, 269, 896, 346, 
                579, 512, 147, 910, 448, 234, 706, 995, 296, 917, 442, 131, 721, 487, 209, 126, 339, 555, 33, 475, 876, 
                135, 957, 408, 199, 571, 557, 554, 396, 31, 35, 8, 948, 395, 914, 905, 214, 601, 809, 64, 761, 551, 
                700, 575, 106, 65, 193, 275, 606, 892, 41, 875, 420, 852, 168, 879, 238, 618, 996, 356, 515, 91, 553, 
                402, 26, 895, 83, 756, 227, 788, 784, 964, 974, 391, 494, 748, 334, 211, 942, 864, 452, 682, 270, 50, 
                653, 593, 367, 68, 861, 289, 900, 218, 711, 413, 615, 656, 854, 869, 534, 923, 675, 285, 673, 267, 932, 
                497, 316, 139, 620, 567, 751, 759, 48, 965, 314, 845, 630, 658, 172, 384, 707, 683, 186, 953, 406, 32, 
                783, 728, 587, 882, 15, 559, 539, 742, 870, 828, 655, 713, 22, 716, 984, 693, 749, 723, 972, 114, 511, 
                345, 27, 264, 350, 704, 662, 272, 970, 621, 454, 484, 437, 916, 310, 560, 358, 200, 684, 196, 324, 890, 
                92, 20, 605, 546, 127, 291, 526, 790, 337, 732, 151, 758, 966, 924, 578, 423, 818, 701, 66, 590, 441, 
                752, 582, 453, 991, 793, 230, 804, 377, 245, 705, 648, 153, 295, 623, 128, 258, 833, 753, 989, 100, 425, 
                496, 600, 7, 152, 342, 439, 176, 143, 73, 810, 592, 674, 495, 538, 192, 122, 979, 306, 671, 540, 271, 
                519, 754, 164, 805, 777, 665, 501, 821, 522, 474, 125, 692, 308, 158, 456, 365, 819, 846, 277, 263, 221, 
                543, 353, 175, 51, 155, 687, 604, 960, 796, 254, 720, 443, 6, 897, 336, 278, 719, 891, 572, 872, 447, 
                466, 779, 997, 398, 351, 480, 159, 253, 612, 949, 814, 328, 789, 60, 565, 299, 162, 467, 851, 531, 212, 
                906, 727, 803, 228, 799, 827, 40, 689, 58, 986, 568, 231, 201, 768, 82, 873, 77, 348, 775, 45, 624, 
                28, 134, 969, 49, 154, 18, 364, 999, 676, 633, 668, 737, 778, 971, 547, 170, 840, 436, 21, 444, 798, 
                232, 85, 874, 666, 108, 925, 862, 481, 629, 934, 446, 410, 871, 317, 392, 516, 699, 530, 887, 335, 859, 
                30, 229, 78, 645, 902, 800, 301, 67, 355, 619, 563, 834, 988, 379, 903, 322, 2, 300, 394, 667, 381, 
                10, 717, 149, 427, 409, 217, 787, 724, 963, 830, 148, 69, 235, 469, 124, 597, 97, 549, 794, 378, 341, 
                785, 801, 847, 849, 478, 260, 145, 920, 652, 837, 973, 843, 177, 762, 188, 962, 347, 491, 844, 922, 608, 
                907, 992, 670, 697, 184, 23, 344, 510, 880, 865, 262, 935, 207, 458, 330, 982, 654, 878, 595, 868, 112, 
                939, 607, 321, 532, 528, 72, 663, 294, 369, 411, 182, 251, 130, 698, 817, 760, 951, 722, 366, 712, 850, 
                857, 544, 136, 509, 412, 86, 117, 79, 113, 215, 382, 101, 426, 102, 103, 933, 244, 359, 634, 884, 503, 
                290, 672, 644, 265, 651, 94, 816, 577, 500, 223, 527, 750, 735, 226, 338, 362, 586, 450, 541, 434, 249, 
                588, 121, 836, 598, 918, 240, 994, 422, 403, 288, 388, 397, 472, 659, 564, 625, 42, 204, 493, 191, 537, 
                187, 715, 237, 208, 738, 505, 926, 853, 220, 639, 393, 93, 12, 647, 326, 281, 451, 118, 782, 640, 990, 
                374, 940, 213, 944, 832, 138, 471, 773, 908, 977, 202, 55, 268, 53, 116, 464, 998, 650, 319, 680, 210, 
                729, 952, 286, 11, 764, 611, 433, 955, 400, 160, 780, 570, 822, 407, 349, 866, 284, 490, 709, 171, 185, 
                558, 502, 479, 461, 225, 694, 980, 390, 813, 418, 938, 104, 89, 241, 311, 913, 280, 357, 904, 976, 638, 
                178, 476, 424, 679, 771, 370, 298, 166, 63, 483, 807, 203, 489, 189, 95, 283, 62, 331, 525, 695, 860, 
                368, 678, 16, 363, 75, 855, 946, 37, 959, 646, 792, 219, 414, 252, 956, 54, 195, 642, 603, 302, 791, 
                486, 372, 261, 34, 361, 150, 1, 726, 146, 521, 580, 276, 432, 325, 734, 157, 696, 233, 919, 731, 255, 
                110, 909, 806, 993, 257, 688, 293, 835, 132, 499, 156, 194, 740, 536, 808, 320, 594, 181, 641, 340, 746, 
                690, 376, 498, 115, 206, 61, 757, 47, 867, 576, 820, 637, 399, 610, 385, 730, 297, 180, 141, 248, 266, 
                517, 56, 17, 236, 802, 767, 812, 591, 360, 921, 649, 602, 183, 725, 710, 488, 535, 772, 627, 898, 98, 
                901, 507, 163, 589, 552, 273, 584, 975, 770, 664, 958, 929, 71, 899, 84, 669, 894, 46, 774, 848, 419, 
                987, 795, 304, 333, 9, 282, 5, 354, 514, 404, 927, 983, 585, 574, 781, 303, 545, 581, 29, 556, 259, 
                96, 877, 529, 811, 825, 415, 548, 120, 375, 287, 445, 945, 243, 14, 99, 686, 438, 635, 863, 508, 482, 
                826, 90, 111, 736, 428, 769, 473, 88, 660, 824, 823, 950, 416, 239, 702, 931, 13, 889, 915, 0, 144, 
                343, 741, 566, 941, 371, 661, 829, 733, 161, 274, 401, 815, 513, 981, 76, 305, 250, 205, 883, 455, 626, 
                968, 786, 967, 44, 943, 681, 198, 739, 506, 858, 523, 80, 133, 765, 708, 744, 179, 468, 429, 485, 256, 
                457, 4, 242, 74, 518, 52, 524, 691, 387, 492, 307, 912, 831, 613, 888, 167, 247, 838, 81, 279, 109, 
                190, 352, 470, 59, 140, 174, 504, 165, 747, 137, 622, 36, 569, 197
            };

        increment = 1L;
        
        for ( long i = 0; i < nbRound/increment; i++ )
        {
            //System.out.println( "\nInserting " + i + " in the tree ---->" );
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    int elemNb = ( int )( i * increment + j );
                    long val = values[elemNb];
                    System.out.println( "deleting " + elemNb );
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.delete( writeTxn, val );
                }
            }
        }
    }
    
    
    @Test
    public void testDelete() throws Exception
    {
        //btree.setPageNbElem( 8 );

        // Inject some data
        long increment = 1_000L;
        long nbRound = 100_000L;
        long t0 = System.currentTimeMillis();

        long[] values = new long[(int)nbRound];

        for ( long i = 0; i < nbRound; i++ )
        {
            values[(int)i] = i;
        }
        
        shuffle( values );
        //printValues( values );

        /*values = new long[]
            {
                71, 61, 17, 29, 86, 3, 26, 40, 73, 89, 15, 83, 65, 34, 53, 24, 14, 1, 36, 75, 
                80, 74, 23, 6, 19, 94, 2, 90, 85, 98, 41, 84, 69, 79, 56, 48, 52, 72, 39, 13, 57, 
                12, 45, 97, 59, 5, 35, 16, 58, 21, 81, 54, 42, 28, 66, 22, 91, 64, 51, 60, 70, 62, 
                92, 46, 4, 37, 32, 96, 31, 50, 33, 77, 99, 27, 49, 67, 47, 43, 68, 78, 20, 9, 38, 
                8, 18, 25, 10, 30, 0, 82, 76, 87, 55, 44, 95, 88, 93, 11, 63, 7
            };
        */

        for ( long i = 0; i < nbRound/increment; i++ )
        {
            //System.out.println( "\nInserting " + i + " in the tree ---->" );
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    long val = values[( int )( i * increment + j )];
                    //System.out.println( "\n\nInjecting " + val );
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.insert( writeTxn, val, Long.toString( val ) );
                }
            }
        }

        long t1 = System.currentTimeMillis();
        
        System.out.println( "Delta for " + nbRound + " : " + ( t1 - t0 ) );
        int counter = 0;

        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
            while ( cursor.hasNext() )
            {
                cursor.next();
                counter++;
            }
        }
        
        assertEquals( nbRound, counter );
        
        // Now delete the elements
        shuffle( values );
        
        //printValues( values );
        /*
        values = new long[]
            {
                88, 87, 32, 74, 16, 18, 70, 99, 62, 19, 5, 40, 7, 73, 21, 22, 79, 13, 89, 9, 
                42, 17, 20, 68, 8, 65, 78, 31, 30, 69, 92, 57, 50, 29, 98, 34, 52, 35, 12, 27, 1, 
                53, 82, 39, 44, 76, 55, 61, 24, 10, 36, 14, 33, 48, 80, 15, 28, 64, 49, 97, 77, 43, 
                47, 56, 37, 71, 81, 63, 6, 38, 84, 23, 93, 67, 95, 96, 54, 60, 91, 51, 94, 58, 85, 
                11, 83, 72, 45, 0, 46, 26, 2, 25, 86, 3, 59, 75, 90, 66, 41, 4
            };
        //increment = 1L;
         */
        
        long tt0 = System.currentTimeMillis();

        for ( long i = 0; i < nbRound/increment; i++ )
        {
            //System.out.println( "\nInserting " + i + " in the tree ---->" );
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    int elemNb = ( int )( i * increment + j );
                    long val = values[elemNb];
                    //System.out.println( "Deleting " + elemNb + "th element : " + val );
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.delete( writeTxn, val );
                }
            }
        }

        long tt1 = System.currentTimeMillis();
        
        System.out.println( "Delta for " + nbRound + " : " + ( tt1 - tt0 ) );
    }

    
    @Test
    public void testDeleteRandom() throws Exception
    {
        btree.setPageNbElem( 4 );

        // Inject some data
        long increment = 1L;
        long t0 = System.currentTimeMillis();

        //long[] values = new long[(int)nbRound];
        long[] values = new long[]
            {
                6, 9, 4, 13, 10, 12, 11, 3, 2, 8,
                15, 16, 7, 14, 1, 5, 0
        };
        long nbRound = values.length;

        /*
        for ( long i = 0; i < nbRound; i++ )
        {
            values[(int)i] = i;
        }
        
        shuffle( values );
        */

        for ( long i = 0; i < nbRound/increment; i++ )
        {
            //System.out.println( "\nInserting " + i + " in the tree ---->" );
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    long val = values[( int )( i * increment + j )];
                    //System.out.println( "Injecting " + val );
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.insert( writeTxn, val, Long.toString( val ) );
                }
                
                System.out.println( btree );
            }
        }

        long t1 = System.currentTimeMillis();
        
        System.out.println( "Delta for " + nbRound + " : " + ( t1 - t0 ) );
        int counter = 0;

        /*values = new long[]
            {
                13, 14
        };*/

        for ( long i = 0; i < nbRound/increment; i++ )
        {
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );
                
                for ( long j = 0; j < increment; j++ )
                {
                    long val = values[( int )( i * increment + j )];

                    System.out.println( "Deleting " + val );
                    btree.delete( writeTxn, val );
                    System.out.println( btree );
                }
            }
        }
    }


    /**
     * Test the browse methods on a btree containing many nodes
     */
    @Test
    public void testDeleteBTreeNodes() throws Exception
    {
        // Inject some data
        long increment = 2L;
        long nbRound = 32L;
        long t0 = System.currentTimeMillis();
        
        for ( long i = 0; i < nbRound/increment; i++ )
        {
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( long j = 0; j < increment; j++ )
                {
                    long val = i*increment + j;
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.insert( writeTxn, val, Long.toString( val ) );
                }
            }
        }

        long t1 = System.currentTimeMillis();
        
        System.out.println( "Delta add    : " + ( t1 - t0 ) );
        System.out.println( "File name    : " + dataDir );
        
        long[] values = new long[(int)nbRound];
        
        for ( long i = 0; i < nbRound; i++ )
        {
            values[(int)i] = i;
        }
        
        shuffle( values );
        
        //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
        long t10 = System.currentTimeMillis();
        
        for ( int i = 0; i < nbRound/increment; i++ )
        {
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );

                for ( int j = 0; j < increment; j++ )
                {
                    int index = (int)(i*increment + j);
                    //MavibotInspector.check( recordManager, recordManager.getRecordManagerHeader() );
                    btree.delete( writeTxn, values[index] );
                }
            }
            
            // Check that we can still browse the tree
            try ( Transaction readTxn = recordManager.beginReadTransaction() )
            {
                BTree<Long, String> readBtree = readTxn.getBTree( "test" );
                TupleCursor<Long, String> cursor = readBtree.browse( readTxn );
                
                cursor.beforeFirst();
                int nbElems = 0;
                
                while ( cursor.hasNext() )
                {
                    cursor.next();
                    nbElems++;
                }
                
                assertEquals( nbElems, nbRound - increment * ( i + 1 ) );
            }
        }

        long t11 = System.currentTimeMillis();
        
        System.out.println( "Delta delete    : " + ( t11 - t10 ) );

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );

            // Move forward
            cursor.afterLast();
            cursor.beforeFirst();
    
            assertFalse( cursor.hasPrev() );
            assertFalse( cursor.hasNext() );
        }
    }


    /**
     * Test the browse methods on a btree containing many nodes
     */
    @Test
    public void testBrowseBTreeNodesPrev() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            for ( long i = 1; i < 1_000L; i++ )
            {
                btree.insert( writeTxn, i, Long.toString( i ) );
            }
        }

        //MavibotInspector.check( recordManager, recordManager.getCurrentRecordManagerHeader() );

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );

            // Move backward
            cursor.afterLast();
    
            assertTrue( cursor.hasPrev() );
            assertFalse( cursor.hasNext() );
    
            checkPrev( cursor, 999L, "999", false, true );
    
            for ( long i = 998L; i > 1L; i-- )
            {
                checkPrev( cursor, i, Long.toString( i ), true, true );
            }
    
            assertTrue( cursor.hasPrev() );

            checkPrev( cursor, 1L, "1", true, false );
        }
    }


    /**
     * Test the browse methods on a btree containing just a leaf with duplicate values
     */
    @Test
    public void testBrowseBTreeLeafNextDupsN() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        // Inject some duplicate data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            btree.insert( writeTxn, 1L, "1" );
            btree.insert( writeTxn, 1L, "4" );
            btree.insert( writeTxn, 1L, "2" );
            btree.insert( writeTxn, 2L, "3" );
            btree.insert( writeTxn, 3L, "5" );
            btree.insert( writeTxn, 3L, "7" );
            btree.insert( writeTxn, 3L, "6" );
        }

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
            // Move forward
            cursor.beforeFirst();
    
            assertFalse( cursor.hasPrev() );
            assertTrue( cursor.hasNext() );
    
            checkNext( cursor, 1L, "2", true, false );
            checkNext( cursor, 2L, "3", true, true );
            checkNext( cursor, 3L, "6", false, true );
        }
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
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browseFrom( readTxn, 1L );
    
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
    }


    /**
     * Test the browseFrom methods on a btree containing just a leaf
     */
    @Test
    public void testBrowseFromBTreeLeaf() throws IOException, BTreeAlreadyManagedException
    {
        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            btree.insert( writeTxn, 1L, "1" );
            btree.insert( writeTxn, 7L, "7" );
            btree.insert( writeTxn, 3L, "3" );
            btree.insert( writeTxn, 5L, "5" );
            btree.insert( writeTxn, 9L, "9" );
        }

        // Create the cursor, starting at 5
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browseFrom( readTxn, 5L );
    
            assertTrue( cursor.hasPrev() );
            assertTrue( cursor.hasNext() );
            
            assertEquals( 5L, cursor.get().key.longValue() );
    
            // Move forward
            checkNext( cursor, 7L, "7", true, true );
            checkNext( cursor, 9L, "9", false, true );
    
            cursor.close();
    
            // now, start at 5 and move backward
            cursor = btree.browseFrom( readTxn, 5L );
    
            assertTrue( cursor.hasPrev() );
            assertTrue( cursor.hasNext() );
    
            // Move backward
            checkPrev( cursor, 3L, "3", true, true );
            checkPrev( cursor, 1L, "1", true, false );
            cursor.close();
    
            // Start at the first key
            cursor = btree.browseFrom( readTxn, 1L );
            assertFalse( cursor.hasPrev() );
            assertTrue( cursor.hasNext() );
    
            checkNext( cursor, 3L, "3", true, true );
    
            // Start before the first key
            cursor = btree.browseFrom( readTxn, 0L );
            assertFalse( cursor.hasPrev() );
            assertTrue( cursor.hasNext() );
    
            checkNext( cursor, 1L, "1", true, false );
            checkNext( cursor, 3L, "3", true, true );
    
            // Start at the last key
            cursor = btree.browseFrom( readTxn, 9L );
            assertTrue( cursor.hasPrev() );
            assertFalse( cursor.hasNext() );
    
            checkPrev( cursor, 7L, "7", true, true );
    
            // Start after the last key
            cursor = btree.browseFrom( readTxn, 10L );
            assertTrue( cursor.hasPrev() );
            assertFalse( cursor.hasNext() );
    
            checkPrev( cursor, 9L, "9", false, true );
            checkPrev( cursor, 7L, "7", true, true );
    
            // Start in the middle with a non existent key
            cursor = btree.browseFrom( readTxn, 4L );
            assertTrue( cursor.hasPrev() );
            assertTrue( cursor.hasNext() );
    
            checkNext( cursor, 7L, "7", true, true );
    
            // Start in the middle with a non existent key
            cursor = btree.browseFrom( readTxn, 4L );
    
            checkPrev( cursor, 3L, "3", true, true );
        }
    }


    /**
     * Test the browseFrom method on a btree with a non existing key
     */
    @Test
    public void testBrowseFromBTreeNodesNotExistingKey() throws IOException, BTreeAlreadyManagedException
    {
        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );

            for ( long i = 0; i <= 1000L; i += 2 )
            {
                btree.insert( writeTxn, i, Long.toString( i ) );
            }
        }

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browseFrom( readTxn, 1500L );
    
            assertFalse( cursor.hasNext() );
            assertTrue( cursor.hasPrev() );
            assertEquals( 1000L, cursor.prev().getKey().longValue() );
        }
    }




    /**
     * Test the TupleCursor.moveToPrevNonDuplicateKey method on a B-tree containing nodes
     */
    @Test
    public void testPrevKey() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            
            for ( long i = 1; i < 1000L; i++ )
            {
                System.out.println( "Insert " + i );
                try
                {
                    btree.insert( writeTxn, i, Long.toString( i ) );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        }

        // Create the cursor
        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
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
    }


    /**
     * Test the overwriting of elements
     */
    @Test
    public void testOverwrite() throws Exception
    {
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            BTree<Long, String> btree = writeTxn.getBTree( "test" );
            btree.insert( writeTxn, 1L, "1" );
        }

        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            assertTrue( btree.hasKey( readTxn, 1L ) );
    
            assertEquals( "1", btree.get( readTxn, 1L ) );
    
            try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
            {
                btree.insert( writeTxn, 1L, "10" );
            }
    
            assertTrue( btree.hasKey( readTxn, 1L ) );
            assertEquals( "10", btree.get( readTxn, 1L ) );
        }
    
        btree.close();
    }


    //@Ignore("test used for debugging")
    @Test
    public void testAdd20Random() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException, CursorException
    {
        long[] values = new long[]
            {
                14, 7, 43, 37, 49, 3, 20, 26, 17, 29,
                40, 33, 21, 18, 9, 30, 45, 36, 12, 8
        };

        btree.setPageNbElem( 4 );

        // Inject some data
        try ( WriteTransaction writeTxn = recordManager.beginWriteTransaction() )
        {
            for ( long value : values )
            {
                BTree<Long, String> btree = writeTxn.getBTree( "test" );
                btree.insert( writeTxn, value, Long.toString( value ) );
            }
        }

        MavibotInspector.check( recordManager );

        try ( Transaction readTxn = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree = readTxn.getBTree( "test" );
            TupleCursor<Long, String> cursor = btree.browse( readTxn );
    
            while ( cursor.hasNext() )
            {
                cursor.next();
            }
        }
    }
}