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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.apache.directory.mavibot.btree.util.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for free page reclaimer.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PageReclaimerTest
{
    private static final String TREE_NAME = "uid-tree";

    private RecordManager rm;

    private PersistedBTree<Integer, String> uidTree;
    
    @Rule
    public TemporaryFolder tmpDir;

    private File dbFile;


    @Before
    public void setup() throws Exception
    {
        tmpDir = new TemporaryFolder();
        tmpDir.create();
        
        dbFile = tmpDir.newFile( "spacereclaimer.db" );

        //System.out.println(dbFile.getAbsolutePath());
        rm = new RecordManager( dbFile.getAbsolutePath() );
        rm.setPageReclaimerThreshold( 10 );
        
        uidTree = ( PersistedBTree<Integer, String> ) rm.addBTree( TREE_NAME, IntSerializer.INSTANCE, StringSerializer.INSTANCE, false );
    }


    @After
    public void cleanup() throws Exception
    {
        rm.close();
        dbFile.delete();
        tmpDir.delete();
    }

    
    private void closeAndReopenRM() throws Exception
    {
        uidTree.close();
        rm.close();
        rm = new RecordManager( dbFile.getAbsolutePath() );
        uidTree = ( PersistedBTree ) rm.getManagedTree( TREE_NAME );
    }

    
    @Test
    public void testReclaimer() throws Exception
    {
        int total = 11;
        for ( int i=0; i < total; i++ )
        {
            uidTree.insert( i, String.valueOf( i ) );
        }

        //System.out.println( "Total size before closing " + dbFile.length() );
        //System.out.println( dbFile.length() );
        closeAndReopenRM();
        //System.out.println( "Total size AFTER closing " + dbFile.length() );
        
        int count = 0;
        TupleCursor<Integer, String> cursor = uidTree.browse();
        while ( cursor.hasNext() )
        {
            Tuple<Integer, String> t = cursor.next();
            assertEquals( t.key, Integer.valueOf( count ) );
            count++;
        }
        
        assertEquals( count, total );
    }
    

    /**
     * with the reclaimer threshold 10 and total entries of 1120
     * there was a condition that resulted in OOM while reopening the RM
     * 
     * This issue was fixed after PageReclaimer was updated to run in
     * a transaction.
     * 
     * This test is present to verify the fix
     * 
     * @throws Exception
     */
    @Test
    public void testReclaimerWithMagicNum() throws Exception
    {
    	rm.setPageReclaimerThreshold( 10 );
    	
        int total = 1120;
        for ( int i=0; i < total; i++ )
        {
            uidTree.insert( i, String.valueOf( i ) );
        }

        closeAndReopenRM();
        
        int count = 0;
        TupleCursor<Integer, String> cursor = uidTree.browse();
        while ( cursor.hasNext() )
        {
            Tuple<Integer, String> t = cursor.next();
            assertEquals( t.key, Integer.valueOf( count ) );
            count++;
        }
        
        assertEquals( count, total );
    }

    
    /**
     * Test reclaimer functionality while multiple threads writing to the same BTree
     * 
     * @throws Exception
     */
    @Test
    public void testReclaimerWithMultiThreads() throws Exception
    {
        final int numEntriesPerThread = 11;
        final int numThreads = 5;
        
        final int total = numThreads * numEntriesPerThread;
        
        final Map<Integer, Integer> keyMap = new ConcurrentHashMap<Integer, Integer>();
        
        final Random rnd = new Random();
        
        final CountDownLatch latch = new CountDownLatch( numThreads );
        
        Runnable r = new Runnable()
        {
            @Override
            public void run()
            {
                for ( int i=0; i < numEntriesPerThread; i++ )
                {
                    try
                    {
                        int key = rnd.nextInt( total );
                        while( true )
                        {
                            if( !keyMap.containsKey( key ) )
                            {
                                keyMap.put( key, key );
                                break;
                            }
                            
                            //System.out.println( "duplicate " + key );
                            key = rnd.nextInt( total );
                        }
                        
                        uidTree.insert( key, String.valueOf( key ) );
                    }
                    catch( Exception e )
                    {
                        throw new RuntimeException(e);
                    }
                }
                
                latch.countDown();
            }
        };

        for ( int i=0; i<numThreads; i++ )
        {
            Thread t = new Thread( r );
            t.start();
        }
        
        latch.await();
        
        //System.out.println( "Total size before closing " + dbFile.length() );
        closeAndReopenRM();
        //System.out.println( "Total size AFTER closing " + dbFile.length() );
        
        int count = 0;
        TupleCursor<Integer, String> cursor = uidTree.browse();
        while ( cursor.hasNext() )
        {
            Tuple<Integer, String> t = cursor.next();
            assertEquals( t.key, Integer.valueOf( count ) );
            count++;
        }
        
        cursor.close();
        
        assertEquals( count, total );
    }

    @Test
    @SuppressWarnings("all")
    public void testInspectTreeState() throws Exception
    {
        File file = File.createTempFile( "freepagedump", ".db" );
        
        if ( file.exists() )
        {
            boolean deleted = file.delete();
            if ( !deleted )
            {
                throw new IllegalStateException( "Could not delete the data file " + file.getAbsolutePath() );
            }
        }
            
        RecordManager manager = new RecordManager( file.getAbsolutePath() );
        manager.setPageReclaimerThreshold(17);
        //manager._disableReclaimer( true );
        
        PersistedBTreeConfiguration config = new PersistedBTreeConfiguration();

        config.setName( "dump-tree" );
        config.setKeySerializer( IntSerializer.INSTANCE );
        config.setValueSerializer( StringSerializer.INSTANCE );
        config.setAllowDuplicates( false );
        config.setPageSize( 4 );

        BTree btree = new PersistedBTree( config );
        manager.manage( btree );
        
        // insert 5 so that we get 1 root and 2 child nodes
        for( int i=0; i<5; i++ )
        {
            btree.insert( i, String.valueOf( i ) );
        }
        
        /*
        System.out.println( "Total number of pages created " + manager.nbCreatedPages );
        System.out.println( "Total number of pages reused " + manager.nbReusedPages );
        System.out.println( "Total number of pages freed " + manager.nbFreedPages );
        System.out.println( "Total file size (bytes) " + file.length() );
        */
        
        long totalPages = file.length() / RecordManager.DEFAULT_PAGE_SIZE;
        
        // in RM the header page gets skipped before incrementing nbCreatedPages 
        assertEquals( manager.nbCreatedPages.get() + 1, totalPages );
        
        //System.out.println(btree.getRootPage());
        //System.out.println( file.getAbsolutePath() );
        
        check( manager, btree );
        
        manager.close();
        
        file.delete();
    }
    
   
    private void check(RecordManager manager, BTree btree) throws Exception
    {
        MavibotInspector.check(manager);
        
        List<Long> allOffsets = MavibotInspector.getGlobalPages();
        //System.out.println( "Global: " + allOffsets);
        //System.out.println("Total global offsets " + allOffsets.size() );
        
        int pagesize = RecordManager.DEFAULT_PAGE_SIZE;
        long total = manager.fileChannel.size();
        
        List<Long> unaccounted = new ArrayList<Long>();
        
        for(long i = pagesize; i<= total-pagesize; i+=pagesize)
        {
            if( !allOffsets.contains( Long.valueOf( i ) ) )
            {
                unaccounted.add( i );
            }
        }
        
        TupleCursor<NameRevision, Long> cursor = manager.btreeOfBtrees.browse();
        while(cursor.hasNext())
        {
            Tuple<NameRevision, Long> t = cursor.next();
            System.out.println( t.getKey() + " offset " + t.getValue() );
        }
        
        cursor.close();

        //System.out.println("Unaccounted offsets " + unaccounted);
        assertEquals( 0, unaccounted.size() );
    }
    
}
