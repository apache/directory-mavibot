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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.CursorException;
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
 * test the RecordManager
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RecordManagerTest
{
    private BTree<Long, String> btree = null;

    private RecordManager recordManager = null;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dataDir = null;


    @Before
    public void createBTree() throws IOException
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        openRecordManagerAndBtree();

        try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
        {
            // Create a new BTree
            btree = recordManager.addBTree( writeTransaction, "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    @After
    public void cleanup() throws IOException
    {
        btree.close();

        recordManager.close();
        
        if ( dataDir.exists() )
        {
            FileUtils.deleteDirectory( dataDir );
        }
    }


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
                try ( Transaction readTransaction = recordManager.beginReadTransaction() )
                {
                    btree = recordManager.getBtree( readTransaction, btree.getName() );
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    /**
     * Test the creation of a RecordManager, and that we can read it back.
     */
    @Test
    public void testRecordManager() throws IOException, BTreeAlreadyManagedException
    {
        assertEquals( 3, recordManager.getNbManagedTrees( recordManager.getCurrentRecordManagerHeader() ) );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        try ( Transaction readTransaction = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree1 = recordManager.getBtree( readTransaction, "test" );
    
            assertNotNull( btree1 );
            assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
            assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
            assertEquals( btree.getName(), btree1.getName() );
            assertEquals( btree.getNbElems(), btree1.getNbElems() );
            assertEquals( btree.getBtreeInfo().getPageNbElem(), btree1.getBtreeInfo().getPageNbElem() );
            assertEquals( btree.getBtreeHeader().getRevision(), btree1.getBtreeHeader().getRevision() );
            assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );
        }
    }


    /**
     * Test the creation of a RecordManager with a BTree containing data.
     */
    @Test
    public void testRecordManagerWithBTree() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Now, add some elements in the BTree
        try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
        {
            btree.insert( writeTransaction, 3L, "V3" );
            btree.insert( writeTransaction, 1L, "V1" );
            btree.insert( writeTransaction, 5L, "V5" );
        }

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 3, recordManager.getNbManagedTrees( recordManager.getCurrentRecordManagerHeader() ) );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );
        
        try ( Transaction transaction = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree1 = recordManager.getBtree( transaction,  "test" );
    
            assertNotNull( btree1 );
            assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
            assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
            assertEquals( btree.getName(), btree1.getName() );
            assertEquals( btree.getNbElems(), btree1.getNbElems() );
            assertEquals( btree.getBtreeInfo().getPageNbElem(), btree1.getBtreeInfo().getPageNbElem() );
            assertEquals( btree.getBtreeHeader().getRevision(), btree1.getBtreeHeader().getRevision() );
            assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );
    
            // Check the stored element
            assertTrue( btree1.hasKey( transaction, 1L ) );
            assertTrue( btree1.hasKey( transaction, 3L ) );
            assertTrue( btree1.hasKey( transaction, 5L ) );
            assertEquals( "V1", btree1.get( transaction, 1L ) );
            assertEquals( "V3", btree1.get( transaction, 3L ) );
            assertEquals( "V5", btree1.get( transaction, 5L ) );
        }
    }


    /**
     * Test the creation of a RecordManager with a BTree containing data, enough for some Node to be created.
     */
    @Test
    public void testRecordManagerWithBTreeLeafNode() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        // Now, add some elements in the BTree
        for ( long i = 1L; i < 32L; i++ )
        {
            try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
            {
                btree.insert( writeTransaction, i, "V" + i );
            }
        }

        for ( long i = 1L; i < 32L; i++ )
        {
            try ( Transaction transaction = recordManager.beginReadTransaction() )
            {
                if ( !btree.hasKey( transaction, i ) )
                {
                    System.out.println( "Not found !!! " + i );
                }
                assertTrue( btree.hasKey( transaction, i ) );
                assertEquals( "V" + i, btree.get( transaction, i ) );
            }
        }

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 3, recordManager.getNbManagedTrees( recordManager.getCurrentRecordManagerHeader() ) );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        try ( Transaction transaction = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree1 = recordManager.getBtree( transaction, "test" );

            assertNotNull( btree1 );
            assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
            assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
            assertEquals( btree.getName(), btree1.getName() );
            assertEquals( btree.getNbElems(), btree1.getNbElems() );
            assertEquals( btree.getBtreeInfo().getPageNbElem(), btree1.getBtreeInfo().getPageNbElem() );
            assertEquals( btree.getBtreeHeader().getRevision(), btree1.getBtreeHeader().getRevision() );
            assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

            // Check the stored element
            for ( long i = 1L; i < 32L; i++ )
            {
                if ( !btree1.hasKey( transaction, i ) )
                {
                    System.out.println( "Not found " + i );
                }
                assertTrue( btree1.hasKey( transaction, i ) );
                assertEquals( "V" + i, btree1.get( transaction, i ) );
            }
        }
    }


    /**
     * Test the creation of a RecordManager with a BTree containing 100 000 elements
     */
    @Test
    @Ignore("This is a performance test")
    public void testRecordManagerWithBTreeLeafNode100K() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        // Don't keep any revision
        recordManager.setKeepRevisions( false );

        String fileName = dataDir.getAbsolutePath() + "/mavibot.db";
        File file = new File( fileName );
        long fileSize = file.length();
        long nbElems = 100000L;
        System.out.println( "----- Size before = " + fileSize );

        // Now, add some elements in the BTree
        long t0 = System.currentTimeMillis();

        for ( Long i = 0L; i < nbElems; i++ )
        {
            String value = "V" + i;
            
            try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
            {
                btree.insert( writeTransaction, i, value );
            }

            /*
            if ( !recordManager1.check() )
            {
                System.out.println( "Failure while adding element " + i );
                fail();
            }
            */

            if ( i % 10000 == 0 )
            {
                fileSize = file.length();
                System.out.println( "----- Size after insertion of " + i + " = " + fileSize );
                System.out.println( recordManager );
                //System.out.println( btree );
            }
        }
        long t1 = System.currentTimeMillis();

        fileSize = file.length();
        System.out.println( "Size after insertion of 100 000 elements : " + fileSize );
        System.out.println( "Time taken to write 100 000 elements : " + ( t1 - t0 ) );
        System.out.println( "  Nb elem/s : " + ( ( nbElems * 1000 ) / ( t1 - t0 ) ) );
        //System.out.println( "Nb created page " + recordManager.nbCreatedPages.get() );
        //System.out.println( "Nb allocated page " + recordManager.nbReusedPages.get() );
        //System.out.println( "Nb page we have freed " + recordManager.nbFreedPages.get() );
        System.out.println( recordManager );

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees( recordManager.getCurrentRecordManagerHeader() ) );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        try ( Transaction transaction = recordManager.beginReadTransaction() )
        {
            BTree<Long, String> btree1 = recordManager.getBtree( transaction, "test" );
    
            assertNotNull( btree1 );
            assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
            assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
            assertEquals( btree.getName(), btree1.getName() );
            assertEquals( btree.getNbElems(), btree1.getNbElems() );
            assertEquals( btree.getBtreeInfo().getPageNbElem(), btree1.getBtreeInfo().getPageNbElem() );
            assertEquals( btree.getBtreeHeader().getRevision(), btree1.getBtreeHeader().getRevision() );
            assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );
    
            // Check the stored element
            long t2 = System.currentTimeMillis();
            for ( long i = 0L; i < nbElems; i++ )
            {
                //assertTrue( btree1.exist( i ) );
                assertEquals( "V" + i, btree1.get( transaction, i ) );
            }
            long t3 = System.currentTimeMillis();
            System.out.println( "Time taken to verify 100 000 elements : " + ( t3 - t2 ) );
    
            // Check the stored element a second time
            long t4 = System.currentTimeMillis();
            for ( long i = 0L; i < nbElems; i++ )
            {
                //assertTrue( btree1.exist( i ) );
                assertEquals( "V" + i, btree1.get( transaction, i ) );
            }
            long t5 = System.currentTimeMillis();
            System.out.println( "Time taken to verify 100 000 elements : " + ( t5 - t4 ) );
        }
    }


    private void checkBTreeRevisionBrowse( BTree<Long, String> btree, long revision, long... values )
        throws IOException,
        KeyNotFoundException, CursorException
    {
        try ( Transaction transaction = recordManager.beginReadTransaction() )
        {
            TupleCursor<Long, String> cursor = btree.browse( transaction );
            List<Long> expected = new ArrayList<Long>( values.length );
            Set<Long> found = new HashSet<Long>( values.length );
    
            for ( long value : values )
            {
                expected.add( value );
            }
    
            int nb = 0;
    
            while ( cursor.hasNext() )
            {
                Tuple<Long, String> res = cursor.next();
    
                long key = res.getKey();
                assertEquals( expected.get( nb ), ( Long ) key );
                assertFalse( found.contains( key ) );
                found.add( key );
                assertEquals( "V" + key, res.getValue() );
                nb++;
            }
    
            assertEquals( values.length, nb );
            cursor.close();
        }
    }


    private void checkBTreeRevisionBrowseFrom( BTree<Long, String> btree, long revision, long from, long... values )
        throws IOException,
        KeyNotFoundException
    {
        try ( Transaction transaction = recordManager.beginReadTransaction() )
        {
            TupleCursor<Long, String> cursor = btree.browseFrom( transaction, from );
            List<Long> expected = new ArrayList<Long>( values.length );
            Set<Long> found = new HashSet<Long>( values.length );
    
            for ( long value : values )
            {
                expected.add( value );
            }
    
            int nb = 0;
    
            while ( cursor.hasNext() )
            {
                Tuple<Long, String> res = cursor.next();
    
                long key = res.getKey();
                assertEquals( expected.get( nb ), ( Long ) key );
                assertFalse( found.contains( key ) );
                found.add( key );
                assertEquals( "V" + key, res.getValue() );
                nb++;
            }
    
            assertEquals( values.length, nb );
            cursor.close();
        }
    }


    @Test
    public void testAdds() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
        {
            btree.insert( writeTransaction, 1L, "V1" );
            btree.insert( writeTransaction, 2L, "V2" );
        }
    }


    @Ignore
    @Test
    public void testAddInTxns() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        /*
        for ( Long key : recordManager.writeCounter.keySet() )
        {
            System.out.println( "Page " + Long.toHexString( key ) + " written " + recordManager.writeCounter.get( key )
                + " times" );
        }

        System.out.println( "Test start" );
        */
        try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
        {
            /*
            System.out.println( "Before V1" );
            for ( Long key : recordManager.writeCounter.keySet() )
            {
                System.out.println( "Page " + Long.toHexString( key ) + " written " + recordManager.writeCounter.get( key )
                    + " times" );
            }
            */
            btree.insert( writeTransaction, 1L, "V1" );
            /*
            for ( Long key : recordManager.writeCounter.keySet() )
            {
                System.out.println( "Page " + Long.toHexString( key ) + " written " + recordManager.writeCounter.get( key )
                    + " times" );
            }
            
            System.out.println( "After V1" );
            */
    
            //System.out.println( "Before V2" );
            btree.insert( writeTransaction, 2L, "V2" );
            //System.out.println( "After V2" );
    
            //System.out.println( "Before V3" );
            btree.insert( writeTransaction, 3L, "V3" );
            /*
            for ( Long key : recordManager.writeCounter.keySet() )
            {
                System.out.println( "Page " + Long.toHexString( key ) + " written " + recordManager.writeCounter.get( key )
                    + " times" );
            }
            */
        }

        /*
        for ( Long key : recordManager.writeCounter.keySet() )
        {
            System.out.println( "Page " + Long.toHexString( key ) + " written " + recordManager.writeCounter.get( key )
                + " times" );
        }
        */
    }


    @Test
    public void testInspector() throws Exception
    {
        MavibotInspector inspector = new MavibotInspector( new File( "/Users/elecharny/Downloads/mavibot.db" ) );
        inspector.start();
    }
}
