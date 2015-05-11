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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
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

        try
        {
            // Create a new BTree
            btree = recordManager.addBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );
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
                btree = recordManager.getManagedTree( btree.getName() );
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
    @Ignore
    public void testRecordManager() throws IOException, BTreeAlreadyManagedException
    {
        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );
    }


    /**
     * Test the creation of a RecordManager with a BTree containing data.
     */
    @Test
    public void testRecordManagerWithBTree() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );
        btree.insert( 1L, "V1" );
        btree.insert( 5L, "V5" );

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        assertTrue( btree1.hasKey( 1L ) );
        assertTrue( btree1.hasKey( 3L ) );
        assertTrue( btree1.hasKey( 5L ) );
        assertEquals( "V1", btree1.get( 1L ) );
        assertEquals( "V3", btree1.get( 3L ) );
        assertEquals( "V5", btree1.get( 5L ) );
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
            btree.insert( i, "V" + i );
        }

        for ( long i = 1L; i < 32L; i++ )
        {
            if ( !btree.hasKey( i ) )
            {
                System.out.println( "Not found !!! " + i );
            }
            assertTrue( btree.hasKey( i ) );
            assertEquals( "V" + i, btree.get( i ) );
        }

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        for ( long i = 1L; i < 32L; i++ )
        {
            if ( !btree1.hasKey( i ) )
            {
                System.out.println( "Not found " + i );
            }
            assertTrue( btree1.hasKey( i ) );
            assertEquals( "V" + i, btree1.get( i ) );
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
            btree.insert( i, value );

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
        System.out.println( "Nb created page " + recordManager.nbCreatedPages.get() );
        System.out.println( "Nb allocated page " + recordManager.nbReusedPages.get() );
        System.out.println( "Nb page we have freed " + recordManager.nbFreedPages.get() );
        System.out.println( recordManager );

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        long t2 = System.currentTimeMillis();
        for ( long i = 0L; i < nbElems; i++ )
        {
            //assertTrue( btree1.exist( i ) );
            assertEquals( "V" + i, btree1.get( i ) );
        }
        long t3 = System.currentTimeMillis();
        System.out.println( "Time taken to verify 100 000 elements : " + ( t3 - t2 ) );

        // Check the stored element a second time
        long t4 = System.currentTimeMillis();
        for ( long i = 0L; i < nbElems; i++ )
        {
            //assertTrue( btree1.exist( i ) );
            assertEquals( "V" + i, btree1.get( i ) );
        }
        long t5 = System.currentTimeMillis();
        System.out.println( "Time taken to verify 100 000 elements : " + ( t5 - t4 ) );
    }


    private void checkBTreeRevisionBrowse( BTree<Long, String> btree, long revision, long... values )
        throws IOException,
        KeyNotFoundException
    {
        TupleCursor<Long, String> cursor = btree.browse( revision );
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


    private void checkBTreeRevisionBrowseFrom( BTree<Long, String> btree, long revision, long from, long... values )
        throws IOException,
        KeyNotFoundException
    {
        TupleCursor<Long, String> cursor = btree.browseFrom( revision, from );
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


    /**
     * Test the creation of a RecordManager with a BTree containing data, where we keep the revisions,
     * and browse the BTree.
     */
    @Test
    public void testRecordManagerBrowseWithKeepRevisions() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        recordManager.setKeepRevisions( true );

        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );
        long rev1 = btree.getRevision();

        btree.insert( 1L, "V1" );
        long rev2 = btree.getRevision();

        btree.insert( 5L, "V5" );
        long rev3 = btree.getRevision();

        // Check that we can browse each revision
        // revision 1
        checkBTreeRevisionBrowse( btree, rev1, 3L );

        // Revision 2
        checkBTreeRevisionBrowse( btree, rev2, 1L, 3L );

        // Revision 3
        checkBTreeRevisionBrowse( btree, rev3, 1L, 3L, 5L );

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        assertTrue( btree1.hasKey( 1L ) );
        assertTrue( btree1.hasKey( 3L ) );
        assertTrue( btree1.hasKey( 5L ) );
        assertEquals( "V1", btree1.get( 1L ) );
        assertEquals( "V3", btree1.get( 3L ) );
        assertEquals( "V5", btree1.get( 5L ) );

        // Check that we can read the revision again
        // revision 1
        checkBTreeRevisionBrowse( btree, rev1 );

        // Revision 2
        checkBTreeRevisionBrowse( btree, rev2 );

        // Revision 3
        checkBTreeRevisionBrowse( btree, rev3, 1L, 3L, 5L );
    }


    /**
     * Test the creation of a RecordManager with a BTree containing data, where we keep the revision, and
     * we browse from a key
     */
    @Test
    public void testRecordManagerBrowseFromWithRevision() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        recordManager.setKeepRevisions( true );

        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );
        long rev1 = btree.getRevision();

        btree.insert( 1L, "V1" );
        long rev2 = btree.getRevision();

        btree.insert( 5L, "V5" );
        long rev3 = btree.getRevision();

        // Check that we can browse each revision
        // revision 1
        checkBTreeRevisionBrowseFrom( btree, rev1, 3L, 3L );

        // Revision 2
        checkBTreeRevisionBrowseFrom( btree, rev2, 3L, 3L );

        // Revision 3
        checkBTreeRevisionBrowseFrom( btree, rev3, 3L, 3L, 5L );

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        assertTrue( btree1.hasKey( 1L ) );
        assertTrue( btree1.hasKey( 3L ) );
        assertTrue( btree1.hasKey( 5L ) );
        assertEquals( "V1", btree1.get( 1L ) );
        assertEquals( "V3", btree1.get( 3L ) );
        assertEquals( "V5", btree1.get( 5L ) );

        // Check that we can read the revision again
        // revision 1
        checkBTreeRevisionBrowseFrom( btree, rev1, 3L );

        // Revision 2
        checkBTreeRevisionBrowseFrom( btree, rev2, 3L );

        // Revision 3
        checkBTreeRevisionBrowseFrom( btree, rev3, 3L, 3L, 5L );
    }


    /**
     * Test a get() from a given revision
     */
    @Test
    public void testGetWithRevision() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        recordManager.setKeepRevisions( true );

        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );
        long rev1 = btree.getRevision();

        btree.insert( 1L, "V1" );
        long rev2 = btree.getRevision();

        btree.insert( 5L, "V5" );
        long rev3 = btree.getRevision();

        // Delete one element
        btree.delete( 3L );
        long rev4 = btree.getRevision();

        // Check that we can get a value from each revision
        // revision 1
        assertEquals( "V3", btree.get( rev1, 3L ) );

        // revision 2
        assertEquals( "V1", btree.get( rev2, 1L ) );
        assertEquals( "V3", btree.get( rev2, 3L ) );

        // revision 3
        assertEquals( "V1", btree.get( rev3, 1L ) );
        assertEquals( "V3", btree.get( rev3, 3L ) );
        assertEquals( "V5", btree.get( rev3, 5L ) );

        // revision 4
        assertEquals( "V1", btree.get( rev4, 1L ) );
        assertEquals( "V5", btree.get( rev4, 5L ) );

        try
        {
            btree.get( rev4, 3L );
            fail();
        }
        catch ( KeyNotFoundException knfe )
        {
            // expected
        }

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        assertTrue( btree1.hasKey( 1L ) );
        assertFalse( btree1.hasKey( 3L ) );
        assertTrue( btree1.hasKey( 5L ) );
        assertEquals( "V1", btree1.get( 1L ) );
        assertEquals( "V5", btree1.get( 5L ) );

        // Check that we can get a value from each revision
        // revision 1
        checkBTreeRevisionBrowse( btree, rev1 );

        // revision 2
        checkBTreeRevisionBrowse( btree, rev2 );

        // revision 3
        checkBTreeRevisionBrowse( btree, rev3 );

        // revision 4
        checkBTreeRevisionBrowse( btree, rev4, 1L, 5L );

        try
        {
            btree.get( rev4, 3L );
            fail();
        }
        catch ( KeyNotFoundException knfe )
        {
            // expected
        }
    }


    /**
     * Test a contain() from a given revision
     */
    @Test
    public void testContainWithRevision() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        recordManager.setKeepRevisions( true );

        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );
        long rev1 = btree.getRevision();

        btree.insert( 1L, "V1" );
        long rev2 = btree.getRevision();

        btree.insert( 5L, "V5" );
        long rev3 = btree.getRevision();

        // Delete one element
        btree.delete( 3L );
        long rev4 = btree.getRevision();

        // Check that we can get a value from each revision
        // revision 1
        assertFalse( btree.contains( rev1, 1L, "V1" ) );
        assertTrue( btree.contains( rev1, 3L, "V3" ) );
        assertFalse( btree.contains( rev1, 5L, "V5" ) );

        // revision 2
        assertTrue( btree.contains( rev2, 1L, "V1" ) );
        assertTrue( btree.contains( rev2, 3L, "V3" ) );
        assertFalse( btree.contains( rev2, 5L, "V5" ) );

        // revision 3
        assertTrue( btree.contains( rev3, 1L, "V1" ) );
        assertTrue( btree.contains( rev3, 3L, "V3" ) );
        assertTrue( btree.contains( rev3, 5L, "V5" ) );

        // revision 4
        assertTrue( btree.contains( rev4, 1L, "V1" ) );
        assertFalse( btree.contains( rev4, 3L, "V3" ) );
        assertTrue( btree.contains( rev4, 5L, "V5" ) );

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        assertTrue( btree1.hasKey( 1L ) );
        assertFalse( btree1.hasKey( 3L ) );
        assertTrue( btree1.hasKey( 5L ) );
        assertEquals( "V1", btree1.get( 1L ) );
        assertEquals( "V5", btree1.get( 5L ) );

        // Check that we can get a value from each revision
        // revision 1
        assertFalse( btree.contains( rev1, 1L, "V1" ) );
        assertFalse( btree.contains( rev1, 3L, "V3" ) );
        assertFalse( btree.contains( rev1, 5L, "V5" ) );

        // revision 2
        assertFalse( btree.contains( rev2, 1L, "V1" ) );
        assertFalse( btree.contains( rev2, 3L, "V3" ) );
        assertFalse( btree.contains( rev2, 5L, "V5" ) );

        // revision 3
        assertFalse( btree.contains( rev3, 1L, "V1" ) );
        assertFalse( btree.contains( rev3, 3L, "V3" ) );
        assertFalse( btree.contains( rev3, 5L, "V5" ) );

        // revision 4
        assertTrue( btree.contains( rev4, 1L, "V1" ) );
        assertFalse( btree.contains( rev4, 3L, "V3" ) );
        assertTrue( btree.contains( rev4, 5L, "V5" ) );
    }


    /**
     * Test a hasKey() from a given revision
     */
    @Test
    public void testHasKeyWithRevision() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        recordManager.setKeepRevisions( true );

        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );
        long rev1 = btree.getRevision();

        btree.insert( 1L, "V1" );
        long rev2 = btree.getRevision();

        btree.insert( 5L, "V5" );
        long rev3 = btree.getRevision();

        // Delete one element
        btree.delete( 3L );
        long rev4 = btree.getRevision();

        // Check that we can get a value from each revision
        // revision 1
        assertFalse( btree.hasKey( rev1, 1L ) );
        assertTrue( btree.hasKey( rev1, 3L ) );
        assertFalse( btree.hasKey( rev1, 5L ) );

        // revision 2
        assertTrue( btree.hasKey( rev2, 1L ) );
        assertTrue( btree.hasKey( rev2, 3L ) );
        assertFalse( btree.hasKey( rev2, 5L ) );

        // revision 3
        assertTrue( btree.hasKey( rev3, 1L ) );
        assertTrue( btree.hasKey( rev3, 3L ) );
        assertTrue( btree.hasKey( rev3, 5L ) );

        // revision 4
        assertTrue( btree.hasKey( rev4, 1L ) );
        assertFalse( btree.hasKey( rev4, 3L ) );
        assertTrue( btree.hasKey( rev4, 5L ) );

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getKeyComparator().getClass().getName(), btree1.getKeyComparator().getClass().getName() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        assertTrue( btree1.hasKey( 1L ) );
        assertFalse( btree1.hasKey( 3L ) );
        assertTrue( btree1.hasKey( 5L ) );
        assertEquals( "V1", btree1.get( 1L ) );
        assertEquals( "V5", btree1.get( 5L ) );

        // Check that we can get a value from each revision
        // revision 1
        assertFalse( btree.hasKey( rev1, 1L ) );
        assertFalse( btree.hasKey( rev1, 3L ) );
        assertFalse( btree.hasKey( rev1, 5L ) );

        // revision 2
        assertFalse( btree.hasKey( rev2, 1L ) );
        assertFalse( btree.hasKey( rev2, 3L ) );
        assertFalse( btree.hasKey( rev2, 5L ) );

        // revision 3
        assertFalse( btree.hasKey( rev3, 1L ) );
        assertFalse( btree.hasKey( rev3, 3L ) );
        assertFalse( btree.hasKey( rev3, 5L ) );

        // revision 4
        assertTrue( btree.hasKey( rev4, 1L ) );
        assertFalse( btree.hasKey( rev4, 3L ) );
        assertTrue( btree.hasKey( rev4, 5L ) );
    }


    /**
     * Test with BTrees containing duplicate keys
     */
    @Test
    public void testBTreesDuplicateKeys() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        int pageSize = 16;
        int numKeys = 1;
        String name = "duplicateTree";
        String[] testValues = new String[]
            { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F", "10" };

        BTree<Long, String> dupsTree = BTreeFactory.createPersistedBTree( name, LongSerializer.INSTANCE,
            StringSerializer.INSTANCE, pageSize, true );

        recordManager.manage( dupsTree );

        for ( long i = 0; i < numKeys; i++ )
        {
            for ( int k = 0; k < pageSize + 1; k++ )
            {
                dupsTree.insert( i, testValues[k] );
            }
        }

        // Now, try to reload the file back
        openRecordManagerAndBtree();

        dupsTree = recordManager.getManagedTree( name );

        for ( long i = 0; i < numKeys; i++ )
        {
            ValueCursor<String> values = dupsTree.getValues( i );

            for ( int k = 0; k < pageSize + 1; k++ )
            {
                assertTrue( values.next().equals( testValues[k] ) );
            }
        }
    }


    @Test
    public void testAdds() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        btree.insert( 1L, "V1" );
        btree.insert( 2L, "V2" );
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
        recordManager.beginTransaction();
        /*
        System.out.println( "Before V1" );
        for ( Long key : recordManager.writeCounter.keySet() )
        {
            System.out.println( "Page " + Long.toHexString( key ) + " written " + recordManager.writeCounter.get( key )
                + " times" );
        }
        */
        btree.insert( 1L, "V1" );
        /*
        for ( Long key : recordManager.writeCounter.keySet() )
        {
            System.out.println( "Page " + Long.toHexString( key ) + " written " + recordManager.writeCounter.get( key )
                + " times" );
        }
        
        System.out.println( "After V1" );
        */

        //System.out.println( "Before V2" );
        btree.insert( 2L, "V2" );
        //System.out.println( "After V2" );

        //System.out.println( "Before V3" );
        btree.insert( 3L, "V3" );
        /*
        for ( Long key : recordManager.writeCounter.keySet() )
        {
            System.out.println( "Page " + Long.toHexString( key ) + " written " + recordManager.writeCounter.get( key )
                + " times" );
        }
        */

        recordManager.commit();

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
