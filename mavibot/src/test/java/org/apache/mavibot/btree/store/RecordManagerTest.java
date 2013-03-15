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
package org.apache.mavibot.btree.store;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.mavibot.btree.BTree;
import org.apache.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.mavibot.btree.exception.KeyNotFoundException;
import org.apache.mavibot.btree.serializer.LongSerializer;
import org.apache.mavibot.btree.serializer.StringSerializer;
import org.junit.Test;


/**
 * test the RecordManager
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class RecordManagerTest
{
    /**
     * Test the creation of a RecordManager, and that we can read it back.  
     */
    @Test
    public void testRecordManager() throws IOException, BTreeAlreadyManagedException
    {
        File tempFile = File.createTempFile( "mavibot", ".db" );
        String tempFileName = tempFile.getAbsolutePath();
        tempFile.deleteOnExit();

        RecordManager recordManager = new RecordManager( tempFileName, 32 );

        assertNotNull( recordManager );

        // Create a new BTree
        BTree<Long, String> btree = new BTree<Long, String>( "test", new LongSerializer(), new StringSerializer() );

        // And make it managed by the RM
        recordManager.manage( btree );

        // Close the recordManager
        recordManager.close();

        // Now, try to reload the file back
        RecordManager recordManager1 = new RecordManager( tempFileName );

        assertEquals( 1, recordManager1.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager1.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree btree1 = recordManager1.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getComparator().getClass().getName(), btree1.getComparator().getClass().getName() );
        assertEquals( btree.getFile(), btree1.getFile() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        recordManager1.close();
    }


    /**
     * Test the creation of a RecordManager with a BTree containing data.
     */
    @Test
    public void testRecordManagerWithBTree() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        File tempFile = File.createTempFile( "mavibot", ".db" );
        String tempFileName = tempFile.getAbsolutePath();
        tempFile.deleteOnExit();

        RecordManager recordManager = new RecordManager( tempFileName, 32 );

        assertNotNull( recordManager );

        // Create a new BTree
        BTree<Long, String> btree = new BTree<Long, String>( "test", new LongSerializer(), new StringSerializer() );

        // And make it managed by the RM
        recordManager.manage( btree );

        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );
        btree.insert( 1L, "V1" );
        btree.insert( 5L, "V5" );

        // Close the recordManager
        recordManager.close();

        // Now, try to reload the file back
        RecordManager recordManager1 = new RecordManager( tempFileName );

        assertEquals( 1, recordManager1.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager1.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager1.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getComparator().getClass().getName(), btree1.getComparator().getClass().getName() );
        assertEquals( btree.getFile(), btree1.getFile() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        assertTrue( btree1.exist( 1L ) );
        assertTrue( btree1.exist( 3L ) );
        assertTrue( btree1.exist( 5L ) );
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
        File tempFile = File.createTempFile( "mavibot", ".db" );
        String tempFileName = tempFile.getAbsolutePath();
        tempFile.deleteOnExit();

        RecordManager recordManager = new RecordManager( tempFileName, 32 );

        assertNotNull( recordManager );

        // Create a new BTree
        BTree<Long, String> btree = new BTree<Long, String>( "test", new LongSerializer(), new StringSerializer() );

        // And make it managed by the RM
        recordManager.manage( btree );

        // Now, add some elements in the BTree
        for ( long i = 1L; i < 32L; i++ )
        {
            btree.insert( i, "V" + i );
        }

        // Close the recordManager
        recordManager.close();

        // Now, try to reload the file back
        RecordManager recordManager1 = new RecordManager( tempFileName );

        assertEquals( 1, recordManager1.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager1.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree<Long, String> btree1 = recordManager1.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getComparator().getClass().getName(), btree1.getComparator().getClass().getName() );
        assertEquals( btree.getFile(), btree1.getFile() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );

        // Check the stored element
        for ( long i = 1L; i < 32L; i++ )
        {
            assertTrue( btree1.exist( i ) );
            assertEquals( "V" + i, btree1.get( i ) );
        }
    }
}
