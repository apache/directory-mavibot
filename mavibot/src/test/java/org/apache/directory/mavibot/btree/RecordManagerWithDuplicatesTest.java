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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * test the RecordManager whith duplicate values
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RecordManagerWithDuplicatesTest
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
        btree.close();

        recordManager.close();
        assertTrue( recordManager.isContextOk() );

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
            btree = recordManager.getManagedTree( "test" );
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
        assertTrue( btree.isAllowDuplicates() );
    }


    /**
     * Test the creation of a RecordManager with a BTree containing data.
     */
    @Test
    public void testRecordManagerWithBTreeSameValue() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );
        btree.insert( 3L, "V5" );

        assertTrue( btree.contains( 3L, "V3" ) );
        assertTrue( btree.contains( 3L, "V5" ) );

        // Now, try to reload the file back
        openRecordManagerAndBtree();
        assertNotNull( btree );

        assertTrue( btree.contains( 3L, "V3" ) );
        assertTrue( btree.contains( 3L, "V5" ) );
    }


    /**
     * Test the creation of a RecordManager with a BTree containing data.
     */
    @Test
    public void testRecordManagerWithBTreeVariousValues() throws IOException, BTreeAlreadyManagedException,
        KeyNotFoundException
    {
        // Now, add some elements in the BTree
        for ( long i = 1; i < 128; i++ )
        {
            String v1 = "V" + i;
            btree.insert( i, v1 );

            String v2 = "V" + i + 1;
            btree.insert( i, v2 );
        }

        // Check that the elements are present
        for ( long i = 1; i < 128; i++ )
        {
            String v1 = "V" + i;
            String v2 = "V" + i + 1;
            assertTrue( btree.contains( i, v1 ) );
            assertTrue( btree.contains( i, v2 ) );

        }

        // Now, try to reload the file back
        openRecordManagerAndBtree();
        assertNotNull( btree );

        for ( long i = 1; i < 128; i++ )
        {
            String v1 = "V" + i;
            String v2 = "V" + i + 1;
            assertTrue( btree.contains( i, v1 ) );
            assertTrue( btree.contains( i, v2 ) );

        }
    }
}
