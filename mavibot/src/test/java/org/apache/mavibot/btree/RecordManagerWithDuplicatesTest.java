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
package org.apache.mavibot.btree;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.mavibot.btree.exception.KeyNotFoundException;
import org.apache.mavibot.btree.serializer.LongSerializer;
import org.apache.mavibot.btree.serializer.StringSerializer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * test the RecordManager whith duplicate values
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
//@Ignore("ignoring till RM functionality is standardized")
public class RecordManagerWithDuplicatesTest
{
    private BTree<Long, String> btree = null;

    private RecordManager recordManager = null;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dataDir = null;


    @Before
    public void createBTree()
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        openRecordManagerAndBtree();

        try
        {
            // Create a new BTree which allows duplicate values
            btree = recordManager.addBTree( "test", new LongSerializer(), new StringSerializer(), true );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
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
    public void testRecordManager() throws IOException, BTreeAlreadyManagedException
    {
        assertEquals( 1, recordManager.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree btree1 = recordManager.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getComparator().getClass().getName(), btree1.getComparator().getClass().getName() );
        assertEquals( btree.getFile(), btree1.getFile() );
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
    @Ignore("This test is failing atm...")
    public void testRecordManagerWithBTree() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        // Now, add some elements in the BTree
        btree.insert( 3L, "V3" );

        // Now, try to reload the file back
        openRecordManagerAndBtree();
    }
}
