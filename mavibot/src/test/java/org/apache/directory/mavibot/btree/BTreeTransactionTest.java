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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test the PersistedBTree with transaction
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BTreeTransactionTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private File dataDir = null;
    private BTree<Long, String> btree = null;
    private RecordManager recordManager = null;


    @Before
    public void createBTree() throws IOException
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        openRecordManagerAndBtrees();

        try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
        {
            // Create a new BTree with transaction and another one without
            btree = recordManager.addBTree( writeTransaction, "testWithTxn", LongSerializer.INSTANCE, StringSerializer.INSTANCE );
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


    private void openRecordManagerAndBtrees()
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
                    btree = recordManager.getBtree( readTransaction, btree.getName(), 0L );
                }
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void testWithTransaction() throws IOException
    {
        long nbIteration = 100_000L;
        long t0 = System.currentTimeMillis();

        for ( long i = 0L; i < nbIteration; i++ )
        {
            try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
            {
                btree.insert( writeTransaction, i, Long.toString( i ) );
            }
        }

        long t1 = System.currentTimeMillis();

        System.out.println( "Delta with transaction for " + nbIteration + " elements = " + ( t1 - t0 ) );
    }
}
