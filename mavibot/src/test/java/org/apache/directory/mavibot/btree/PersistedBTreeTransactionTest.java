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

import static org.junit.Assert.assertTrue;

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
public class PersistedBTreeTransactionTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private File dataDirWithTxn = null;
    private File dataDirNoTxn = null;
    private BTree<Long, String> btreeWithTransactions = null;
    private BTree<Long, String> btreeNoTransactions = null;
    private RecordManager recordManagerTxn = null;
    private RecordManager recordManagerNoTxn = null;


    @Before
    public void createBTree() throws IOException
    {
        dataDirWithTxn = tempFolder.newFolder( UUID.randomUUID().toString() );
        dataDirNoTxn = tempFolder.newFolder( UUID.randomUUID().toString() );

        openRecordManagerAndBtrees();

        try
        {
            // Create a new BTree with transaction and another one without
            btreeWithTransactions = recordManagerTxn.addBTree( "testWithTxn", LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );
            btreeNoTransactions = recordManagerNoTxn.addBTree( "testNoTxn", LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    @After
    public void cleanup() throws IOException
    {
        btreeNoTransactions.close();
        btreeWithTransactions.close();

        recordManagerNoTxn.close();
        recordManagerTxn.close();
        
        assertTrue( recordManagerNoTxn.isContextOk() );
        assertTrue( recordManagerTxn.isContextOk() );

        if ( dataDirNoTxn.exists() )
        {
            FileUtils.deleteDirectory( dataDirNoTxn );
        }

        if ( dataDirWithTxn.exists() )
        {
            FileUtils.deleteDirectory( dataDirWithTxn );
        }
    }


    private void openRecordManagerAndBtrees()
    {
        try
        {
            if ( recordManagerTxn != null )
            {
                recordManagerTxn.close();
            }

            if ( recordManagerNoTxn != null )
            {
                recordManagerNoTxn.close();
            }

            // Now, try to reload the file back
            recordManagerTxn = new RecordManager( dataDirWithTxn.getAbsolutePath() );
            recordManagerNoTxn = new RecordManager( dataDirNoTxn.getAbsolutePath() );

            // load the last created btree
            if ( btreeWithTransactions != null )
            {
                btreeWithTransactions = recordManagerTxn.getManagedTree( btreeWithTransactions.getName() );
            }

            if ( btreeNoTransactions != null )
            {
                btreeNoTransactions = recordManagerNoTxn.getManagedTree( btreeNoTransactions.getName() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void testWithoutTransaction() throws IOException
    {
        long t0 = System.currentTimeMillis();

        for ( long i = 0L; i < 1000L; i++ )
        {
            btreeNoTransactions.insert( i, Long.toString( i ) );
        }

        long t1 = System.currentTimeMillis();

        System.out.println( "Delta without transaction for 100K elements = " + ( t1 - t0 ) );
    }


    @Test
    @Ignore("Fails atm")
    public void testWithTransaction() throws IOException
    {
        long t0 = System.currentTimeMillis();

        for ( long i = 0L; i < 1000L; i++ )
        {
            System.out.println( i );
            //btreeWithTransactions.beginTransaction();
            btreeWithTransactions.insert( i, Long.toString( i ) );
            //btreeWithTransactions.commit();
        }

        long t1 = System.currentTimeMillis();

        System.out.println( "Delta with transaction for 100K elements = " + ( t1 - t0 ) );
    }
}
