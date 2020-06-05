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
        
        //recordManager.dump();

        try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
        {
            // Create a new BTree with transaction and another one without
            btree = recordManager.addBTree( writeTransaction, "testWithTxn", LongSerializer.INSTANCE, StringSerializer.INSTANCE );

            //recordManager.dump();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }

        recordManager.dump();
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
            recordManager = new RecordManager( dataDir.getAbsolutePath(), 512, 1024 );
            
            System.out.println( "File : " + dataDir.getAbsolutePath() );

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
        long nbPerLoop = 100L;
        long nbIteration = 10_000L / nbPerLoop;
        long t0 = System.currentTimeMillis();

        for ( long i = 0L; i < nbIteration; i++ )
        {
            try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
            {
                for ( int j = 0; j < nbPerLoop; j++ )
                {
                    long key = i * nbPerLoop + j;
                    btree.insert( writeTransaction, key , Long.toString( key ) );
                }
            }
            
            recordManager.dump();
        }

        System.out.println( "File size : " + recordManager.fileChannel.size() );
        long t1 = System.currentTimeMillis();

        System.out.println( "Delta with transaction for " + nbIteration + " elements = " + ( t1 - t0 ) );
    }

    
    private class MultiThreadedTest extends Thread
    {
        private long delay;
        private boolean stopped = false;
        
        private MultiThreadedTest( int i, long delay )
        {
            super();
            this.setName( "Worker-Thread" + i );
            this.delay = delay;
        }
        
        @Override
        public void run()
        {
            while ( !stopped )
            {
                try
                {
                    sleep( delay );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
                
                try ( Transaction readTransaction = recordManager.beginReadTransaction() )
                {
                    //recordManager.dump( Thread.currentThread(), readTransaction.getRecordManagerHeader() );
                    BTree<Long, String> btree = readTransaction.getBTree( "testWithTxn" );
                    //System.out.println( btree.getBtreeHeader() );

                    TupleCursor cursor = btree.browse( readTransaction );
                    //System.out.println( "Nb elements to read (" + btree.getNbElems() + ")\n" );
                    
                    long nbElems = 0;
                    try
                    {
                        while ( cursor.hasNext() )
                        {
                            cursor.next();
                            nbElems++;
                        }
                    }
                    catch ( NullPointerException npe )
                    {
                        cursor = btree.browse( readTransaction );
                        nbElems = 0;
                        while ( cursor.hasNext() )
                        {
                            cursor.next();
                            nbElems++;
                            System.out.print( nbElems + "/" );
                        }
                        
                        System.out.println();
                    }
                    //System.out.println( "Thread[" + Thread.currentThread() + "], nRead " + nbElems + " nb elements (" + btree.getNbElems() + ")" );
                    cursor.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
        
        public void abort()
        {
            stopped = true;
        }
    }

    @Test
    public void testMultiThreadsWithTransaction() throws IOException, InterruptedException
    {
        int nbThreads = 20;
        MultiThreadedTest[] readThreads = new MultiThreadedTest[nbThreads];

        for ( int i = 0; i < nbThreads; ++i )
        {
            readThreads[i] = new MultiThreadedTest( i, ( i + 1 ) * 10L );
            readThreads[i].start();
        }
        
        //recordManager.dump();
        
        int nbElements = 1000;
        long nbPerLoop = 4L;
        long nbIteration = nbElements / nbPerLoop;
        long t0 = System.currentTimeMillis();

        //recordManager.dump();
        
        for ( long i = 0L; i < nbIteration; i++ )
        {
            try ( WriteTransaction writeTransaction = recordManager.beginWriteTransaction() )
            {
                for ( int j = 0; j < nbPerLoop; j++ )
                {
                    long key = i * nbPerLoop + j;
                    BTree<Long, String> btree = writeTransaction.getBTree( "testWithTxn" );

                    btree.insert( writeTransaction, key , Long.toString( key ) );
                }
            }
            
            Thread.sleep(10);
            //recordManager.dump();
        }

        System.out.println( "File size : " + recordManager.fileChannel.size() );
        long t1 = System.currentTimeMillis();

        System.out.println( "Delta with transaction for " + nbElements  + " elements = " + ( t1 - t0 ) );
        
        try ( Transaction readTransaction = recordManager.beginReadTransaction() )
        {
            //recordManager.dump( Thread.currentThread(), readTransaction.getRecordManagerHeader() );
            BTree<Long, String> btree = readTransaction.getBTree( "testWithTxn" );
            System.out.println( btree.getBtreeHeader() );

            TupleCursor cursor = btree.browse( readTransaction );
            
            long nbElems = 0;
            while ( cursor.hasNext() )
            {
                Tuple<Long, String> tuple = cursor.next();
                
                assertTrue( ( long )nbElems == tuple.key );
                nbElems++;
            }
            
            System.out.println( "Read " + nbElems + " nb elements (" + btree.getNbElems() + ")" );
            cursor.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

        //Thread.sleep( 1000000L );
        
        for ( int i = 0; i < nbThreads; ++i )
        {
            readThreads[i].abort();
        }

        for ( int i = 0; i < nbThreads; ++i )
        {
            readThreads[i].join();
        }
        //Thread.sleep( 1000000L );
    }
}
