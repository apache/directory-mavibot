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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;


/**
 * A class to test multi-threaded operations on the btree
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class MultiThreadedInMemoryBtreeTest
{
    /** The btree we use */
    private static BTree<Long, String> btree;


    /**
     * Create the btree once
     * @throws IOException If the creation failed
     */
    @BeforeClass
    public static void setup() throws IOException
    {
        btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE );
    }


    /**
     * Close the btree
     */
    @AfterClass
    public static void shutdown() throws IOException
    {
        btree.close();
    }


    /**
     * Create a btree with 50 000 elements in it
     * @throws IOException If the creation failed
     */
    private void create50KBTree() throws IOException
    {
        Random random = new Random( System.nanoTime() );

        int nbElems = 50000;

        // Create a BTree with 50 000 entries
        btree.setPageSize( 32 );

        for ( int i = 0; i < nbElems; i++ )
        {
            Long key = ( long ) random.nextLong();
            String value = Long.toString( key );

            try
            {
                btree.insert( key, value );

                if ( i % 10000 == 0 )
                {
                    System.out.println( "Written " + i + " elements" );
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                System.out.println( btree );
                System.out.println( "Error while adding " + value );
                return;
            }
        }
    }


    /**
     * Browse the btree in its current revision, reading all of its elements
     * @return The number of read elements
     * @throws IOException If the browse failed
     */
    private int testBrowse() throws IOException, KeyNotFoundException
    {
        TupleCursor<Long, String> cursor = btree.browse();

        int nb = 0;
        long elem = Long.MIN_VALUE;

        while ( cursor.hasNext() )
        {
            Tuple<Long, String> res = cursor.next();

            if ( res.getKey() > elem )
            {
                elem = res.getKey();
                nb++;
            }
        }

        cursor.close();

        return nb;
    }


    /**
     * Check that we can read the btree while it is being modified. We will start
     * 100 readers for one writer.
     *
     * @throws InterruptedException If the btree access failed.
     */
    @Test
    public void testBrowseMultiThreads() throws InterruptedException
    {
        int nbThreads = 100;
        final CountDownLatch latch = new CountDownLatch( nbThreads );

        Thread writer = new Thread()
        {
            public void run()
            {
                try
                {
                    create50KBTree();
                }
                catch ( Exception e )
                {
                }
            }
        };

        long t0 = System.currentTimeMillis();

        // Start the writer
        writer.start();

        for ( int i = 0; i < nbThreads; i++ )
        {
            Thread test = new Thread()
            {
                public void run()
                {
                    try
                    {
                        int res = 0;
                        int previous = -1;

                        while ( previous < res )
                        {
                            previous = res;
                            res = testBrowse();
                            Thread.sleep( 500 );
                        }

                        latch.countDown();
                    }
                    catch ( Exception e )
                    {
                    }
                }
            };

            // Start each reader
            test.start();
        }

        // Wait for all the readers to be done
        latch.await();

        long t1 = System.currentTimeMillis();

        System.out.println( " Time to create 50K entries and to have " + nbThreads + " threads reading them : "
            + ( ( t1 - t0 ) / 1000 ) + " seconds" );
    }


    /**
     * Test that we can use many threads inserting data in a BTree
     * @throws InterruptedException
     */
    @Test
    public void testInsertMultiThreads() throws InterruptedException, IOException
    {
        int nbThreads = 100;
        final CountDownLatch latch = new CountDownLatch( nbThreads );
        final AtomicBoolean error = new AtomicBoolean(false);

        //Thread.sleep( 60000L );

        long t0 = System.currentTimeMillis();

        class MyThread extends Thread
        {
            private int prefix = 0;

            public void run()
            {
                try
                {
                    // Inject 1000 elements
                    for ( int j = 0; j < 1000; j++ )
                    {
                        long value = prefix * 1000 + j;
                        String valStr = Long.toString( value );
                        //System.out.println( "---------------------------Inserting " + valStr + " for Thread " + Thread.currentThread().getName() );
                        btree.insert( value, valStr );

                        if ( j % 100 == 0 )
                        {
                            //System.out.println( "---------------------------Inserting " + valStr + " for Thread " + Thread.currentThread().getName() );
//                            long res = checkBtree( prefix, 1000, j );
//
//                            if ( res != -1L )
//                            {
//                                //retry
//                                System.out.println( "Failure to retrieve " + j );
//                                latch.countDown();
//                                error.set( true );
//                                return;
//                            }
                        }
                    }

                    latch.countDown();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    System.out.println( e.getMessage() );
                }
            }

            public MyThread( int prefix )
            {
                this.prefix = prefix;
            }
        }

        for ( int i = 0; i < nbThreads; i++ )
        {
            MyThread test = new MyThread( i );

            // Start each reader
            test.start();
        }

        // Wait for all the readers to be done
        latch.await();

        if ( error.get() )
        {
            System.out.println( "ERROR -----------------" );
            return;
        }

        long t1 = System.currentTimeMillis();

        // Check that the tree contains all the values
        assertEquals( -1L, checkBtree( 1000, nbThreads ) );

        System.out.println( " Time to create 1M entries : "
            + ( ( t1 - t0 ) ) + " milliseconds" );
    }


    private long checkBtree( int prefix, int nbElems, int currentElem ) throws IOException
    {
        long i = 0L;

        try
        {
            for ( i = 0L; i < currentElem; i++ )
            {
                long key = prefix * nbElems + i;
                assertEquals( Long.toString( key ), btree.get( key ) );
            }

            return -1L;
        }
        catch ( KeyNotFoundException knfe )
        {
            System.out.println( "cannot find " + ( prefix * nbElems + i ) );
            return i;
        }
    }


    private long checkBtree( int nbElems, int nbThreads ) throws IOException
    {
        long i = 0L;

        try
        {
            for ( long j = 0; j < nbThreads; j++ )
            {
                for ( i = 0L; i < nbElems; i++ )
                {
                    long key = j * nbElems + i;
                    assertEquals( Long.toString( key ), btree.get( key ) );
                }
            }

            return -1L;
        }
        catch ( KeyNotFoundException knfe )
        {
            System.out.println( "cannot find " + i );
            return i;
        }
    }
}
