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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.mavibot.btree.serializer.LongSerializer;
import org.apache.mavibot.btree.serializer.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * A class to test multi-threaded operations on the btree
 *  
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class MultiThreadedBtreeTest
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
        btree = new BTree<Long, String>( new LongSerializer(), new StringSerializer() );
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
     * Create a btree with 500 000 elements in it
     * @throws IOException If the creation failed
     */
    private void create500KBTree() throws IOException
    {
        Random random = new Random( System.nanoTime() );

        int nbElems = 500000;

        // Create a BTree with 500 000 entries
        btree.setPageSize( 32 );

        for ( int i = 0; i < nbElems; i++ )
        {
            Long key = ( long ) random.nextLong();
            String value = Long.toString( key );

            try
            {
                btree.insert( key, value );

                if ( i % 100000 == 0 )
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
    private int testBrowse() throws IOException
    {
        Cursor<Long, String> cursor = btree.browse();

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

        long revision = cursor.getRevision();

        cursor.close();

        //System.out.println( Thread.currentThread().getName() + " Nb elements read : " + nb + " on revision : "
        //    + revision );

        return nb;
    }


    /**
     * Chack that we can read the btree while it is being modified. We will start
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
                    create500KBTree();
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

        System.out.println( " Time to create 500K entries and to have " + nbThreads + " threads reading them : "
            + ( ( t1 - t0 ) / 1000 ) + " seconds" );
    }


    /**
     * Test that we can use many threads inserting data in a BTree
     * @throws InterruptedException
     */
    @Test
    public void testInsertMultiThreads() throws InterruptedException, IOException
    {
        int nbThreads = 20;
        final CountDownLatch latch = new CountDownLatch( nbThreads );

        //Thread.sleep( 60000L );

        long t0 = System.currentTimeMillis();

        for ( int i = 0; i < nbThreads; i++ )
        {
            final long prefix = i;
            Thread test = new Thread()
            {
                public void run()
                {
                    try
                    {
                        // Inject 10000 elements
                        for ( int j = 0; j < 10000; j++ )
                        {
                            long value = prefix * 10000 + j;
                            btree.insert( value, Long.toString( value ) );

                            /*
                            if ( j % 10000 == 0 )
                            {
                                System.out.println( "Thread " + Thread.currentThread().getName() + " flushed " + j
                                    + " elements" );
                            }
                            */
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

        // Check that the tree contains all the values
        try
        {
            for ( long i = 0L; i < 100000L; i++ )
            {
                assertEquals( Long.toString( i ), btree.get( i ) );
            }
        }
        catch ( KeyNotFoundException knfe )
        {
            fail();
        }

        System.out.println( " Time to create 1M entries : "
            + ( ( t1 - t0 ) / 1000 ) + " seconds" );
    }
}
