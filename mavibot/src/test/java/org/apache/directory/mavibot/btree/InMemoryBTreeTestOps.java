package org.apache.directory.mavibot.btree;


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
import java.io.IOException;
import java.util.Random;

import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * A class to test multi-threaded operations on the btree
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryBTreeTestOps
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
     * Create a btree with 500 000 elements in it
     * @throws IOException If the creation failed
     */
    private void createTree() throws IOException
    {
        Random random = new Random( System.nanoTime() );

        int nbElems = 50000;

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


    @Test
    public void testCreateTree() throws InterruptedException, IOException
    {

        long t0 = System.currentTimeMillis();

        // Start the writer
        createTree();
        long t1 = System.currentTimeMillis();
        System.out.println( "Time to create a tree with 500 000 elements in memory:" + ( ( t1 - t0 ) )
            + " milliseconds" );
    }
}
