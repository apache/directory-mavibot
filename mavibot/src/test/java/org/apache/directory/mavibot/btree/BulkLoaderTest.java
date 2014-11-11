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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.directory.mavibot.btree.BulkLoader.LevelEnum;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Test the BulkLoader class.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BulkLoaderTest
{
    private void checkBtree( BTree<Long, String> oldBtree, BTree<Long, String> newBtree )
        throws EndOfFileExceededException, IOException, KeyNotFoundException
    {
        assertEquals( oldBtree.getNbElems(), newBtree.getNbElems() );

        TupleCursor<Long, String> cursorOld = oldBtree.browse();
        TupleCursor<Long, String> cursorNew = newBtree.browse();

        while ( cursorOld.hasNext() && cursorNew.hasNext() )
        {
            Tuple<Long, String> tupleOld = cursorOld.next();
            Tuple<Long, String> tupleNew = cursorNew.next();

            assertEquals( tupleOld.getKey(), tupleNew.getKey() );
            assertEquals( tupleOld.getValue(), tupleNew.getValue() );
        }

        assertEquals( cursorOld.hasNext(), cursorNew.hasNext() );
    }


    /**
     * Test that we can compact a btree which has no element
     */
    @Test
    public void testInMemoryBulkLoadNoElement() throws IOException, KeyNotFoundException
    {
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        BTree<Long, String> newBtree = ( BTree<Long, String> ) BulkLoader.compact( btree );

        checkBtree( btree, newBtree );
        TupleCursor<Long, String> cursorOld = btree.browse();
        TupleCursor<Long, String> cursorNew = btree.browse();

        assertFalse( cursorOld.hasNext() );
        assertFalse( cursorNew.hasNext() );
    }


    /**
     * Test that we can compact a btree which has a partially full leaf only
     */
    @Ignore
    @Test
    public void testInMemoryBulkLoad3Elements() throws IOException, KeyNotFoundException
    {
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        for ( Long i = 0L; i < 3L; i++ )
        {
            String value = "V" + i;
            btree.insert( i, value );
        }

        BTree<Long, String> newBtree = ( BTree<Long, String> ) BulkLoader.compact( btree );

        checkBtree( btree, newBtree );
    }


    /**
     * Test that we can compact a btree which has a 2 full leaves
     */
    @Ignore
    @Test
    public void testInMemoryBulkLoad8Elements() throws IOException, KeyNotFoundException
    {
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        for ( Long i = 0L; i < 8L; i++ )
        {
            String value = "V" + i;
            btree.insert( i, value );
        }

        BTree<Long, String> newBtree = ( BTree<Long, String> ) BulkLoader.compact( btree );

        checkBtree( btree, newBtree );
    }


    /**
     * Test that we can compact a btree which has a few leaves, one being partially full
     * @throws BTreeAlreadyManagedException 
     */
    @Test
    public void testPersistedBulkLoad10Elements() throws IOException, KeyNotFoundException,
        BTreeAlreadyManagedException
    {
        for ( int i = 0; i < 1001; i++ )
        {
            Random random = new Random( System.currentTimeMillis() );
            File file = File.createTempFile( "managedbtreebuilder", ".data" );
            file.deleteOnExit();

            try
            {
                RecordManager rm = new RecordManager( file.getAbsolutePath() );
                PersistedBTree<Long, String> btree = ( PersistedBTree<Long, String> ) rm.addBTree( "test",
                    LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );

                BulkLoader<Long, String> bulkLoader = new BulkLoader<Long, String>();
                int nbElems = i;
                int addedElems = 0;

                final Tuple<Long, String>[] elems = new Tuple[nbElems];
                Map<Long, Tuple<Long, Set<String>>> expected = new HashMap<Long, Tuple<Long, Set<String>>>();

                long t00 = System.currentTimeMillis();

                while ( addedElems < nbElems )
                {
                    long key = random.nextLong() % 3333333L;

                    if ( expected.containsKey( key ) )
                    {
                        continue;
                    }

                    long w = random.nextLong() % 3333333L;
                    String value = "V" + w;
                    elems[addedElems] = new Tuple<Long, String>( key, value );

                    Tuple<Long, Set<String>> expectedTuple = expected.get( key );

                    if ( expectedTuple == null )
                    {
                        expectedTuple = new Tuple<Long, Set<String>>( key, new TreeSet<String>() );
                    }

                    expectedTuple.value.add( value );
                    expected.put( key, expectedTuple );
                    addedElems++;

                    if ( addedElems % 100 == 0 )
                    {
                        //System.out.println( "Nb added elements = " + addedElems );
                    }
                }
                long t01 = System.currentTimeMillis();

                // System.out.println( "Time to create the " + nbElems + " elements " + ( ( t01 - t00 ) / 1 ) );

                Iterator<Tuple<Long, String>> tupleIterator = new Iterator<Tuple<Long, String>>()
                {
                    private int pos = 0;


                    @Override
                    public Tuple<Long, String> next()
                    {
                        return elems[pos++];
                    }


                    @Override
                    public boolean hasNext()
                    {
                        return pos < elems.length;
                    }


                    @Override
                    public void remove()
                    {
                    }
                };

                long t0 = System.currentTimeMillis();
                BTree<Long, String> result = bulkLoader.load( btree, tupleIterator, 128 );
                long t1 = System.currentTimeMillis();

                System.out.println( "== Btree #" + i + ", Time to bulkoad the " + nbElems + " elements "
                    + ( t1 - t0 ) + "ms" );

                TupleCursor<Long, String> cursor = result.browse();
                int nbFetched = 0;

                long t2 = System.currentTimeMillis();

                while ( cursor.hasNext() )
                {
                    Tuple<Long, String> elem = cursor.next();

                    assertTrue( expected.containsKey( elem.key ) );
                    Tuple<Long, Set<String>> tuple = expected.get( elem.key );
                    assertNotNull( tuple );
                    nbFetched++;
                }

                long t3 = System.currentTimeMillis();

                //System.out.println( "Time to read the " + nbElems + " elements " + ( t3 - t2 ) );
                assertEquals( nbElems, nbFetched );

                checkBtree( btree, result );
            }
            finally
            {
                file.delete();
            }
        }
    }


    /**
     * Test that we can compact a btree which has a full parent node, with all the leaves full.
     */
    @Test
    public void testInMemoryBulkLoad20Elements() throws IOException, KeyNotFoundException
    {
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        for ( Long i = 0L; i < 20L; i++ )
        {
            String value = "V" + i;
            btree.insert( i, value );
        }

        BTree<Long, String> newBtree = ( BTree<Long, String> ) BulkLoader.compact( btree );

        checkBtree( btree, newBtree );
    }


    /**
     * Test that we can compact a btree which has two full parent nodes, with all the leaves full.
     * That means we have an upper node with one element.
     */
    @Ignore
    @Test
    public void testInMemoryBulkLoad40Elements() throws IOException, KeyNotFoundException
    {
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        for ( Long i = 0L; i < 40L; i++ )
        {
            String value = "V" + i;
            btree.insert( i, value );
        }

        BTree<Long, String> newBtree = ( BTree<Long, String> ) BulkLoader.compact( btree );

        checkBtree( btree, newBtree );
    }


    /**
     * Test that we can compact a btree which has two full parent nodes, with all the leaves full.
     * That means we have an upper node with one element.
     */
    @Test
    public void testInMemoryBulkLoad100Elements() throws IOException, KeyNotFoundException
    {
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        for ( Long i = 0L; i < 100L; i++ )
        {
            String value = "V" + i;
            btree.insert( i, value );
        }

        BTree<Long, String> newBtree = ( BTree<Long, String> ) BulkLoader.compact( btree );

        checkBtree( btree, newBtree );
    }


    @Ignore
    @Test
    public void testInMemoryBulkLoadN() throws IOException, KeyNotFoundException
    {
        Random random = new Random( System.nanoTime() );
        long t0 = System.currentTimeMillis();

        for ( long n = 0L; n < 2500L; n++ )
        {
            BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
                StringSerializer.INSTANCE );
            btree.setPageSize( 4 );

            for ( Long i = 0L; i < n; i++ )
            {
                String value = "V" + i;
                btree.insert( i, value );
            }

            //long t1 = System.currentTimeMillis();

            //System.out.println( "Delta initial load = " + ( t1 - t0 ) );

            //long t2 = System.currentTimeMillis();

            BTree<Long, String> newBtree = ( BTree<Long, String> ) BulkLoader.compact( btree );

            //long t3 = System.currentTimeMillis();

            //System.out.println( "Delta initial load = " + ( t3 - t2 ) );

            System.out.println( "Checking for N = " + n );
            checkBtree( btree, newBtree );
        }
    }


    @Ignore
    @Test
    public void testInMemoryBulkLoad21() throws IOException, KeyNotFoundException
    {
        Random random = new Random( System.nanoTime() );
        long t0 = System.currentTimeMillis();

        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        for ( Long i = 0L; i < 21; i++ )
        {
            String value = "V" + i;
            btree.insert( i, value );
        }

        //long t1 = System.currentTimeMillis();

        //System.out.println( "Delta initial load = " + ( t1 - t0 ) );

        //long t2 = System.currentTimeMillis();

        BTree<Long, String> newBtree = ( BTree<Long, String> ) BulkLoader.compact( btree );

        //long t3 = System.currentTimeMillis();

        //System.out.println( "Delta initial load = " + ( t3 - t2 ) );

        System.out.println( "Checking for N = " + 21 );
        checkBtree( btree, newBtree );
    }


    /**
     * test the computeLeafLevel method
     */
    @Test
    public void testPersistedBulkLoadComputeLeafLevel() throws IOException, KeyNotFoundException,
        BTreeAlreadyManagedException
    {
        Random random = new Random( System.currentTimeMillis() );
        File file = File.createTempFile( "managedbtreebuilder", ".data" );
        file.deleteOnExit();

        try
        {
            RecordManager rm = new RecordManager( file.getAbsolutePath() );
            PersistedBTree<Long, String> btree = ( PersistedBTree<Long, String> ) rm.addBTree( "test",
                LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );

            BulkLoader<Long, String> bulkLoader = new BulkLoader<Long, String>();

            int[] expectedNbPages = new int[]
                {
                    0,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3
            };

            int[] expectedLimit = new int[]
                {
                    0,
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    0, 0, 0, 0, 0, 0, 0, 16, 16, 16, 16, 16, 16, 16, 16, 32,
                    16, 16, 16, 16, 16, 16, 16, 32, 32, 32, 32, 32, 32, 32, 32, 48
            };

            int[] expectedKeys = new int[]
                {
                    0,
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    9, 10, 11, 12, 13, 14, 15, 16, 16, 16, 16, 16, 16, 16, 16, 16,
                    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16
            };

            for ( int i = 0; i < 49; i++ )
            {
                System.out.println( "=======================================" );
                System.out.println( "== Iteration n#" + i );
                System.out.println( "=======================================" );

                BulkLoader<Long, String>.LevelInfo leafInfo = bulkLoader.computeLevel( btree, i, LevelEnum.LEAF );

                assertEquals( expectedNbPages[i], leafInfo.nbPages );
                assertEquals( expectedLimit[i], leafInfo.nbElemsLimit );
                assertEquals( expectedKeys[i], leafInfo.currentPage.getNbElems() );
            }
        }
        finally
        {
            file.delete();
        }
    }


    /**
     * test the computeNodeLevel method
     */
    @Test
    public void testPersistedBulkLoadComputeNodeLevel() throws IOException, KeyNotFoundException,
        BTreeAlreadyManagedException
    {
        Random random = new Random( System.currentTimeMillis() );
        File file = File.createTempFile( "managedbtreebuilder", ".data" );
        file.deleteOnExit();

        try
        {
            RecordManager rm = new RecordManager( file.getAbsolutePath() );
            PersistedBTree<Long, String> btree = ( PersistedBTree<Long, String> ) rm.addBTree( "test",
                LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );

            BulkLoader<Long, String> bulkLoader = new BulkLoader<Long, String>();

            int[] expectedNbPages = new int[]
                {
                    -1, -1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3
            };

            int[] expectedLimit = new int[]
                {
                    -1,
                    -1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                    0, 0, 0, 0, 0, 0, 0, 0, 17, 17, 17, 17, 17, 17, 17, 17, 34,
                    17, 17, 17, 17, 17, 17, 17, 17, 34, 34, 34, 34, 34, 34, 34, 34, 51
            };

            int[] expectedKeys = new int[]
                {
                    -1, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    8, 9, 10, 11, 12, 13, 14, 15, 16, 16, 16, 16, 16, 16, 16, 16, 16,
                    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16
            };

            for ( int i = 2; i < 52; i++ )
            {
                System.out.println( "=======================================" );
                System.out.println( "== Iteration n#" + i );
                System.out.println( "=======================================" );

                BulkLoader<Long, String>.LevelInfo nodeInfo = bulkLoader.computeLevel( btree, i, LevelEnum.NODE );

                assertEquals( expectedNbPages[i], nodeInfo.nbPages );
                assertEquals( expectedLimit[i], nodeInfo.nbElemsLimit );
                assertEquals( expectedKeys[i], nodeInfo.currentPage.getNbElems() );
            }
        }
        finally
        {
            file.delete();
        }
    }


    /**
     * test the computeNodeLevel method
     */
    @Test
    public void testPersistedBulkLoadComputeLevels() throws IOException, KeyNotFoundException,
        BTreeAlreadyManagedException
    {
        Random random = new Random( System.currentTimeMillis() );
        File file = File.createTempFile( "managedbtreebuilder", ".data" );
        file.deleteOnExit();

        try
        {
            RecordManager rm = new RecordManager( file.getAbsolutePath() );
            PersistedBTree<Long, String> btree = ( PersistedBTree<Long, String> ) rm.addBTree( "test",
                LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );

            BulkLoader<Long, String> bulkLoader = new BulkLoader<Long, String>();

            int[] expectedNbPages = new int[]
                {
                    -1, -1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3
            };

            int[] expectedLimit = new int[]
                {
                    -1,
                    -1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                    0, 0, 0, 0, 0, 0, 0, 0, 17, 17, 17, 17, 17, 17, 17, 17, 34,
                    17, 17, 17, 17, 17, 17, 17, 17, 34, 34, 34, 34, 34, 34, 34, 34, 51
            };

            int[] expectedKeys = new int[]
                {
                    -1, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                    8, 9, 10, 11, 12, 13, 14, 15, 16, 16, 16, 16, 16, 16, 16, 16, 16,
                    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16
            };

            for ( int i = 2599; i <= 2599; i++ )
            {
                System.out.println( "=======================================" );
                System.out.println( "== Iteration #" + i );
                System.out.println( "=======================================" );

                List<BulkLoader<Long, String>.LevelInfo> levels = bulkLoader.computeLevels( btree, i );

                for ( BulkLoader<Long, String>.LevelInfo level : levels )
                {
                    System.out.println( level );
                }
            }
        }
        finally
        {
            file.delete();
        }
    }
}
