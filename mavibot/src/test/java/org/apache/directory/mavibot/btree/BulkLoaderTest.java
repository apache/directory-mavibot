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
import java.util.ArrayList;
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
     * Test that we can load 100 BTrees with 0 to 1000 elements
     * @throws BTreeAlreadyManagedException 
     */
    @Test
    public void testPersistedBulkLoad1000Elements() throws IOException, KeyNotFoundException,
        BTreeAlreadyManagedException
    {
        for ( int i = 1000000; i < 1000001; i++ )
        {
            Random random = new Random( System.currentTimeMillis() );
            File file = File.createTempFile( "managedbtreebuilder", ".data" );
            file.deleteOnExit();

            try
            {
                RecordManager rm = new RecordManager( file.getAbsolutePath() );
                PersistedBTree<Long, String> btree = ( PersistedBTree<Long, String> ) rm.addBTree( "test",
                    LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );
                btree.setPageSize( 64 );

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
                BTree<Long, String> result = BulkLoader.load( btree, tupleIterator, 1024000 );
                long t1 = System.currentTimeMillis();

                if ( ( i % 100 ) == 0 )
                {
                    System.out.println( "== Btree #" + i + ", Time to bulkoad the " + nbElems + " elements "
                        + ( t1 - t0 ) + "ms" );
                }

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

            //System.out.println( "Checking for N = " + n );
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

        //System.out.println( "Checking for N = " + 21 );
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
                LevelInfo<Long, String> leafInfo = BulkLoader.computeLevel( btree, i, LevelEnum.LEAF );

                assertEquals( expectedNbPages[i], leafInfo.getNbPages() );
                assertEquals( expectedLimit[i], leafInfo.getNbElemsLimit() );
                assertEquals( expectedKeys[i], leafInfo.getCurrentPage().getNbElems() );
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
                LevelInfo<Long, String> nodeInfo = BulkLoader.computeLevel( btree, i, LevelEnum.NODE );

                assertEquals( expectedNbPages[i], nodeInfo.getNbPages() );
                assertEquals( expectedLimit[i], nodeInfo.getNbElemsLimit() );
                assertEquals( expectedKeys[i], nodeInfo.getCurrentPage().getNbElems() );
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
                List<LevelInfo<Long, String>> levels = BulkLoader.computeLevels( btree, i );
            }
        }
        finally
        {
            file.delete();
        }
    }


    /**
     * Test that we can load 100 BTrees with 0 to 1000 elements, each one of them having multiple values
     * @throws BTreeAlreadyManagedException 
     */
    //@Ignore("The test is failing atm due to the sub-btree construction which is not working correctly when we have too many elements")
    @Test
    public void testPersistedBulkLoad1000ElementsMultipleValues() throws IOException, KeyNotFoundException,
        BTreeAlreadyManagedException
    {
        for ( int i = 1; i < 1001; i++ )
        {
            Random random = new Random( System.currentTimeMillis() );
            File file = File.createTempFile( "managedbtreebuilder", ".data" );
            file.deleteOnExit();

            try
            {
                RecordManager rm = new RecordManager( file.getAbsolutePath() );
                PersistedBTree<Long, String> btree = ( PersistedBTree<Long, String> ) rm.addBTree( "test",
                    LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );

                int nbElems = i;
                int addedElems = 0;

                final Tuple<Long, String>[] elems = new Tuple[nbElems];
                Map<Long, Tuple<Long, Set<String>>> expected = new HashMap<Long, Tuple<Long, Set<String>>>();
                long valueNumber = 0;

                long t00 = System.currentTimeMillis();

                while ( addedElems < nbElems )
                {
                    long key = random.nextLong() % 33L;
                    String value = "V" + valueNumber++;

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
                BTree<Long, String> result = BulkLoader.load( btree, tupleIterator, 128 );
                long t1 = System.currentTimeMillis();

                //System.out.println( "== Btree #" + i + ", Time to bulkoad the " + nbElems + " elements "
                //    + ( t1 - t0 ) + "ms" );

                TupleCursor<Long, String> cursor = result.browse();
                int nbFetched = 0;

                long t2 = System.currentTimeMillis();

                try
                {
                    while ( cursor.hasNext() )
                    {
                        Tuple<Long, String> elem = cursor.next();

                        assertTrue( expected.containsKey( elem.key ) );
                        Tuple<Long, Set<String>> tuple = expected.get( elem.key );
                        assertNotNull( tuple );
                        nbFetched++;
                    }
                }
                catch ( Exception e )
                {
                    for ( Tuple<Long, String> tuple : elems )
                    {
                        System.out
                            .println( "listTuples.add( new Tuple<Long, String>( " + tuple.getKey() + "L, \""
                                + tuple.getValue() + "\" ) );" );
                    }

                    e.printStackTrace();
                    break;
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
     * Test that we can load 100 BTrees with 0 to 1000 elements, each one of them having multiple values
     * @throws BTreeAlreadyManagedException 
     */
    @Test
    public void testPersistedBulkLoad1000ElementsMultipleValuesDebug() throws IOException, KeyNotFoundException,
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

            int nbElems = 4;
            int addedElems = 0;

            final Tuple<Long, String>[] elems = new Tuple[nbElems];
            Map<Long, Tuple<Long, Set<String>>> expected = new HashMap<Long, Tuple<Long, Set<String>>>();
            long valueNumber = 0;

            elems[0] = new Tuple<Long, String>( 26L, "V0" );
            elems[1] = new Tuple<Long, String>( 26L, "V1" );
            elems[2] = new Tuple<Long, String>( -22L, "V2" );
            elems[3] = new Tuple<Long, String>( 5L, "V3" );

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
            BTree<Long, String> result = null;

            result = BulkLoader.load( btree, tupleIterator, 128 );
            long t1 = System.currentTimeMillis();

            TupleCursor<Long, String> cursor = result.browse();
            int nbFetched = 0;

            long t2 = System.currentTimeMillis();

            while ( cursor.hasNext() )
            {
                Tuple<Long, String> elem = cursor.next();
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


    @Test
    public void testDebug() throws IOException
    {
        final List<Tuple<Long, String>> listTuples = new ArrayList<Tuple<Long, String>>();

        listTuples.add( new Tuple<Long, String>( 0L, "V0" ) );
        listTuples.add( new Tuple<Long, String>( -14L, "V1" ) );
        listTuples.add( new Tuple<Long, String>( 7L, "V2" ) );
        listTuples.add( new Tuple<Long, String>( 6L, "V3" ) );
        listTuples.add( new Tuple<Long, String>( -12L, "V4" ) );
        listTuples.add( new Tuple<Long, String>( 17L, "V5" ) );
        listTuples.add( new Tuple<Long, String>( -18L, "V6" ) );
        listTuples.add( new Tuple<Long, String>( 7L, "V7" ) );
        listTuples.add( new Tuple<Long, String>( 32L, "V8" ) );
        listTuples.add( new Tuple<Long, String>( -21L, "V9" ) );
        listTuples.add( new Tuple<Long, String>( 9L, "V10" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V11" ) );
        listTuples.add( new Tuple<Long, String>( -7L, "V12" ) );
        listTuples.add( new Tuple<Long, String>( -13L, "V13" ) );
        listTuples.add( new Tuple<Long, String>( 23L, "V14" ) );
        listTuples.add( new Tuple<Long, String>( -1L, "V15" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V16" ) );
        listTuples.add( new Tuple<Long, String>( -13L, "V17" ) );
        listTuples.add( new Tuple<Long, String>( 9L, "V18" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V19" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V20" ) );
        listTuples.add( new Tuple<Long, String>( 7L, "V21" ) );
        listTuples.add( new Tuple<Long, String>( 28L, "V22" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V23" ) );
        listTuples.add( new Tuple<Long, String>( 3L, "V24" ) );
        listTuples.add( new Tuple<Long, String>( -31L, "V25" ) );
        listTuples.add( new Tuple<Long, String>( -14L, "V26" ) );
        listTuples.add( new Tuple<Long, String>( -1L, "V27" ) );
        listTuples.add( new Tuple<Long, String>( 5L, "V28" ) );
        listTuples.add( new Tuple<Long, String>( 29L, "V29" ) );
        listTuples.add( new Tuple<Long, String>( -24L, "V30" ) );
        listTuples.add( new Tuple<Long, String>( 8L, "V31" ) );
        listTuples.add( new Tuple<Long, String>( -1L, "V32" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V33" ) );
        listTuples.add( new Tuple<Long, String>( -24L, "V34" ) );
        listTuples.add( new Tuple<Long, String>( -7L, "V35" ) );
        listTuples.add( new Tuple<Long, String>( -3L, "V36" ) );
        listTuples.add( new Tuple<Long, String>( -7L, "V37" ) );
        listTuples.add( new Tuple<Long, String>( -9L, "V38" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V39" ) );
        listTuples.add( new Tuple<Long, String>( -27L, "V40" ) );
        listTuples.add( new Tuple<Long, String>( 19L, "V41" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V42" ) );
        listTuples.add( new Tuple<Long, String>( -14L, "V43" ) );
        listTuples.add( new Tuple<Long, String>( -4L, "V44" ) );
        listTuples.add( new Tuple<Long, String>( -2L, "V45" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V46" ) );
        listTuples.add( new Tuple<Long, String>( -21L, "V47" ) );
        listTuples.add( new Tuple<Long, String>( 17L, "V48" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V49" ) );
        listTuples.add( new Tuple<Long, String>( -11L, "V50" ) );
        listTuples.add( new Tuple<Long, String>( -23L, "V51" ) );
        listTuples.add( new Tuple<Long, String>( 3L, "V52" ) );
        listTuples.add( new Tuple<Long, String>( 4L, "V53" ) );
        listTuples.add( new Tuple<Long, String>( -28L, "V54" ) );
        listTuples.add( new Tuple<Long, String>( 24L, "V55" ) );
        listTuples.add( new Tuple<Long, String>( 12L, "V56" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V57" ) );
        listTuples.add( new Tuple<Long, String>( -2L, "V58" ) );
        listTuples.add( new Tuple<Long, String>( -3L, "V59" ) );
        listTuples.add( new Tuple<Long, String>( 14L, "V60" ) );
        listTuples.add( new Tuple<Long, String>( -6L, "V61" ) );
        listTuples.add( new Tuple<Long, String>( -9L, "V62" ) );
        listTuples.add( new Tuple<Long, String>( 16L, "V63" ) );
        listTuples.add( new Tuple<Long, String>( -15L, "V64" ) );
        listTuples.add( new Tuple<Long, String>( -25L, "V65" ) );
        listTuples.add( new Tuple<Long, String>( 17L, "V66" ) );
        listTuples.add( new Tuple<Long, String>( -12L, "V67" ) );
        listTuples.add( new Tuple<Long, String>( -13L, "V68" ) );
        listTuples.add( new Tuple<Long, String>( -21L, "V69" ) );
        listTuples.add( new Tuple<Long, String>( -27L, "V70" ) );
        listTuples.add( new Tuple<Long, String>( -8L, "V71" ) );
        listTuples.add( new Tuple<Long, String>( -14L, "V72" ) );
        listTuples.add( new Tuple<Long, String>( -24L, "V73" ) );
        listTuples.add( new Tuple<Long, String>( 12L, "V74" ) );
        listTuples.add( new Tuple<Long, String>( 1L, "V75" ) );
        listTuples.add( new Tuple<Long, String>( -6L, "V76" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V77" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V78" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V79" ) );
        listTuples.add( new Tuple<Long, String>( 12L, "V80" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V81" ) );
        listTuples.add( new Tuple<Long, String>( 10L, "V82" ) );
        listTuples.add( new Tuple<Long, String>( 28L, "V83" ) );
        listTuples.add( new Tuple<Long, String>( 23L, "V84" ) );
        listTuples.add( new Tuple<Long, String>( -20L, "V85" ) );
        listTuples.add( new Tuple<Long, String>( 22L, "V86" ) );
        listTuples.add( new Tuple<Long, String>( -2L, "V87" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V88" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V89" ) );
        listTuples.add( new Tuple<Long, String>( -7L, "V90" ) );
        listTuples.add( new Tuple<Long, String>( 20L, "V91" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V92" ) );
        listTuples.add( new Tuple<Long, String>( 12L, "V93" ) );
        listTuples.add( new Tuple<Long, String>( 24L, "V94" ) );
        listTuples.add( new Tuple<Long, String>( 5L, "V95" ) );
        listTuples.add( new Tuple<Long, String>( 1L, "V96" ) );
        listTuples.add( new Tuple<Long, String>( 11L, "V97" ) );
        listTuples.add( new Tuple<Long, String>( 3L, "V98" ) );
        listTuples.add( new Tuple<Long, String>( -4L, "V99" ) );
        listTuples.add( new Tuple<Long, String>( 6L, "V100" ) );
        listTuples.add( new Tuple<Long, String>( 27L, "V101" ) );
        listTuples.add( new Tuple<Long, String>( -23L, "V102" ) );
        listTuples.add( new Tuple<Long, String>( 18L, "V103" ) );
        listTuples.add( new Tuple<Long, String>( 30L, "V104" ) );
        listTuples.add( new Tuple<Long, String>( -29L, "V105" ) );
        listTuples.add( new Tuple<Long, String>( 13L, "V106" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V107" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V108" ) );
        listTuples.add( new Tuple<Long, String>( 1L, "V109" ) );
        listTuples.add( new Tuple<Long, String>( 10L, "V110" ) );
        listTuples.add( new Tuple<Long, String>( -11L, "V111" ) );
        listTuples.add( new Tuple<Long, String>( 29L, "V112" ) );
        listTuples.add( new Tuple<Long, String>( -21L, "V113" ) );
        listTuples.add( new Tuple<Long, String>( -30L, "V114" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V115" ) );
        listTuples.add( new Tuple<Long, String>( 9L, "V116" ) );
        listTuples.add( new Tuple<Long, String>( 5L, "V117" ) );
        listTuples.add( new Tuple<Long, String>( 12L, "V118" ) );
        listTuples.add( new Tuple<Long, String>( -32L, "V119" ) );
        listTuples.add( new Tuple<Long, String>( -1L, "V120" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V121" ) );
        listTuples.add( new Tuple<Long, String>( -22L, "V122" ) );
        listTuples.add( new Tuple<Long, String>( -32L, "V123" ) );
        listTuples.add( new Tuple<Long, String>( -23L, "V124" ) );
        listTuples.add( new Tuple<Long, String>( -25L, "V125" ) );
        listTuples.add( new Tuple<Long, String>( -24L, "V126" ) );
        listTuples.add( new Tuple<Long, String>( 9L, "V127" ) );
        listTuples.add( new Tuple<Long, String>( -27L, "V128" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V129" ) );
        listTuples.add( new Tuple<Long, String>( 12L, "V130" ) );
        listTuples.add( new Tuple<Long, String>( -17L, "V131" ) );
        listTuples.add( new Tuple<Long, String>( -6L, "V132" ) );
        listTuples.add( new Tuple<Long, String>( 14L, "V133" ) );
        listTuples.add( new Tuple<Long, String>( -16L, "V134" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V135" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V136" ) );
        listTuples.add( new Tuple<Long, String>( 20L, "V137" ) );
        listTuples.add( new Tuple<Long, String>( -2L, "V138" ) );
        listTuples.add( new Tuple<Long, String>( 14L, "V139" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V140" ) );
        listTuples.add( new Tuple<Long, String>( 13L, "V141" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V142" ) );
        listTuples.add( new Tuple<Long, String>( -29L, "V143" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V144" ) );
        listTuples.add( new Tuple<Long, String>( 6L, "V145" ) );
        listTuples.add( new Tuple<Long, String>( -22L, "V146" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V147" ) );
        listTuples.add( new Tuple<Long, String>( -4L, "V148" ) );
        listTuples.add( new Tuple<Long, String>( 27L, "V149" ) );
        listTuples.add( new Tuple<Long, String>( 31L, "V150" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V151" ) );
        listTuples.add( new Tuple<Long, String>( 30L, "V152" ) );
        listTuples.add( new Tuple<Long, String>( -31L, "V153" ) );
        listTuples.add( new Tuple<Long, String>( -6L, "V154" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V155" ) );
        listTuples.add( new Tuple<Long, String>( -22L, "V156" ) );
        listTuples.add( new Tuple<Long, String>( 15L, "V157" ) );
        listTuples.add( new Tuple<Long, String>( 25L, "V158" ) );
        listTuples.add( new Tuple<Long, String>( -26L, "V159" ) );
        listTuples.add( new Tuple<Long, String>( 22L, "V160" ) );
        listTuples.add( new Tuple<Long, String>( 32L, "V161" ) );
        listTuples.add( new Tuple<Long, String>( 16L, "V162" ) );
        listTuples.add( new Tuple<Long, String>( -27L, "V163" ) );
        listTuples.add( new Tuple<Long, String>( 11L, "V164" ) );
        listTuples.add( new Tuple<Long, String>( -9L, "V165" ) );
        listTuples.add( new Tuple<Long, String>( -11L, "V166" ) );
        listTuples.add( new Tuple<Long, String>( -14L, "V167" ) );
        listTuples.add( new Tuple<Long, String>( 19L, "V168" ) );
        listTuples.add( new Tuple<Long, String>( -21L, "V169" ) );
        listTuples.add( new Tuple<Long, String>( -21L, "V170" ) );
        listTuples.add( new Tuple<Long, String>( 10L, "V171" ) );
        listTuples.add( new Tuple<Long, String>( 17L, "V172" ) );
        listTuples.add( new Tuple<Long, String>( 30L, "V173" ) );
        listTuples.add( new Tuple<Long, String>( -12L, "V174" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V175" ) );
        listTuples.add( new Tuple<Long, String>( 14L, "V176" ) );
        listTuples.add( new Tuple<Long, String>( 9L, "V177" ) );
        listTuples.add( new Tuple<Long, String>( -14L, "V178" ) );
        listTuples.add( new Tuple<Long, String>( 5L, "V179" ) );
        listTuples.add( new Tuple<Long, String>( 8L, "V180" ) );
        listTuples.add( new Tuple<Long, String>( -32L, "V181" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V182" ) );
        listTuples.add( new Tuple<Long, String>( -17L, "V183" ) );
        listTuples.add( new Tuple<Long, String>( -26L, "V184" ) );
        listTuples.add( new Tuple<Long, String>( -26L, "V185" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V186" ) );
        listTuples.add( new Tuple<Long, String>( -12L, "V187" ) );
        listTuples.add( new Tuple<Long, String>( 7L, "V188" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V189" ) );
        listTuples.add( new Tuple<Long, String>( 16L, "V190" ) );
        listTuples.add( new Tuple<Long, String>( -26L, "V191" ) );
        listTuples.add( new Tuple<Long, String>( -26L, "V192" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V193" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V194" ) );
        listTuples.add( new Tuple<Long, String>( -24L, "V195" ) );
        listTuples.add( new Tuple<Long, String>( 32L, "V196" ) );
        listTuples.add( new Tuple<Long, String>( 9L, "V197" ) );
        listTuples.add( new Tuple<Long, String>( 13L, "V198" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V199" ) );
        listTuples.add( new Tuple<Long, String>( 32L, "V200" ) );
        listTuples.add( new Tuple<Long, String>( -29L, "V201" ) );
        listTuples.add( new Tuple<Long, String>( -16L, "V202" ) );
        listTuples.add( new Tuple<Long, String>( 9L, "V203" ) );
        listTuples.add( new Tuple<Long, String>( 25L, "V204" ) );
        listTuples.add( new Tuple<Long, String>( 18L, "V205" ) );
        listTuples.add( new Tuple<Long, String>( 4L, "V206" ) );
        listTuples.add( new Tuple<Long, String>( -4L, "V207" ) );
        listTuples.add( new Tuple<Long, String>( 4L, "V208" ) );
        listTuples.add( new Tuple<Long, String>( 23L, "V209" ) );
        listTuples.add( new Tuple<Long, String>( 31L, "V210" ) );
        listTuples.add( new Tuple<Long, String>( 17L, "V211" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V212" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V213" ) );
        listTuples.add( new Tuple<Long, String>( 18L, "V214" ) );
        listTuples.add( new Tuple<Long, String>( 8L, "V215" ) );
        listTuples.add( new Tuple<Long, String>( -5L, "V216" ) );
        listTuples.add( new Tuple<Long, String>( 13L, "V217" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V218" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V219" ) );
        listTuples.add( new Tuple<Long, String>( 22L, "V220" ) );
        listTuples.add( new Tuple<Long, String>( -2L, "V221" ) );
        listTuples.add( new Tuple<Long, String>( -3L, "V222" ) );
        listTuples.add( new Tuple<Long, String>( -9L, "V223" ) );
        listTuples.add( new Tuple<Long, String>( -4L, "V224" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V225" ) );
        listTuples.add( new Tuple<Long, String>( 18L, "V226" ) );
        listTuples.add( new Tuple<Long, String>( -8L, "V227" ) );
        listTuples.add( new Tuple<Long, String>( 1L, "V228" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V229" ) );
        listTuples.add( new Tuple<Long, String>( 25L, "V230" ) );
        listTuples.add( new Tuple<Long, String>( 22L, "V231" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V232" ) );
        listTuples.add( new Tuple<Long, String>( -27L, "V233" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V234" ) );
        listTuples.add( new Tuple<Long, String>( -27L, "V235" ) );
        listTuples.add( new Tuple<Long, String>( 17L, "V236" ) );
        listTuples.add( new Tuple<Long, String>( -15L, "V237" ) );
        listTuples.add( new Tuple<Long, String>( 3L, "V238" ) );
        listTuples.add( new Tuple<Long, String>( -1L, "V239" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V240" ) );
        listTuples.add( new Tuple<Long, String>( -17L, "V241" ) );
        listTuples.add( new Tuple<Long, String>( -18L, "V242" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V243" ) );
        listTuples.add( new Tuple<Long, String>( 7L, "V244" ) );
        listTuples.add( new Tuple<Long, String>( 18L, "V245" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V246" ) );
        listTuples.add( new Tuple<Long, String>( -31L, "V247" ) );
        listTuples.add( new Tuple<Long, String>( 18L, "V248" ) );
        listTuples.add( new Tuple<Long, String>( -28L, "V249" ) );
        listTuples.add( new Tuple<Long, String>( 7L, "V250" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V251" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V252" ) );
        listTuples.add( new Tuple<Long, String>( -15L, "V253" ) );
        listTuples.add( new Tuple<Long, String>( -4L, "V254" ) );
        listTuples.add( new Tuple<Long, String>( 11L, "V255" ) );
        listTuples.add( new Tuple<Long, String>( 30L, "V256" ) );
        listTuples.add( new Tuple<Long, String>( -27L, "V257" ) );
        listTuples.add( new Tuple<Long, String>( 30L, "V258" ) );
        listTuples.add( new Tuple<Long, String>( -6L, "V259" ) );
        listTuples.add( new Tuple<Long, String>( -4L, "V260" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V261" ) );
        listTuples.add( new Tuple<Long, String>( 7L, "V262" ) );
        listTuples.add( new Tuple<Long, String>( -6L, "V263" ) );
        listTuples.add( new Tuple<Long, String>( -4L, "V264" ) );
        listTuples.add( new Tuple<Long, String>( 29L, "V265" ) );
        listTuples.add( new Tuple<Long, String>( 26L, "V266" ) );
        listTuples.add( new Tuple<Long, String>( -7L, "V267" ) );
        listTuples.add( new Tuple<Long, String>( -24L, "V268" ) );
        listTuples.add( new Tuple<Long, String>( 4L, "V269" ) );
        listTuples.add( new Tuple<Long, String>( -9L, "V270" ) );
        listTuples.add( new Tuple<Long, String>( -18L, "V271" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V272" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V273" ) );
        listTuples.add( new Tuple<Long, String>( 24L, "V274" ) );
        listTuples.add( new Tuple<Long, String>( -13L, "V275" ) );
        listTuples.add( new Tuple<Long, String>( 31L, "V276" ) );
        listTuples.add( new Tuple<Long, String>( -21L, "V277" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V278" ) );
        listTuples.add( new Tuple<Long, String>( -5L, "V279" ) );
        listTuples.add( new Tuple<Long, String>( -6L, "V280" ) );
        listTuples.add( new Tuple<Long, String>( -17L, "V281" ) );
        listTuples.add( new Tuple<Long, String>( -1L, "V282" ) );
        listTuples.add( new Tuple<Long, String>( -1L, "V283" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V284" ) );
        listTuples.add( new Tuple<Long, String>( -29L, "V285" ) );
        listTuples.add( new Tuple<Long, String>( 1L, "V286" ) );
        listTuples.add( new Tuple<Long, String>( -15L, "V287" ) );
        listTuples.add( new Tuple<Long, String>( 14L, "V288" ) );
        listTuples.add( new Tuple<Long, String>( -15L, "V289" ) );
        listTuples.add( new Tuple<Long, String>( -6L, "V290" ) );
        listTuples.add( new Tuple<Long, String>( -26L, "V291" ) );
        listTuples.add( new Tuple<Long, String>( 24L, "V292" ) );
        listTuples.add( new Tuple<Long, String>( -22L, "V293" ) );
        listTuples.add( new Tuple<Long, String>( 2L, "V294" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V295" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V296" ) );
        listTuples.add( new Tuple<Long, String>( 11L, "V297" ) );
        listTuples.add( new Tuple<Long, String>( 28L, "V298" ) );
        listTuples.add( new Tuple<Long, String>( 15L, "V299" ) );
        listTuples.add( new Tuple<Long, String>( 17L, "V300" ) );
        listTuples.add( new Tuple<Long, String>( -25L, "V301" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V302" ) );
        listTuples.add( new Tuple<Long, String>( -20L, "V303" ) );
        listTuples.add( new Tuple<Long, String>( -12L, "V304" ) );
        listTuples.add( new Tuple<Long, String>( -10L, "V305" ) );
        listTuples.add( new Tuple<Long, String>( -9L, "V306" ) );
        listTuples.add( new Tuple<Long, String>( 16L, "V307" ) );
        listTuples.add( new Tuple<Long, String>( -25L, "V308" ) );
        listTuples.add( new Tuple<Long, String>( 6L, "V309" ) );
        listTuples.add( new Tuple<Long, String>( 20L, "V310" ) );
        listTuples.add( new Tuple<Long, String>( -31L, "V311" ) );
        listTuples.add( new Tuple<Long, String>( -17L, "V312" ) );
        listTuples.add( new Tuple<Long, String>( -19L, "V313" ) );
        listTuples.add( new Tuple<Long, String>( 0L, "V314" ) );
        listTuples.add( new Tuple<Long, String>( -32L, "V315" ) );
        listTuples.add( new Tuple<Long, String>( 21L, "V316" ) );
        listTuples.add( new Tuple<Long, String>( 19L, "V317" ) );
        listTuples.add( new Tuple<Long, String>( -31L, "V318" ) );

        File file = File.createTempFile( "managedbtreebuilder", ".data" );
        file.deleteOnExit();

        try
        {
            RecordManager rm = new RecordManager( file.getAbsolutePath() );
            PersistedBTree<Long, String> btree = ( PersistedBTree<Long, String> ) rm.addBTree( "test",
                LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );

            // btree.valueThresholdUp = 8;

            Iterator<Tuple<Long, String>> tupleIterator = new Iterator<Tuple<Long, String>>()
            {
                private int pos = 0;


                @Override
                public Tuple<Long, String> next()
                {
                    Tuple<Long, String> tuple = listTuples.get( pos++ );

                    return tuple;
                }


                @Override
                public boolean hasNext()
                {
                    return pos < listTuples.size();
                }


                @Override
                public void remove()
                {
                }
            };

            long t0 = System.currentTimeMillis();
            BTree<Long, String> result = null;

            result = BulkLoader.load( btree, tupleIterator, 128 );

            TupleCursor<Long, String> cursor = result.browse();
            int nbFetched = 0;
            Tuple<Long, String> prev = null;
            Tuple<Long, String> elem = null;

            long t2 = System.currentTimeMillis();

            try
            {
                while ( cursor.hasNext() )
                {
                    prev = elem;
                    elem = cursor.next();
                    nbFetched++;
                }
            }
            catch ( Exception e )
            {
                System.out.println( "--->" + prev );
                e.printStackTrace();
            }

            long t3 = System.currentTimeMillis();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }
}
