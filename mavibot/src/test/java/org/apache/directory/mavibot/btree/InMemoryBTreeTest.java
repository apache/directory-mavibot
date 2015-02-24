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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;


/**
 * A unit test class for in-memory BTree
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryBTreeTest
{
    // Some values to inject in a btree
    private static int[] sortedValues = new int[]
        {
            0, 1, 2, 4, 5, 6, 8, 9, 11, 12,
            13, 14, 16, 19, 21, 22, 23, 25, 26, 28,
            30, 31, 32, 34, 36, 37, 38, 39, 41, 42,
            44, 45, 47, 50, 52, 53, 54, 55, 56, 58,
            59, 60, 63, 64, 67, 68, 70, 72, 73, 74,
            76, 77, 79, 80, 81, 82, 85, 88, 89, 90,
            92, 93, 95, 97, 98, 100, 101, 102, 103, 104,
            105, 106, 107, 109, 110, 111, 112, 117, 118, 120,
            121, 128, 129, 130, 131, 132, 135, 136, 137, 138,
            139, 140, 141, 142, 143, 146, 147, 148, 149, 150,
            152, 154, 156, 160, 161, 162, 163, 165, 167, 168,
            169, 171, 173, 174, 175, 176, 177, 178, 179, 180,
            181, 182, 183, 189, 190, 193, 194, 195, 199, 200,
            202, 203, 205, 206, 207, 208, 209, 210, 212, 215,
            216, 217, 219, 220, 222, 223, 224, 225, 226, 227,
            228, 230, 231, 235, 236, 238, 239, 241, 242, 243,
            245, 246, 247, 249, 250, 251, 252, 254, 256, 257,
            258, 259, 261, 262, 263, 264, 266, 268, 272, 273,
            274, 276, 277, 278, 279, 282, 283, 286, 289, 290,
            292, 293, 294, 296, 298, 299, 300, 301, 303, 305,
            308, 310, 316, 317, 318, 319, 322, 323, 324, 326,
            327, 329, 331, 333, 334, 335, 336, 337, 338, 339,
            340, 341, 346, 347, 348, 349, 350, 351, 352, 353,
            355, 356, 357, 358, 359, 361, 365, 366, 373, 374,
            375, 379, 380, 381, 382, 384, 385, 387, 388, 389,
            390, 392, 393, 395, 396, 397, 398, 399, 400, 401,
            404, 405, 406, 407, 410, 411, 412, 416, 417, 418,
            420, 421, 422, 424, 426, 427, 428, 430, 431, 432,
            433, 436, 439, 441, 443, 444, 445, 446, 447, 448,
            449, 450, 451, 452, 453, 454, 455, 456, 458, 459,
            464, 466, 469, 470, 471, 472, 475, 477, 478, 482,
            483, 484, 485, 486, 488, 490, 491, 492, 493, 495,
            496, 497, 500, 502, 503, 504, 505, 506, 507, 509,
            510, 514, 516, 518, 520, 521, 523, 524, 526, 527,
            528, 529, 530, 532, 533, 535, 538, 539, 540, 542,
            543, 544, 546, 547, 549, 550, 551, 553, 554, 558,
            559, 561, 563, 564, 566, 567, 568, 569, 570, 571,
            572, 576, 577, 578, 580, 582, 583, 586, 588, 589,
            590, 592, 593, 596, 597, 598, 599, 600, 601, 604,
            605, 606, 607, 609, 610, 613, 615, 617, 618, 619,
            620, 621, 626, 627, 628, 631, 632, 633, 635, 636,
            637, 638, 639, 640, 641, 643, 645, 647, 648, 649,
            650, 651, 652, 653, 655, 656, 658, 659, 660, 662,
            666, 669, 673, 674, 675, 676, 677, 678, 680, 681,
            682, 683, 685, 686, 687, 688, 689, 690, 691, 692,
            693, 694, 696, 698, 699, 700, 701, 705, 708, 709,
            711, 713, 714, 715, 719, 720, 723, 725, 726, 727,
            728, 731, 732, 733, 734, 735, 736, 739, 740, 743,
            744, 745, 746, 747, 749, 750, 752, 753, 762, 763,
            765, 766, 768, 770, 772, 773, 774, 776, 777, 779,
            782, 784, 785, 788, 790, 791, 793, 794, 795, 798,
            799, 800, 801, 803, 804, 805, 808, 810, 812, 813,
            814, 816, 818, 821, 822, 823, 824, 827, 828, 829,
            831, 832, 833, 834, 835, 837, 838, 839, 840, 843,
            846, 847, 849, 852, 853, 854, 856, 857, 859, 860,
            863, 864, 865, 866, 867, 868, 869, 872, 873, 877,
            880, 881, 882, 883, 887, 888, 889, 890, 891, 894,
            895, 897, 898, 899, 902, 904, 905, 907, 908, 910,
            911, 912, 915, 916, 917, 918, 919, 923, 925, 926,
            927, 928, 929, 930, 932, 935, 936, 937, 938, 939,
            944, 945, 947, 952, 953, 954, 955, 956, 957, 958,
            960, 967, 970, 971, 972, 974, 975, 976, 978, 979,
            980, 981, 983, 984, 985, 987, 988, 989, 991, 995
    };


    /**
     * Checks the created BTree contains the expected values
     */
    private boolean checkTreeLong( Set<Long> expected, BTree<Long, String> btree ) throws IOException
    {
        // We loop on all the expected value to see if they have correctly been inserted
        // into the btree
        for ( Long key : expected )
        {
            try
            {
                btree.get( key );
            }
            catch ( KeyNotFoundException knfe )
            {
                return false;
            }
        }

        return true;
    }


    /**
     * Test the insertion of elements in a BTree. We will try 1000 times to insert 1000
     * random elements in [0..1024], and check every tree to see if all the added elements
     * are present. This pretty much validate the the insertion, assuming that due to the
     * randomization of the injected values, we will statically meet all the use cases.
     * @throws Exception
     */
    @Test
    public void testPageInsert() throws Exception
    {
        Set<Long> expected = new HashSet<Long>();
        List<Long> added = new ArrayList<Long>();

        Random random = new Random( System.nanoTime() );

        int nbError = 0;

        long l1 = System.currentTimeMillis();
        int n = 0;
        long delta = l1;
        int nbTrees = 1000;
        int nbElems = 1000;

        for ( int j = 0; j < nbTrees; j++ )
        {
            BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
                StringSerializer.INSTANCE );
            btree.setPageSize( 32 );

            for ( int i = 0; i < nbElems; i++ )
            {
                Long key = ( long ) random.nextInt( 1024 );
                String value = "V" + key;
                expected.add( key );
                added.add( key );

                //System.out.println( "Adding " + i + "th : " + key );

                try
                {
                    btree.insert( key, value );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    System.out.println( btree );
                    System.out.println( "Error while adding " + value );
                    nbError++;
                    btree.close();

                    return;
                }
            }

            assertTrue( checkTreeLong( expected, btree ) );

            /* For debug only
            if ( !checkTree( expected, btree ) )
            {
                boolean isFirst = true;

                for ( Long key : added )
                {
                    if ( isFirst )
                    {
                        isFirst = false;
                    }
                    else
                    {
                        System.out.print( ", " );
                    }

                    System.out.print( key );
                }
            }
            */

            if ( j % 10000 == 0 )
            {
                if ( n > 0 )
                {
                    long t0 = System.currentTimeMillis();
                    System.out.println( "Delta" + n + ": " + ( t0 - delta ) );
                    delta = t0;
                }

                n++;
            }

            expected.clear();
            added.clear();

            btree.close();
        }

        long l2 = System.currentTimeMillis();

        System.out.println( "Delta : " + ( l2 - l1 ) + ", nbError = " + nbError
            + ", Nb insertion per second : " + ( nbTrees * nbElems * 1000 ) / ( l2 - l1 ) );
    }


    /**
     * Test the deletion of elements in a BTree. We will try 1000 times to delete 1000
     * random elements in [0..1024], and check every tree to see if all the removed elements
     * are absent. This pretty much validate the the deletion operation is valid, assuming
     * that due to the randomization of the deleted values, we will statically meet all the
     * use cases.
     * @throws Exception
     */
    @Test
    public void testPageDeleteRandom() throws IOException
    {
        Set<Long> expected = new HashSet<Long>();
        List<Long> added = new ArrayList<Long>();

        Random random = new Random( System.nanoTime() );

        int nbError = 0;

        long l1 = System.currentTimeMillis();
        int n = 0;
        long delta = l1;
        int nbTrees = 1000;
        int nbElems = 1000;

        for ( int j = 0; j < nbTrees; j++ )
        {
            BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
                StringSerializer.INSTANCE );
            btree.setPageSize( 8 );

            for ( int i = 0; i < nbElems; i++ )
            {
                Long key = ( long ) random.nextInt( 1024 );
                String value = "V" + key;
                expected.add( key );
                added.add( key );

                //System.out.println( "Adding " + i + "th : " + key );

                try
                {
                    btree.insert( key, value );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    System.out.println( btree );
                    System.out.println( "Error while adding " + value );
                    nbError++;
                    btree.close();

                    return;
                }
            }

            assertTrue( checkTreeLong( expected, btree ) );

            // Now, delete the elements
            /*
            boolean isFirst = true;

            for ( long element : added )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    System.out.print( ", " );
                }

                System.out.print( element );
            }

            //System.out.println( "\n--------------------" );
             */

            //int i = 0;

            for ( long element : expected )
            {
                //System.out.println( "Deleting #" + i + " : " + element );
                //i++;
                //System.out.println( btree );
                Tuple<Long, String> tuple = btree.delete( element );

                if ( tuple == null )
                {
                    System.out.println( btree );
                }

                assertEquals( Long.valueOf( element ), tuple.getKey() );

                checkNull( btree, element );

                //System.out.println( "" );
            }

            if ( j % 10000 == 0 )
            {
                if ( n > 0 )
                {
                    long t0 = System.currentTimeMillis();
                    System.out.println( "Delta" + n + ": " + ( t0 - delta ) );
                    delta = t0;
                }

                n++;

            }

            expected.clear();
            added.clear();

            btree.close();
        }

        long l2 = System.currentTimeMillis();

        System.out.println( "Delta : " + ( l2 - l1 ) + ", nbError = " + nbError
            + ", Nb deletion per second : " + ( nbTrees * nbElems * 1000 ) / ( l2 - l1 ) );
    }


    @Test
    public void testDeleteDebug() throws IOException
    {
        long[] values = new long[]
            {
                148, 746, 525, 327, 1, 705, 171, 1023, 769, 1021,
                128, 772, 744, 771, 925, 884, 346, 519, 989, 350,
                649, 895, 464, 164, 190, 298, 203, 69, 483, 38,
                266, 83, 88, 285, 879, 342, 231, 432, 722, 432,
                258, 307, 237, 151, 43, 36, 135, 166, 325, 886,
                878, 307, 925, 835, 800, 895, 519, 947, 703, 27,
                324, 668, 40, 943, 804, 230, 223, 584, 828, 575,
                69, 955, 344, 325, 896, 423, 855, 783, 225, 447,
                28, 23, 262, 679, 782, 517, 412, 878, 641, 940,
                368, 245, 1005, 226, 939, 320, 396, 437, 373, 61
        };

        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 8 );

        for ( long value : values )
        {
            String strValue = "V" + value;

            try
            {
                btree.insert( value, strValue );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                System.out.println( btree );
                System.out.println( "Error while adding " + value );
                btree.close();

                return;
            }
        }

        long[] deletes = new long[]
            {
                1,
                828,
                285,
                804,
                258,
                262,
        };

        for ( long value : deletes )
        {
            Tuple<Long, String> tuple = btree.delete( value );

            if ( tuple != null )
            {
                assertEquals( Long.valueOf( value ), tuple.getKey() );
            }

            checkNull( btree, value );
        }

        btree.close();
    }


    /**
     * Test the deletion of elements from a BTree.
     */
    @Test
    public void testPageDelete() throws Exception
    {
        Set<Long> expected = new HashSet<Long>();
        List<Long> added = new ArrayList<Long>();

        Random random = new Random( System.nanoTime() );

        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 8 );

        // Insert some values
        for ( int i = 0; i < 8; i++ )
        {
            Long key = ( long ) random.nextInt( 1024 );
            String value = "V" + key;
            added.add( key );

            try
            {
                btree.insert( key, value );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                System.out.println( btree );
                System.out.println( "Error while adding " + value );
                btree.close();

                return;
            }
        }

        assertTrue( checkTreeLong( expected, btree ) );

        // Now, delete entries
        for ( long key : added )
        {
            //System.out.println( "Removing " + key + " from " + btree );
            try
            {
                btree.delete( key );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                System.out.println( btree );
                System.out.println( "Error while deleting " + key );
                btree.close();

                return;
            }

            assertTrue( checkTreeLong( expected, btree ) );
        }

        btree.close();
    }


    /**
     * This test is used to debug some corner cases.
     * We don't run it except to check a special condition
     */
    @Test
    @Ignore("This is a debug test")
    public void testPageInsertDebug() throws Exception
    {
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        Long[] elems = new Long[]
            {
                235L, 135L, 247L, 181L, 12L, 112L, 117L, 253L,
                37L, 158L, 56L, 118L, 184L, 101L, 173L, 126L,
                61L, 81L, 140L, 173L, 32L, 163L, 224L, 114L,
                133L, 18L, 14L, 82L, 107L, 219L, 244L, 255L,
                6L, 103L, 170L, 151L, 134L, 196L, 155L, 97L,
                80L, 122L, 89L, 253L, 33L, 101L, 56L, 168L,
                253L, 187L, 99L, 58L, 151L, 206L, 34L, 96L,
                20L, 188L, 143L, 150L, 76L, 111L, 234L, 66L,
                12L, 194L, 164L, 190L, 19L, 192L, 161L, 147L,
                92L, 89L, 237L, 187L, 250L, 13L, 233L, 34L,
                187L, 232L, 248L, 237L, 129L, 1L, 233L, 252L,
                18L, 98L, 56L, 121L, 162L, 233L, 29L, 48L,
                176L, 48L, 182L, 130L
        };

        int size = 0;
        for ( Long elem : elems )
        {
            size++;
            String value = "V" + elem;
            btree.insert( elem, value );

            System.out.println( "Adding " + elem + " :\n" + btree );

            for ( int i = 0; i < size; i++ )
            {
                try
                {
                    btree.get( elems[i] );
                }
                catch ( KeyNotFoundException knfe )
                {
                    System.out.println( "Bad tree, missing " + elems[i] + ", " + btree );
                }
            }

            if ( size == 27 )
            {
                System.out.println( btree );
            }
            //System.out.println( "added " + elem + ":\n" + btree );
        }

        //System.out.println( btree );

        btree.close();
    }


    /*
    @Test
    public void testPageRemove() throws Exception
    {
        Long[] keys = new Long[]{  101L, 113L, 20L, 72L, 215L, 239L, 108L, 21L };

        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( new LongComparator(), 8 );
        System.out.println( btree );

        for ( Long key : keys )
        {
            btree.insert( key, "V" + key );
        }

        System.out.println( btree );

        // Remove from the left
        btree.remove( 20L );
        System.out.println( btree );

        // Remove from the right
        btree.remove( 239L );
        System.out.println( btree );

        // Remove from the middle
        btree.remove( 72L );
        System.out.println( btree );

        // Remove all the remaining elements
        btree.remove( 101L );
        System.out.println( btree );
        btree.remove( 108L );
        System.out.println( btree );
        btree.remove( 215L );
        System.out.println( btree );
        btree.remove( 113L );
        System.out.println( btree );
        btree.remove( 21L );
        System.out.println( btree );

        btree.close();
    }
    */

    /**
     * Test the browse method going forward
     * @throws Exception
     */
    @Test
    public void testBrowseForward() throws Exception
    {
        // Create a BTree with pages containing 8 elements
        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 8 );

        // Inject the values
        for ( int value : sortedValues )
        {
            String strValue = "V" + value;

            btree.insert( value, strValue );
        }

        // Check that the tree contains all the values
        for ( int key : sortedValues )
        {
            String value = btree.get( key );

            assertNotNull( value );
        }

        // Browse starting at position 10
        int pos = 10;
        TupleCursor<Integer, String> cursor = btree.browseFrom( sortedValues[pos] );

        while ( cursor.hasNext() )
        {
            Tuple<Integer, String> tuple = cursor.next();

            assertNotNull( tuple );
            Integer val = sortedValues[pos];
            Integer res = tuple.getKey();
            assertEquals( val, res );
            pos++;
        }

        cursor.close();

        // Now, start on a non existing key (7)
        cursor = btree.browseFrom( 7 );

        // We should start reading values superior to 7, so value 8 at position 6 in the array
        pos = 6;

        while ( cursor.hasNext() )
        {
            Tuple<Integer, String> tuple = cursor.next();

            assertNotNull( tuple );
            Integer val = sortedValues[pos];
            Integer res = tuple.getKey();
            assertEquals( val, res );
            pos++;
        }

        cursor.close();

        // Last, let's browse with no key, we should get all the values
        cursor = btree.browse();

        pos = 0;

        while ( cursor.hasNext() )
        {
            Tuple<Integer, String> tuple = cursor.next();

            assertNotNull( tuple );
            Integer val = sortedValues[pos];
            Integer res = tuple.getKey();
            assertEquals( val, res );
            pos++;
        }

        cursor.close();
        btree.close();
    }


    /**
     * Test the browse method going backward
     * @throws Exception
     */
    @Test
    public void testBrowseBackward() throws Exception
    {
        // Create a BTree with pages containing 8 elements
        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 8 );

        // Inject the values
        for ( int value : sortedValues )
        {
            String strValue = "V" + value;

            btree.insert( value, strValue );
        }

        // Check that the tree contains all the values
        for ( int key : sortedValues )
        {
            String value = btree.get( key );

            assertNotNull( value );
        }

        // Browse starting at position 10
        int pos = 10;
        TupleCursor<Integer, String> cursor = btree.browseFrom( sortedValues[pos] );

        while ( cursor.hasPrev() )
        {
            Tuple<Integer, String> tuple = cursor.prev();

            pos--;

            assertNotNull( tuple );
            Integer val = sortedValues[pos];
            Integer res = tuple.getKey();
            assertEquals( val, res );
        }

        cursor.close();

        // Now, start on a non existing key (7)
        cursor = btree.browseFrom( 7 );

        // We should start reading values superior to 7, so value 8 at position 6 in the array
        pos = 6;

        while ( cursor.hasPrev() )
        {
            Tuple<Integer, String> tuple = cursor.prev();

            pos--;
            assertNotNull( tuple );
            Integer val = sortedValues[pos];
            Integer res = tuple.getKey();
            assertEquals( val, res );
        }

        cursor.close();

        // Last, let's browse with no key, we should get no values
        cursor = btree.browse();

        pos = 0;

        assertFalse( cursor.hasPrev() );

        cursor.close();
        btree.close();
    }


    /**
     * Test a browse over an empty tree
     */
    @Test
    public void testBrowseEmptyTree() throws Exception
    {
        // Create a BTree with pages containing 8 elements
        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 8 );

        TupleCursor<Integer, String> cursor = btree.browse();

        assertFalse( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );

        cursor.close();
        btree.close();
    }


    /**
     * Test a browse forward and backward
     */
    @Test
    public void testBrowseForwardBackward() throws Exception
    {
        // Create a BTree with pages containing 4 elements
        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        for ( int i = 0; i < 16; i++ )
        {
            String strValue = "V" + i;
            btree.insert( i, strValue );
        }

        // Start to browse in the middle
        TupleCursor<Integer, String> cursor = btree.browseFrom( 8 );

        assertTrue( cursor.hasNext() );

        // Get 8
        assertEquals( 8, cursor.next().getKey().intValue() );

        // get 9
        assertEquals( 9, cursor.next().getKey().intValue() );

        // get 10
        assertEquals( 10, cursor.next().getKey().intValue() );

        // get 11
        assertEquals( 11, cursor.next().getKey().intValue() );

        // get 12 (now, we must have gone through at least 2 pages)
        assertEquals( 12, cursor.next().getKey().intValue() );

        assertTrue( cursor.hasPrev() );

        // Lets go backward.
        assertEquals( 11, cursor.prev().getKey().intValue() );

        // Get 10
        assertEquals( 10, cursor.prev().getKey().intValue() );

        // Get 9
        assertEquals( 9, cursor.prev().getKey().intValue() );

        // Get 8
        assertEquals( 8, cursor.prev().getKey().intValue() );

        // Get 7
        assertEquals( 7, cursor.prev().getKey().intValue() );

        cursor.close();
        btree.close();
    }


    /**
     * Test various deletions in a tree, when we have full leaves
     */
    @Test
    public void testDeleteFromFullLeaves() throws Exception
    {
        // Create a BTree with pages containing 4 elements
        BTree<Integer, String> btree = createTwoLevelBTreeFullLeaves();

        // Test removals leadings to various RemoveResult.
        // The tree remains the same after the deletion
        // First, no borrow nor merge
        btree.delete( 1 );

        checkNull( btree, 1 );

        btree.insert( 1, "V1" );

        btree.delete( 3 );

        checkNull( btree, 3 );

        btree.insert( 3, "V3" );

        btree.delete( 4 );

        checkNull( btree, 4 );

        btree.insert( 4, "V4" );

        btree.delete( 11 );

        checkNull( btree, 11 );

        btree.insert( 11, "V11" );

        btree.delete( 20 );

        checkNull( btree, 20 );

        btree.insert( 20, "V20" );

        btree.delete( 0 );

        checkNull( btree, 0 );

        btree.delete( 5 );

        checkNull( btree, 5 );

        btree.delete( 9 );

        checkNull( btree, 9 );

        btree.close();
    }


    /**
     * Test the exist() method
     */
    @Test
    public void testExist() throws IOException, KeyNotFoundException
    {
        // Create a BTree with pages containing 4 elements
        BTree<Integer, String> btree = createTwoLevelBTreeFullLeaves();

        for ( int i = 1; i < 21; i++ )
        {
            assertTrue( btree.hasKey( 5 ) );
        }

        assertFalse( btree.hasKey( 0 ) );
        assertFalse( btree.hasKey( 21 ) );

        btree.close();
    }


    /**
     * Test various deletions in a tree, leadings to borrowFromSibling
     */
    @Test
    public void testDeleteBorrowFromSibling() throws Exception
    {
        // Create a BTree with pages containing 4 elements
        BTree<Integer, String> btree = createTwoLevelBTreeFullLeaves();

        // Delete some useless elements to simulate the tree we want to test
        // Make the left leaf to contain N/2 elements
        btree.delete( 3 );
        btree.delete( 4 );

        // Make the right leaf to contain N/2 elements
        btree.delete( 19 );
        btree.delete( 20 );

        // Make the middle leaf to contain N/2 elements
        btree.delete( 11 );
        btree.delete( 12 );

        // Delete the leftmost key
        btree.delete( 1 );

        checkNull( btree, 1 );

        // Delete the rightmost key
        btree.delete( 18 );

        checkNull( btree, 18 );

        // Delete one element in the left page, but not the first one
        btree.delete( 5 );

        checkNull( btree, 5 );

        // Delete the one element in the right page, but the first one
        btree.delete( 16 );

        checkNull( btree, 16 );

        btree.close();

        // Now do that with a deeper btree
        btree = createMultiLevelBTreeLeavesHalfFull();

        // Add some more elements on the second leaf before deleting some elements in the first leaf
        btree.insert( 8, "V8" );
        btree.insert( 9, "V9" );

        // and delete some
        btree.delete( 2 );

        checkNull( btree, 2 );

        btree.delete( 6 );

        checkNull( btree, 6 );

        // Add some more elements on the pre-last leaf before deleting some elements in the last leaf
        btree.insert( 96, "V96" );
        btree.insert( 97, "V97" );

        // and delete some
        btree.delete( 98 );

        checkNull( btree, 98 );

        btree.delete( 99 );

        checkNull( btree, 99 );

        // Now try to delete elements in the middle
        btree.insert( 48, "V48" );

        btree.delete( 42 );

        checkNull( btree, 42 );

        btree.insert( 72, "V72" );

        btree.delete( 67 );

        checkNull( btree, 67 );

        btree.close();
    }


    /**
     * Test the browse method with a non existing key
     * @throws Exception
     */
    @Test
    public void testBrowseNonExistingKey() throws Exception
    {
        // Create a BTree with pages containing 8 elements
        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 8 );
        for ( int i = 0; i < 11; i++ )
        {
            btree.insert( i, String.valueOf( i ) );
        }

        for ( int i = 0; i < 11; i++ )
        {
            assertNotNull( btree.get( i ) );
        }

        assertTrue( btree.hasKey( 8 ) );
        assertFalse( btree.hasKey( 11 ) );

        TupleCursor<Integer, String> cursor = btree.browseFrom( 11 );
        assertFalse( cursor.hasNext() );

        btree.close();
    }


    private Page<Integer, String> createLeaf( BTree<Integer, String> btree, long revision,
        Tuple<Integer, String>... tuples )
    {
        InMemoryLeaf<Integer, String> leaf = new InMemoryLeaf<Integer, String>( btree );
        int pos = 0;
        leaf.setRevision( revision );
        leaf.setNbElems( tuples.length );
        leaf.setKeys( new KeyHolder[leaf.getNbElems()] );
        leaf.values = ( InMemoryValueHolder<String>[] ) Array
            .newInstance( InMemoryValueHolder.class, leaf.getNbElems() );

        for ( Tuple<Integer, String> tuple : tuples )
        {
            leaf.setKey( pos, new KeyHolder<Integer>( tuple.getKey() ) );
            leaf.values[pos] = new InMemoryValueHolder<String>( btree, tuple.getValue() );
            pos++;
        }

        return leaf;
    }


    private void addPage( BTree<Integer, String> btree, InMemoryNode<Integer, String> node, Page<Integer, String> page,
        int pos )
        throws EndOfFileExceededException, IOException
    {
        Tuple<Integer, String> leftmost = page.findLeftMost();

        if ( pos > 0 )
        {
            node.setKey( pos - 1, new KeyHolder<Integer>( leftmost.getKey() ) );
        }

        node.setPageHolder( pos, new PageHolder<Integer, String>( btree, page ) );
    }


    /**
     * Creates a 2 level depth tree of full pages
     */
    private BTree<Integer, String> createTwoLevelBTreeFullLeaves() throws IOException
    {
        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        // Create a tree with 5 children containing 4 elements each. The tree is full.
        int[] keys = new int[]
            { 1, 2, 5, 6, 3, 4, 9, 10, 7, 8, 13, 14, 11, 12, 17, 18, 15, 16, 19, 20 };

        for ( int key : keys )
        {
            String value = "V" + key;
            btree.insert( key, value );
        }

        return btree;
    }


    /**
     * Creates a 2 level depth tree of half full pages
     */
    private BTree<Integer, String> createTwoLevelBTreeHalfFullLeaves() throws IOException
    {
        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        // Create a tree with 5 children containing 4 elements each. The tree is full.
        int[] keys = new int[]
            { 1, 2, 17, 18, 13, 14, 9, 10, 5, 6, 3 };

        for ( int key : keys )
        {
            String value = "V" + key;
            btree.insert( key, value );
        }

        // Regulate the tree by removing the last value added, so that all the leaves have only 2 elements
        btree.delete( 3 );

        return btree;
    }

    /** A set used to check that the tree contains the contained elements */
    private Set<Integer> EXPECTED1 = new HashSet<Integer>();


    /**
     * Creates a 3 level depth tree, with each page containing only N/2 elements
     */
    private BTree<Integer, String> createMultiLevelBTreeLeavesHalfFull() throws IOException
    {
        // Create a BTree with pages containing 4 elements
        int pageSize = 4;

        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE,
            pageSize );

        InMemoryNode<Integer, String> root = new InMemoryNode<Integer, String>( btree, 1L, pageSize );

        // Create the tree with 3 levels, all the leaves containing only N/2 elements
        int counter = 1;
        for ( int i = 0; i < pageSize + 1; i++ )
        {
            InMemoryNode<Integer, String> node = new InMemoryNode<Integer, String>( btree, 1L, pageSize );

            for ( int j = 0; j < pageSize + 1; j++ )
            {
                int even = counter * 2;

                @SuppressWarnings("unchecked")
                Page<Integer, String> leaf = createLeaf(
                    btree,
                    1L,
                    new Tuple<Integer, String>( even, "v" + even ),
                    new Tuple<Integer, String>( even + 1, "v" + ( even + 1 ) )
                    );

                counter += 2;

                addPage( btree, node, leaf, j );

                EXPECTED1.add( even );
                EXPECTED1.add( even + 1 );
            }

            addPage( btree, root, node, i );
        }

        ( ( AbstractBTree<Integer, String> ) btree ).setRootPage( root );

        return btree;
    }


    /**
     * Remove an element from the tree, checking that the removal was successful
     * @param btree The btree on which we remove an element
     * @param element The removed element
     * @param expected The expected set of elements
     */
    private void checkRemoval( BTree<Integer, String> btree, int element, Set<Integer> expected ) throws IOException,
        KeyNotFoundException
    {
        Tuple<Integer, String> removed = btree.delete( element );
        assertEquals( element, removed.getKey().intValue() );
        assertEquals( "v" + element, removed.getValue() );

        checkNull( btree, element );

        expected.remove( element );
        checkTree( btree, expected );
    }


    /**
     * Check that the tree contains all the elements in the expected set, and that
     * all the elements in the tree are also present in the set
     *
     * @param btree The tree to check
     * @param expected The set with the expected elements
     */
    private void checkTree( BTree<Integer, String> btree, Set<Integer> expected ) throws KeyNotFoundException
    {
        try
        {
            TupleCursor<Integer, String> cursor = btree.browse();
            Integer value = null;

            while ( cursor.hasNext() )
            {
                Tuple<Integer, String> tuple = cursor.next();

                if ( value == null )
                {
                    value = tuple.getKey();
                }
                else
                {
                    assertTrue( value < tuple.getKey() );
                    value = tuple.getKey();
                }

                assertTrue( expected.contains( value ) );
                expected.remove( value );
            }

            assertEquals( 0, expected.size() );
        }
        catch ( IOException ioe )
        {
            fail();
        }
    }


    /**
     * Remove a set of values from a btree
     *
     * @param btree The modified btree
     * @param expected The set of expected values to update
     * @param values The values to remove
     */
    private void delete( BTree<Integer, String> btree, Set<Integer> expected, int... values ) throws IOException
    {
        for ( int value : values )
        {
            btree.delete( value );
            expected.remove( value );
        }
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will generate a merge in the leaves.
     */
    @Test
    public void testDeleteMultiLevelsLeadingToLeafMerge() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // Case 1 : delete the leftmost element in the btree in the leftmost leaf
        Tuple<Integer, String> removed = btree.delete( 2 );
        assertEquals( 2, removed.getKey().intValue() );
        assertEquals( "v2", removed.getValue() );
        checkNull( btree, 2 );

        // delete the third element in the first leaf
        removed = btree.delete( 7 );
        assertEquals( 7, removed.getKey().intValue() );
        assertEquals( "v7", removed.getValue() );
        checkNull( btree, 7 );

        // Case 2 : Delete the second element in the leftmost leaf
        removed = btree.delete( 6 );
        assertEquals( 6, removed.getKey().intValue() );
        assertEquals( "v6", removed.getValue() );
        checkNull( btree, 6 );

        // delete the third element in the first leaf
        removed = btree.delete( 11 );
        assertEquals( 11, removed.getKey().intValue() );
        assertEquals( "v11", removed.getValue() );
        checkNull( btree, 11 );

        // Case 3 : delete the rightmost element in the btree in the rightmost leaf
        removed = btree.delete( 99 );
        assertEquals( 99, removed.getKey().intValue() );
        assertEquals( "v99", removed.getValue() );
        checkNull( btree, 99 );

        // delete the third element in the last leaf
        removed = btree.delete( 98 );
        assertEquals( 98, removed.getKey().intValue() );
        assertEquals( "v98", removed.getValue() );
        checkNull( btree, 98 );

        // Case 2 : Delete the first element in the rightmost leaf
        removed = btree.delete( 94 );
        assertEquals( 94, removed.getKey().intValue() );
        assertEquals( "v94", removed.getValue() );
        checkNull( btree, 94 );

        // delete the third element in the last leaf
        removed = btree.delete( 95 );
        assertEquals( 95, removed.getKey().intValue() );
        assertEquals( "v95", removed.getValue() );
        checkNull( btree, 95 );

        // Case 5 : delete the leftmost element which is referred in the root node
        removed = btree.delete( 22 );
        assertEquals( 22, removed.getKey().intValue() );
        assertEquals( "v22", removed.getValue() );
        checkNull( btree, 22 );

        // delete the third element in the last leaf
        removed = btree.delete( 27 );
        assertEquals( 27, removed.getKey().intValue() );
        assertEquals( "v27", removed.getValue() );
        checkNull( btree, 27 );

        // Case 6 : delete the leftmost element in a leaf in the middle of the tree
        removed = btree.delete( 70 );
        assertEquals( 70, removed.getKey().intValue() );
        assertEquals( "v70", removed.getValue() );
        checkNull( btree, 70 );

        // delete the third element in the leaf
        removed = btree.delete( 71 );
        assertEquals( 71, removed.getKey().intValue() );
        assertEquals( "v71", removed.getValue() );
        checkNull( btree, 71 );

        // Case 7 : delete the rightmost element in a leaf in the middle of the tree
        removed = btree.delete( 51 );
        assertEquals( 51, removed.getKey().intValue() );
        assertEquals( "v51", removed.getValue() );
        checkNull( btree, 51 );

        // delete the third element in the leaf
        removed = btree.delete( 50 );
        assertEquals( 50, removed.getKey().intValue() );
        assertEquals( "v50", removed.getValue() );
        checkNull( btree, 50 );

        btree.close();
    }


    /**
     * Test various deletions in a two level high tree, when we have leaves
     * containing N/2 elements (thus each deletion leads to a merge)
     */
    @Test
    public void testDelete2LevelsTreeWithHalfFullLeaves() throws Exception
    {
        // Create a BTree with pages containing 4 elements
        BTree<Integer, String> btree = createTwoLevelBTreeHalfFullLeaves();

        // Test removals leadings to various merges.
        // Delete from the middle, not the leftmost value of the leaf
        btree.delete( 10 );
        checkNull( btree, 10 );

        // Delete the extraneous value
        btree.delete( 9 );
        checkNull( btree, 9 );

        // Delete the leftmost element in the middle
        btree.delete( 13 );
        checkNull( btree, 13 );

        // Delete the extraneous value
        btree.delete( 14 );
        checkNull( btree, 14 );

        // Delete the rightmost value
        btree.delete( 18 );
        checkNull( btree, 18 );

        // Delete the extraneous value
        btree.delete( 5 );
        checkNull( btree, 5 );

        // Delete the leftmost value of the right leaf
        btree.delete( 6 );
        checkNull( btree, 6 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 1: remove the leftmost element
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowRight1() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 2, 3, 6, 7 );

        // delete the element
        checkRemoval( btree, 10, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 2: remove an element on the leftmost page but not the first one
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowRight2() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 2, 3, 6, 7 );

        // delete the element
        checkRemoval( btree, 11, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 3: remove an element on the rightmost page on the leftmost node on the upper level
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowRight3() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 2, 3, 6, 7 );

        // delete the element
        checkRemoval( btree, 19, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 4: remove the first element in a page in the middle of the first node
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowRight4() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 2, 3, 6, 7 );

        // delete the element
        checkRemoval( btree, 14, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 5: remove the second element in a page in the middle of the first node
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowRight5() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 2, 3, 6, 7 );

        // delete the element
        checkRemoval( btree, 15, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 1: remove the rightmost element
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft1() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 94, 95, 98, 99 );

        // delete the element
        checkRemoval( btree, 91, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 1: remove the element before the rightmost element
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft2() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 94, 95, 98, 99 );

        // delete the element
        checkRemoval( btree, 90, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 1: remove the leftmost element  of the rightmost leaf
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft3() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 94, 95, 98, 99 );

        // delete the element
        checkRemoval( btree, 82, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 1: remove the second elemnt of the leftmost page on the rightmost second level node
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft4() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 94, 95, 98, 99 );

        // delete the element
        checkRemoval( btree, 83, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 6: remove the first element of a leaf in the middle of the tree
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft6() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 42, 43, 46, 47 );

        // delete
        checkRemoval( btree, 50, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 7: remove the second element of a leaf in the middle of the tree
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft7() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 42, 43, 46, 47 );

        // delete
        checkRemoval( btree, 51, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 8: remove the last element of a leaf in the middle of the tree
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft8() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 42, 43, 46, 47 );

        // delete
        checkRemoval( btree, 59, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 9: remove the element before the last one of a leaf in the middle of the tree
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft9() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 42, 43, 46, 47 );

        // delete
        checkRemoval( btree, 58, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 10: remove the mid element of a leaf in the middle of the tree
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft10() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 42, 43, 46, 47 );

        // delete
        checkRemoval( btree, 54, EXPECTED1 );

        btree.close();
    }


    /**
     * Test deletions in a tree with more than one level. We are specifically testing
     * the deletions that will make a node borrowing some element from a sibling.
     *
     * 11: remove the mid+1 element of a leaf in the middle of the tree
     */
    @Test
    public void testDeleteMultiLevelsLeadingToNodeBorrowLeft11() throws Exception
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // deleting as many elements as necessary to get the node ready for a merge
        delete( btree, EXPECTED1, 42, 43, 46, 47 );

        // delete
        checkRemoval( btree, 55, EXPECTED1 );

        btree.close();
    }


    /**
     * Test the addition of elements with null values
     */
    @Test
    public void testAdditionNullValues() throws IOException, KeyNotFoundException
    {
        BTree<Integer, String> btree = createMultiLevelBTreeLeavesHalfFull();

        // Adding an element with a null value
        btree.insert( 100, null );

        assertTrue( btree.hasKey( 100 ) );

        try
        {
            assertNull( btree.get( 100 ) );
        }
        catch ( KeyNotFoundException knfe )
        {
            fail();
        }

        Tuple<Integer, String> deleted = btree.delete( 100 );

        assertNotNull( deleted );
        assertNull( deleted.getValue() );

        btree.close();
    }


    /**
     * Test the insertion of 5 million elements in a BTree
     * @throws Exception
     */
    @Test
    public void testBrowse500K() throws Exception
    {
        Random random = new Random( System.nanoTime() );

        int nbError = 0;

        int n = 0;
        int nbElems = 500000;
        long delta = System.currentTimeMillis();

        // Create a BTree with 5 million entries
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 32 );

        for ( int i = 0; i < nbElems; i++ )
        {
            Long key = ( long ) random.nextLong();
            String value = Long.toString( key );

            try
            {
                btree.insert( key, value );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                System.out.println( btree );
                System.out.println( "Error while adding " + value );
                nbError++;
                btree.close();

                return;
            }

            if ( i % 100000 == 0 )
            {
                if ( n > 0 )
                {
                    long t0 = System.currentTimeMillis();
                    System.out.println( "Delta" + n + ": " + ( t0 - delta ) );
                    delta = t0;
                }

                n++;
            }
        }

        // Now browse them
        long l1 = System.currentTimeMillis();

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

        System.out.println( "Nb elements read : " + nb );

        cursor.close();
        btree.close();

        long l2 = System.currentTimeMillis();

        System.out.println( "Delta : " + ( l2 - l1 ) + ", nbError = " + nbError
            + ", Nb searches per second : " + ( ( nbElems * 1000 ) / ( l2 - l1 ) ) );
    }


    private void checkNull( BTree<Long, String> btree, long key ) throws IOException
    {
        try
        {
            btree.get( key );
            fail();
        }
        catch ( KeyNotFoundException knfe )
        {
            // expected
        }
    }


    private void checkNull( BTree<Integer, String> btree, int key ) throws IOException
    {
        try
        {
            btree.get( key );
            fail();
        }
        catch ( KeyNotFoundException knfe )
        {
            // expected
        }
    }


    /**
     * Test a browse forward and backward
     */
    @Test
    public void testBrowseForwardBackwardExtremes() throws Exception
    {
        // Create a BTree with pages containing 4 elements
        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 4 );

        for ( int i = 8; i < 13; i++ )
        {
            String strValue = "V" + i;
            btree.insert( i, strValue );
        }

        // Start to browse in the middle
        TupleCursor<Integer, String> cursor = btree.browseFrom( 8 );

        assertTrue( cursor.hasNext() );

        // Get 8
        assertEquals( 8, cursor.next().getKey().intValue() );

        // get 9
        assertEquals( 9, cursor.next().getKey().intValue() );

        // get 10
        assertEquals( 10, cursor.next().getKey().intValue() );

        // get 11
        assertEquals( 11, cursor.next().getKey().intValue() );

        // get 12 (now, we must have gone through at least 2 pages)
        assertEquals( 12, cursor.next().getKey().intValue() );

        assertFalse( cursor.hasNext() );
        assertTrue( cursor.hasPrev() );

        // Lets go backward.
        assertEquals( 11, cursor.prev().getKey().intValue() );

        // Get 10
        assertEquals( 10, cursor.prev().getKey().intValue() );

        // Get 9
        assertEquals( 9, cursor.prev().getKey().intValue() );

        // Get 8
        assertEquals( 8, cursor.prev().getKey().intValue() );

        assertFalse( cursor.hasPrev() );
        assertTrue( cursor.hasNext() );

        cursor.close();
        btree.close();
    }


    @Test
    public void testNextAfterPrev() throws Exception
    {
        IntSerializer serializer = IntSerializer.INSTANCE;

        InMemoryBTreeConfiguration<Integer, Integer> config = new InMemoryBTreeConfiguration<Integer, Integer>();
        config.setName( "master" );
        config.setPageSize( 4 );
        config.setSerializers( serializer, serializer );
        BTree<Integer, Integer> btree = new InMemoryBTree<Integer, Integer>( config );

        int i = 7;
        for ( int k = 0; k < i; k++ )
        {
            btree.insert( k, k );
        }

        // 3 is the last element of the first leaf
        TupleCursor<Integer, Integer> cursor = btree.browseFrom( 4 );

        assertTrue( cursor.hasNext() );
        Tuple<Integer, Integer> tuple = cursor.next();
        assertEquals( Integer.valueOf( 4 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 4 ), tuple.getValue() );

        assertTrue( cursor.hasPrev() );
        tuple = cursor.prev();
        assertEquals( Integer.valueOf( 3 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 3 ), tuple.getValue() );

        assertTrue( cursor.hasNext() );
        tuple = cursor.next();
        assertEquals( Integer.valueOf( 4 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 4 ), tuple.getValue() );

        cursor.close();
        btree.close();
    }


    @Test
    public void testCheckRootPageContents() throws Exception
    {
        IntSerializer ser = IntSerializer.INSTANCE;
        BTree<Integer, Integer> btree = BTreeFactory.createInMemoryBTree( "master1", ser, ser, 4 );

        for ( int i = 1; i < 8; i++ )
        {
            btree.insert( i, i );
        }

        System.out.println( btree.getRootPage() );
        assertEquals( 2, btree.getRootPage().getNbElems() );

        assertEquals( 7, btree.getRootPage().findRightMost().getKey().intValue() );

        assertEquals( 1, btree.getRootPage().findLeftMost().getKey().intValue() );

        btree.close();
    }


    /**
     * Test the overwriting of elements
     */
    @Test
    public void testOverwrite() throws Exception
    {
        BTree<Integer, Integer> btree = BTreeFactory.createInMemoryBTree( "test", IntSerializer.INSTANCE,
            IntSerializer.INSTANCE );

        // Adding an element with a null value
        btree.insert( 1, 1 );

        assertTrue( btree.hasKey( 1 ) );

        assertEquals( Integer.valueOf( 1 ), btree.get( 1 ) );

        btree.insert( 1, 10 );

        assertTrue( btree.hasKey( 1 ) );
        assertEquals( Integer.valueOf( 10 ), btree.get( 1 ) );

        btree.close();
    }
}
