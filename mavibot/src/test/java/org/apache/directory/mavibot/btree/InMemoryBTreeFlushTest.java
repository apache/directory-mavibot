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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Set;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * A unit test class for BTree flush() operation
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryBTreeFlushTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

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


    private String create100KElementsFile() throws IOException
    {
        Random random = new Random( System.nanoTime() );

        int nbError = 0;

        long l1 = System.currentTimeMillis();
        int n = 0;
        long delta = l1;
        int nbElems = 100000;

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

                return null;
            }

            if ( i % 10000 == 0 )
            {
                if ( n > 0 )
                {
                    long t0 = System.currentTimeMillis();
                    System.out.println( "Written " + i + " elements in : " + ( t0 - delta ) + "ms" );
                    delta = t0;
                }

                n++;
            }
        }

        long l2 = System.currentTimeMillis();

        System.out.println( "Delta : " + ( l2 - l1 ) + ", nbError = " + nbError
            + ", Nb insertion per second : " + ( ( nbElems ) / ( l2 - l1 ) ) * 1000 );

        // Now, flush the btree

        File tempFile = tempFolder.newFile( "mavibot.tmp" );

        long t0 = System.currentTimeMillis();

        ( ( InMemoryBTree<Long, String> ) btree ).flush( tempFile );

        long t1 = System.currentTimeMillis();

        System.out.println( "Time to flush 100 000 elements : " + ( t1 - t0 ) + "ms" );
        btree.close();

        return tempFile.getCanonicalPath();
    }


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
     * Test the browse method going backward
     * @throws Exception
     */
    @Test
    public void testFlushBTree() throws Exception
    {
        // Create a BTree with pages containing 8 elements
        String path = tempFolder.getRoot().getCanonicalPath();

        BTree<Integer, String> btree = BTreeFactory.createInMemoryBTree( "test", path, IntSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 8 );

        File journal = ( ( InMemoryBTree<Integer, String> ) btree ).getJournal();
        File data = ( ( InMemoryBTree<Integer, String> ) btree ).getFile();

        try
        {
            // Inject the values
            for ( int value : sortedValues )
            {
                String strValue = "V" + value;

                btree.insert( value, strValue );
            }

            // The journal must be full
            assertTrue( journal.length() > 0 );

            // Now, flush the btree
            btree.flush();

            // The journal must be empty
            assertEquals( 0, journal.length() );

            // Load the data into a new tree
            BTree<Integer, String> btreeLoaded = BTreeFactory.createInMemoryBTree( "test", path, IntSerializer.INSTANCE,
                StringSerializer.INSTANCE );
            btree.setPageSize( 8 );

            TupleCursor<Integer, String> cursor1 = btree.browse();
            TupleCursor<Integer, String> cursor2 = btree.browse();

            while ( cursor1.hasNext() )
            {
                assertTrue( cursor2.hasNext() );

                Tuple<Integer, String> tuple1 = cursor1.next();
                Tuple<Integer, String> tuple2 = cursor2.next();

                assertEquals( tuple1.getKey(), tuple2.getKey() );
                assertEquals( tuple1.getValue(), tuple2.getValue() );
            }

            assertFalse( cursor2.hasNext() );

            btree.close();
            btreeLoaded.close();
        }
        finally
        {
            data.delete();
            journal.delete();
        }
    }


    /**
     * Test the insertion of 5 million elements in a BTree
     * @throws Exception
     */
    @Test
    public void testLoadBTreeFromFile() throws Exception
    {
        String data100K = create100KElementsFile();
        File dataFile = new File( data100K );
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree(
            "test",
            dataFile.getParent(),
            LongSerializer.INSTANCE,
            StringSerializer.INSTANCE );
        btree.setPageSize( 32 );
        btree.close();
    }
}
