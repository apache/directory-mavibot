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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.mavibot.btree.comparator.IntComparator;
import org.apache.mavibot.btree.comparator.LongComparator;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;


/**
 * A unit test class for BTree
 * 
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class BTreeTest
{
    // Some values to inject in a btree
    private static int[] sortedValues = new int[]
        {
          0,   1,   2,   4,   5,   6,   8,   9,  11,  12,
         13,  14,  16,  19,  21,  22,  23,  25,  26,  28,
         30,  31,  32,  34,  36,  37,  38,  39,  41,  42,
         44,  45,  47,  50,  52,  53,  54,  55,  56,  58,
         59,  60,  63,  64,  67,  68,  70,  72,  73,  74,
         76,  77,  79,  80,  81,  82,  85,  88,  89,  90,
         92,  93,  95,  97,  98, 100, 101, 102, 103, 104,
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
    private boolean checkTree( Set<Long> expected, BTree<Long, String> btree ) throws IOException
    {
        // We loop on all the expected value to see if they have correctly been inserted
        // into the btree
        for ( Long key : expected )
        {
            String value = btree.find( key );
            
            if ( value == null )
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
        
        for ( int j = 0; j < 1000; j++ )
        {
            BTree<Long, String> btree = new BTree<Long, String>( new LongComparator() );
            btree.setPageSize( 8 );

            for ( int i = 0; i < 1000; i++ )
            {
                Long key = (long)random.nextInt( 1024 );
                String value = "V" + key;
                expected.add( key );
                added.add( key );
                    
                //System.out.println( "Adding " + i + "th : " + key );

                try
                {
                    btree.insert( key, value );
                }
                catch ( Exception e)
                {
                    e.printStackTrace();
                    System.out.println( btree );
                    System.out.println( "Error while adding " + value );
                    nbError++;
                    return;
                }
            }

            assertTrue( checkTree( expected, btree ) );
            
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

            expected.clear();
            added.clear();
        }

        long l2 = System.currentTimeMillis();
        
        System.out.println( "Delta : " + ( l2 - l1 ) + ", nbError = " + nbError );
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
        
        BTree<Long, String> btree = new BTree<Long, String>( new LongComparator() );
        btree.setPageSize( 8 );

        // Insert some values
        for ( int i = 0; i < 8; i++ )
        {
            Long key = (long)random.nextInt( 1024 );
            String value = "V" + key;
            added.add( key );

            try
            {
                btree.insert( key, value );
            }
            catch ( Exception e)
            {
                e.printStackTrace();
                System.out.println( btree );
                System.out.println( "Error while adding " + value );
                return;
            }
        }
        
        assertTrue( checkTree( expected, btree ) );
        
        // Now, delete entries
        for ( long key : added )
        {
            //System.out.println( "Removing " + key + " from " + btree );
            try
            {
                btree.delete( key );
            }
            catch ( Exception e)
            {
                e.printStackTrace();
                System.out.println( btree );
                System.out.println( "Error while deleting " + key );
                return;
            }

            assertTrue( checkTree( expected, btree ) );
        }

    }
    
    
    /**
     * This test is used to debug some corner cases.
     * We don't run it except to check a special condition
     */
    @Test
    @Ignore
    public void testPageInsertDebug() throws Exception
    {
        BTree<Long, String> btree = new BTree<Long, String>( new LongComparator() );
        btree.setPageSize( 4 );

        Long[] elems = new Long[]
            {
                235L, 135L, 247L, 181L,  12L, 112L, 117L, 253L,
                 37L, 158L,  56L, 118L, 184L, 101L, 173L, 126L,
                 61L,  81L, 140L, 173L,  32L, 163L, 224L, 114L,
                133L,  18L,  14L,  82L, 107L, 219L, 244L, 255L,
                  6L, 103L, 170L, 151L, 134L, 196L, 155L,  97L,
                 80L, 122L,  89L, 253L,  33L, 101L,  56L, 168L,
                253L, 187L,  99L,  58L, 151L, 206L,  34L,  96L,
                 20L, 188L, 143L, 150L,  76L, 111L, 234L,  66L,
                 12L, 194L, 164L, 190L,  19L, 192L, 161L, 147L,
                 92L,  89L, 237L, 187L, 250L,  13L, 233L,  34L,
                187L, 232L, 248L, 237L, 129L,   1L, 233L, 252L,
                 18L,  98L,  56L, 121L, 162L, 233L,  29L,  48L,
                176L,  48L, 182L, 130L
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
                if ( btree.find( elems[i] ) == null )
                {
                    System.out.println("Bad tree, missing " + elems[i] + ", " + btree);
                }
            }
            
            if ( size == 27 )
            {
                System.out.println( btree );
            }
            //System.out.println( "added " + elem + ":\n" + btree );
        }
        
        //btree.insert( 115L, "V115" );

        //System.out.println( btree );
    }


    /*
    @Test
    public void testPageRemove() throws Exception
    {
        Long[] keys = new Long[]{  101L, 113L, 20L, 72L, 215L, 239L, 108L, 21L };
        
        BTree<Long, String> btree = new BTree<Long, String>( new LongComparator(), 8 );
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
        BTree<Integer, String> btree = new BTree<Integer, String>( new IntComparator() );
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
            String value = btree.find( key );
            
            assertNotNull( value );
        }

        // Browse starting at position 10
        int pos = 10;
        Cursor<Integer, String> cursor = btree.browse( sortedValues[pos] );
        
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
        cursor = btree.browse( 7 );
        
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
    }
    
    
    /**
     * Test the browse method going backward
     * @throws Exception
     */
    @Test
    public void testBrowseBackward() throws Exception
    {
        
        // Create a BTree with pages containing 8 elements
        BTree<Integer, String> btree = new BTree<Integer, String>( new IntComparator() );
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
            String value = btree.find( key );
            
            assertNotNull( value );
        }

        // Browse starting at position 10
        int pos = 10;
        Cursor<Integer, String> cursor = btree.browse( sortedValues[pos] );
        
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
        cursor = btree.browse( 7 );
        
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
    }
    
    
    /**
     * Test a browse over an empty tree
     */
    @Test
    public void testBrowseEmptyTree() throws Exception
    {
        // Create a BTree with pages containing 8 elements
        BTree<Integer, String> btree = new BTree<Integer, String>( new IntComparator() );
        btree.setPageSize( 8 );

        Cursor<Integer, String> cursor = btree.browse();
        
        assertFalse( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );
        
        cursor.close();
    }
    
    
    /**
     * Test a browse forward and backward
     */
    @Test
    public void testBrowseForwardBackward() throws Exception
    {
        // Create a BTree with pages containing 4 elements
        BTree<Integer, String> btree = new BTree<Integer, String>( new IntComparator() );
        btree.setPageSize( 4 );

        for ( int i = 0; i < 16; i++ )
        {
            String strValue = "V" + i;
            btree.insert( i, strValue );
        }
        
        // Start to browse in the middle
        Cursor<Integer, String> cursor = btree.browse( 8 );

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
        
        // Lets go backward. We should get the same value, as the next() call have incremented the counter
        assertEquals( 12, cursor.prev().getKey().intValue() );

        // Get 11
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
    }
    
    
    @Test
    public void testDelete() throws Exception
    {
        // Create a BTree with pages containing 4 elements
        BTree<Integer, String> btree = new BTree<Integer, String>( new IntComparator() );
        btree.setPageSize( 4 );
        
        // Create a tree with 5 children containing 4 elements each. The tree is full.
        int[] keys = new int[] {1, 2, 5, 6, 3, 4, 9, 10, 7, 8, 9, 10, 7, 8, 13, 14, 11, 12, 17, 18, 15, 16, 19, 20 };

        for ( int key : keys )
        {
            String value = "V" + key;
            btree.insert( key, value );
        }
        
        // Delete one element in the middle of a leaf
        btree.delete( 10 );
        assertNull( btree.find( 10 ) );
        
        // Delete one element at the beginning of a leaf
        btree.delete( 13 );
        assertNull( btree.find( 13 ) );
        assertEquals( Integer.valueOf( 14 ), ((Node<Integer, String>)btree.rootPage).keys[2] );
    }
}
