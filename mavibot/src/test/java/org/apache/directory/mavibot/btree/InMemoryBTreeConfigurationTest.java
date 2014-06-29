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


import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test the creation of a BTree with a configuration.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryBTreeConfigurationTest
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


    /**
     * Test the creation of a in-memory BTree using the BTreeConfiguration.
     */
    @Test
    public void testConfigurationBasic() throws IOException, KeyNotFoundException
    {
        InMemoryBTreeConfiguration<Integer, String> config = new InMemoryBTreeConfiguration<Integer, String>();
        config.setName( "basic" );
        config.setPageSize( 32 );
        config.setSerializers( IntSerializer.INSTANCE, StringSerializer.INSTANCE );

        try
        {
            // Create the BTree
            BTree<Integer, String> btree = new InMemoryBTree<Integer, String>( config );

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

            btree.close();
        }
        finally
        {
            // Erase the mavibot file now
            File mavibotFile = new File( "", "mavibot" );

            if ( mavibotFile.exists() )
            {
                mavibotFile.delete();
            }

            // Erase the journal too
            File mavibotJournal = new File( "", "mavibot.log" );

            if ( mavibotJournal.exists() )
            {
                mavibotJournal.delete();
            }
        }
    }


    /**
     * Test the creation of a BTree using the BTreeConfiguration, flushing the
     * tree on disk, then reloading it in another BTree.
     */
    @Test
    public void testConfigurationFlushReload() throws IOException, KeyNotFoundException
    {
        // Create a temporary file
        File file = tempFolder.newFile( "testFlush.data" );
        String parent = file.getParent();

        try
        {
            InMemoryBTreeConfiguration<Integer, String> config = new InMemoryBTreeConfiguration<Integer, String>();
            config.setPageSize( 32 );
            config.setSerializers( IntSerializer.INSTANCE, StringSerializer.INSTANCE );

            config.setFilePath( parent );
            config.setName( "mavibot" );

            // Create the BTree
            BTree<Integer, String> btree = new InMemoryBTree<Integer, String>( config );

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

            // Flush the data
            btree.close();

            // Now, create a new BTree using the same configuration
            BTree<Integer, String> btreeCopy = new InMemoryBTree<Integer, String>( config );

            // Check that the tree contains all the values
            for ( int key : sortedValues )
            {
                String value = btreeCopy.get( key );

                assertNotNull( value );
            }

            btreeCopy.close();
        }
        finally
        {
            // Erase the mavibot file now
            File mavibotFile = new File( parent, "mavibot.db" );

            if ( mavibotFile.exists() )
            {
                mavibotFile.delete();
            }

            // Erase the journal too
            File mavibotJournal = new File( parent, "mavibot.db.log" );

            if ( mavibotJournal.exists() )
            {
                mavibotJournal.delete();
            }
        }
    }
}
