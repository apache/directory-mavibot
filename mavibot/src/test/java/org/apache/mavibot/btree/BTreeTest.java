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

import org.apache.mavibot.btree.comparator.LongComparator;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 * A unit test class for BTree
 * 
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class BTreeTest
{
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
}
