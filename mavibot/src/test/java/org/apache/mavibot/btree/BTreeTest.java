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
    private boolean checkTree( Set<Long> expected, BTree<Long, String> btree ) throws IOException
    {
        for ( Long key : expected )
        {
            String value = btree.find( key );
            
            if ( value == null )
            {
                return false;
            }

            //System.out.println( "found : " + value );
        }
        
        return true;
    }

    private BTree<Long, String> loadTree( Long[] keys, String[] values ) throws IOException
    {
        BTree<Long, String> btree = new BTree<Long, String>( new LongComparator(), 16 );

        for ( int i = 0; i < keys.length; i++ )
        {
            btree.insert( keys[i], values[i] );
        }
        
        return btree;
    }
    
    
    private void dump( List<Long> added )
    {
        boolean isFirst = true;
        
        for ( Long element : added )
        {
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                System.out.print( ", " );
            }
            
            System.out.print( element + "L" );
        }
        
        System.out.println();
    }
    
    
    @Test
    public void testPageInsert1() throws Exception
    {
        BTree<Long, String> btree = new BTree<Long, String>( new LongComparator() );
        
        Long key = Long.valueOf( 10 );
        String value = "V10";
        btree.insert( key, value );

        key = Long.valueOf( 5 );
        value = "V5";
        btree.insert( key, value );

        key = Long.valueOf( 15 );
        value = "V15";
        btree.insert( key, value );
        
        //System.out.println( btree );
    }
    
    
    @Test
    public void testPageInsert() throws Exception
    {
        Set<Long> expected = new HashSet<Long>();
        List<Long> added = new ArrayList<Long>();
        
        Random random = new Random( System.nanoTime() );
        
        int nbError = 0;
        
        long l1 = System.currentTimeMillis();
        
        for ( int j = 0; j < 100; j++ )
        {
            BTree<Long, String> btree = new BTree<Long, String>( new LongComparator() );
            btree.setPageSize( 8 );

            for ( int i = 0; i < 65536; i++ )
            {
                Long key = (long)random.nextInt( 256 );
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
                    dump( added );
                    nbError++;
                    return;
                }
    
                //System.out.println( btree );
                //dump( added );
            }

            if ( !checkTree( expected, btree ) )
            {
                System.out.println( btree );
            }
            
            assertTrue( checkTree( expected, btree ) );

            expected.clear();
            added.clear();
        }

        long l2 = System.currentTimeMillis();
        
        System.out.println( "Delta : " + ( l2 - l1 ) + ", nbError = " + nbError );
    }
    
    
    @Test
    public void testPageInsertEven() throws Exception
    {
        Long[] keys = new Long[]{ 2L, 4L, 6L, 8L };
        String[] values = new String[]{ "V2", "V4", "V6", "V8" };
        
        BTree<Long, String> btree = loadTree( keys, values );
        
        // Insert 1L
        btree.insert( 1L, "V1" );
        
        System.out.println( btree );
        
        btree = loadTree( keys, values );
        
        // Insert 3L
        btree.insert( 3L, "V3" );
        
        System.out.println( btree );
        
        btree = loadTree( keys, values );
        
        // Insert 5L
        btree.insert( 5L, "V5" );
        
        System.out.println( btree );
        
        btree = loadTree( keys, values );
        
        // Insert 7L
        btree.insert( 7L, "V7" );
        
        System.out.println( btree );
        
        btree = loadTree( keys, values );
        
        // Insert 9L
        btree.insert( 9L, "V9" );
        
        System.out.println( btree );
    }


    @Test
    public void testPageInsert2() throws Exception
    {
        Long[] keys = new Long[]{ 128L, 241L, 58L };
        String[] values = new String[]{ "V128", "V241", "V58" };
        
        BTree<Long, String> btree = loadTree( keys, values );
        
        System.out.println( btree );
    }
    
    
    @Test
    @Ignore
    public void testPageInsert3() throws Exception
    {
        BTree<Long, String> btree = new BTree<Long, String>( new LongComparator() );
        btree.setPageSize( 4 );

        Long[]elems = new Long[]
            {
            102L, 198L, 229L, 202L, 160L, 108L, 128L, 130L,
            233L, 226L, 215L,  88L, 217L, 235L, 173L,  81L,
            133L, 131L, 199L, 237L, 100L,  70L, 203L, 216L,
             90L, 114L, 133L, 103L, 127L, 144L, 163L
            };
        
        for ( Long elem : elems )
        {
            String value = "V" + elem;
            btree.insert( elem, value );
            
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
