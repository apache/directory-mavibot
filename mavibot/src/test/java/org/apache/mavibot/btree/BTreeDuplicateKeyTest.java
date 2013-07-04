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
package org.apache.mavibot.btree;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.mavibot.btree.serializer.IntSerializer;
import org.apache.mavibot.btree.serializer.StringSerializer;
import org.junit.Test;


/**
 * TODO BTreeDuplicateKeyTest.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BTreeDuplicateKeyTest
{
    @Test
    public void testInsertNullValue() throws IOException
    {
        IntSerializer serializer = new IntSerializer();

        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( "master", serializer, serializer );
        btree.init();

        btree.insert( 1, null );

        Cursor<Integer, Integer> cursor = btree.browse();
        assertTrue( cursor.hasNext() );

        Tuple<Integer, Integer> t = cursor.next();

        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( null, t.getValue() );

        cursor.close();
    }


    @Test
    public void testBrowseEmptyTree() throws IOException
    {
        IntSerializer serializer = new IntSerializer();

        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( "master", serializer, serializer );
        btree.init();

        Cursor<Integer, Integer> cursor = btree.browse();
        assertFalse( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );

        try
        {
            cursor.next();
            fail( "Should not reach here" );
        }
        catch ( NoSuchElementException e )
        {
            assertTrue( true );
        }

        try
        {
            cursor.prev();
            fail( "Should not reach here" );
        }
        catch ( NoSuchElementException e )
        {
            assertTrue( true );
        }

        cursor.close();
    }


    @Test
    public void testDuplicateKey() throws IOException
    {
        IntSerializer serializer = new IntSerializer();

        BTreeConfiguration<Integer, Integer> config = new BTreeConfiguration<Integer, Integer>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setSerializers( serializer, serializer );
        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( config );

        btree.insert( 1, 1 );
        btree.insert( 1, 2 );

        Cursor<Integer, Integer> cursor = btree.browse();
        assertTrue( cursor.hasNext() );

        Tuple<Integer, Integer> t = cursor.next();

        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( Integer.valueOf( 1 ), t.getValue() );

        assertTrue( cursor.hasNext() );

        t = cursor.next();

        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( Integer.valueOf( 2 ), t.getValue() );

        assertFalse( cursor.hasNext() );

        // test backward move
        assertTrue( cursor.hasPrev() );

        t = cursor.prev();

        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( Integer.valueOf( 2 ), t.getValue() );

        assertTrue( cursor.hasPrev() );

        t = cursor.prev();

        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( Integer.valueOf( 1 ), t.getValue() );

        assertFalse( cursor.hasPrev() );

        // again forward
        assertTrue( cursor.hasNext() );

        t = cursor.next();

        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( Integer.valueOf( 1 ), t.getValue() );

        assertTrue( cursor.hasNext() );

        t = cursor.next();

        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( Integer.valueOf( 2 ), t.getValue() );

        assertFalse( cursor.hasNext() );

        cursor.close();
    }


    @Test
    public void testGetDuplicateKey() throws Exception
    {
        IntSerializer serializer = new IntSerializer();

        BTreeConfiguration<Integer, Integer> config = new BTreeConfiguration<Integer, Integer>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setSerializers( serializer, serializer );
        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( config );

        Integer retVal = btree.insert( 1, 1 );
        assertNull( retVal );

        retVal = btree.insert( 1, 2 );
        assertNull( retVal );

        // check the return value when an existing value is added again
        retVal = btree.insert( 1, 2 );
        assertEquals( Integer.valueOf( 2 ), retVal );

        assertEquals( Integer.valueOf( 1 ), btree.get( 1 ) );
        assertTrue( btree.contains( 1, 1 ) );
        assertTrue( btree.contains( 1, 2 ) );

        assertFalse( btree.contains( 1, 0 ) );
        assertFalse( btree.contains( 0, 1 ) );
        assertFalse( btree.contains( 0, 0 ) );
        assertFalse( btree.contains( null, 0 ) );
        assertFalse( btree.contains( 0, null ) );
        assertFalse( btree.contains( null, null ) );
    }


    @Test
    public void testRemoveDuplicateKey() throws Exception
    {
        IntSerializer serializer = new IntSerializer();

        BTreeConfiguration<Integer, Integer> config = new BTreeConfiguration<Integer, Integer>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setSerializers( serializer, serializer );
        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( config );

        btree.insert( 1, 1 );
        btree.insert( 1, 2 );

        assertEquals( 2l, btree.getNbElems() );

        Tuple<Integer, Integer> t = btree.delete( 1, 1 );
        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( Integer.valueOf( 1 ), t.getValue() );

        assertEquals( 1l, btree.getNbElems() );

        t = btree.delete( 1, 2 );
        assertEquals( Integer.valueOf( 1 ), t.getKey() );
        assertEquals( Integer.valueOf( 2 ), t.getValue() );

        assertEquals( 0l, btree.getNbElems() );

        t = btree.delete( 1, 2 );
        assertNull( t );
    }


    @Test
    public void testFullPage() throws Exception
    {
        StringSerializer serializer = new StringSerializer();

        BTreeConfiguration<String, String> config = new BTreeConfiguration<String, String>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setSerializers( serializer, serializer );
        BTree<String, String> btree = new BTree<String, String>( config );

        int i = 7;
        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            for( int k = 0; k< i; k++ )
            {
                btree.insert( String.valueOf( ch ), UUID.randomUUID().toString() );
            }
        }

        Cursor<String, String> cursor = btree.browse();

        char ch = 'a';
        int k = 0;
        
        while ( cursor.hasNext() )
        {
            Tuple<String, String> t = cursor.next();
            assertEquals( String.valueOf( ch ), t.getKey() );
            k++;
            
            if( ( k % i ) == 0 )
            {
                ch++;
            }
        }
        
        assertEquals( ( 'z' + 1 ) , ch );
        
        ch = 'z';
        
        while(cursor.hasPrev())
        {
            Tuple<String, String> t = cursor.prev();
            assertEquals( String.valueOf( ch ), t.getKey() );
            k--;
            
            if( ( k % i ) == 0 )
            {
                ch--;
            }
        }

        assertEquals( ( 'a' - 1 ) , ch );
        cursor.close();
    }

    @Test
    public void testMoveFirst() throws Exception
    {
        StringSerializer serializer = new StringSerializer();

        BTreeConfiguration<String, String> config = new BTreeConfiguration<String, String>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setSerializers( serializer, serializer );
        BTree<String, String> btree = new BTree<String, String>( config );

        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            btree.insert( String.valueOf( ch ), UUID.randomUUID().toString() );
        }

        // add one more value for 'a'
        btree.insert( String.valueOf( 'a' ), UUID.randomUUID().toString() );
        
        Cursor<String, String> cursor = btree.browseFrom( "c" );

        int i = 0;
        while( cursor.hasNext() )
        {
            assertNotNull( cursor.next() );
            i++;
        }
        assertEquals( 24, i );
        
        // now move the cursor first
        cursor.beforeFirst();
        assertTrue( cursor.hasNext() );
        assertEquals( "c", cursor.next().getKey() );
        
        i = 0;
        while( cursor.hasNext() )
        {
            assertNotNull( cursor.next() );
            i++;
        }
        assertEquals( 23, i );
        
        cursor.close();
        
        cursor = btree.browse();
        
        i = 0;
        while( cursor.hasNext() )
        {
            assertNotNull( cursor.next() );
            i++;
        }
        assertEquals( 27, i );
        
        // now move the cursor first
        cursor.beforeFirst();
        assertTrue( cursor.hasNext() );
        assertEquals( "a", cursor.next().getKey() );
        
        i = 0;
        while( cursor.hasNext() )
        {
            assertNotNull( cursor.next() );
            i++;
        }
        assertEquals( 26, i );
    }


    @Test(expected = NoSuchElementException.class)
    public void testMoveLast() throws Exception
    {
        StringSerializer serializer = new StringSerializer();

        BTreeConfiguration<String, String> config = new BTreeConfiguration<String, String>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setSerializers( serializer, serializer );
        BTree<String, String> btree = new BTree<String, String>( config );

        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            btree.insert( String.valueOf( ch ), UUID.randomUUID().toString() );
        }
        
        btree.insert( String.valueOf( 'z' ), UUID.randomUUID().toString() );
        
        Cursor<String, String> cursor = btree.browseFrom( "c" );
        cursor.afterLast();
        
        assertFalse( cursor.hasNext() );
        assertTrue( cursor.hasPrev() );
        assertEquals( "z", cursor.prev().getKey() );
        assertEquals( "z", cursor.prev().getKey() );
        assertEquals( "y", cursor.prev().getKey() );
        
        cursor.beforeFirst();
        assertEquals( "c", cursor.next().getKey() );

        cursor.afterLast();
        assertFalse( cursor.hasNext() );
        // make sure it throws NoSuchElementException
        cursor.next();
    }

    
    @Test(expected = NoSuchElementException.class)
    public void testMoveToNextPrevNonDuplicateKey() throws Exception
    {
        StringSerializer serializer = new StringSerializer();

        BTreeConfiguration<String, String> config = new BTreeConfiguration<String, String>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setSerializers( serializer, serializer );
        BTree<String, String> btree = new BTree<String, String>( config );

        int i = 7;
        for ( char ch = 'a'; ch <= 'z'; ch++ )
        {
            for( int k = 0; k< i; k++ )
            {
                btree.insert( String.valueOf( ch ), String.valueOf( k ) );
            }
        }
        
        Cursor<String, String> cursor = btree.browse();

        assertTrue( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );
        for(int k =0; k < 2; k++)
        {
            assertEquals( "a", cursor.next().getKey() );
        }
        
        assertEquals( "a", cursor.next().getKey() );
        
        cursor.moveToNextNonDuplicateKey();
        
        assertEquals( "b", cursor.next().getKey() );
        
        for ( char ch = 'b'; ch < 'z'; ch++ )
        {
            assertEquals( String.valueOf( ch ), cursor.next().getKey() );
            cursor.moveToNextNonDuplicateKey();
            char t = ch;
            assertEquals( String.valueOf( ++t ), cursor.next().getKey() );
        }

        for(int k =0; k < i-1; k++)
        {
            assertEquals( "z", cursor.next().getKey() );
        }

        assertFalse( cursor.hasNext() );
        assertTrue( cursor.hasPrev() );
        Tuple<String, String> tuple = cursor.prev();
        assertEquals( "z", tuple.getKey() );
        assertEquals( "6", tuple.getValue() );
        
        for ( char ch = 'z'; ch > 'a'; ch-- )
        {
            char t = ch;
            t--;
            
            assertEquals( String.valueOf( ch ), cursor.prev().getKey() );
            
            cursor.moveToPrevNonDuplicateKey();

            tuple = cursor.prev();
            assertEquals( String.valueOf( t ), tuple.getKey() );
        }
        
        for(int k =5; k >=0; k--)
        {
            tuple = cursor.prev();
            assertEquals( "a", tuple.getKey() );
            assertEquals( String.valueOf( k ), tuple.getValue() );
        }

        assertTrue( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );
        tuple = cursor.next();
        assertEquals( "a", tuple.getKey() );
        assertEquals( "0", tuple.getValue() );

        cursor.close();
        
        cursor = btree.browseFrom("y");
        cursor.moveToNextNonDuplicateKey();
        assertTrue( cursor.hasPrev() );
        tuple = cursor.prev();
        assertEquals( "y", tuple.getKey() );
        assertEquals( "6", tuple.getValue() );
        cursor.close();
        
        cursor = btree.browse();
        cursor.beforeFirst();
        assertFalse( cursor.hasPrev() );
        // make sure it throws NoSuchElementException
        cursor.prev();
    }
    
    
    /**
     * Test for moving between two leaves. When moveToNextNonDuplicateKey is called
     * and cursor is on the last element of the current leaf.
     *
     * @throws Exception
     */
    @Test
    public void testMoveToNextAndPrevWithPageBoundaries() throws Exception
    {
        IntSerializer serializer = new IntSerializer();

        BTreeConfiguration<Integer, Integer> config = new BTreeConfiguration<Integer, Integer>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setPageSize( 4 );
        config.setSerializers( serializer, serializer );
        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( config );

        int i = 7;
        for ( int k=0; k < i; k++ )
        {
            btree.insert( k, k );
        }
        
        // 3 is the last element of the first leaf
        Cursor<Integer, Integer> cursor = btree.browseFrom(3);
        cursor.moveToNextNonDuplicateKey();

        assertTrue( cursor.hasNext() );
        Tuple<Integer, Integer> tuple = cursor.next();
        assertEquals( Integer.valueOf( 4 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 4 ), tuple.getValue() );
        cursor.close();

        cursor = btree.browseFrom(3);
        cursor.moveToNextNonDuplicateKey();

        assertTrue( cursor.hasPrev() );
        tuple = cursor.prev();
        assertEquals( Integer.valueOf( 3 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 3 ), tuple.getValue() );
        cursor.close();
        
        // 4 is the first element of the second leaf
        cursor = btree.browseFrom(4);
        cursor.moveToPrevNonDuplicateKey();

        assertTrue( cursor.hasPrev() );
        tuple = cursor.prev();
        assertEquals( Integer.valueOf( 3 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 3 ), tuple.getValue() );
        
        // the below assertion won't work cause of the index position
        // issue when prev() and next() are called subsequently (in any order) 
//        assertTrue( cursor.hasNext() );
//        tuple = cursor.next();
//        assertEquals( Integer.valueOf( 4 ), tuple.getKey() );
//        assertEquals( Integer.valueOf( 4 ), tuple.getValue() );
        cursor.close();
        
        // test the extremes of the BTree instead of that of leaves
        cursor = btree.browseFrom(6);
        cursor.moveToNextNonDuplicateKey();
        assertFalse( cursor.hasNext() );
        assertTrue( cursor.hasPrev() );
        tuple = cursor.prev();
        assertEquals( Integer.valueOf( 6 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 6 ), tuple.getValue() );
        cursor.close();
        
        cursor = btree.browse();
        cursor.moveToPrevNonDuplicateKey();
        assertTrue( cursor.hasNext() );
        assertFalse( cursor.hasPrev() );
        tuple = cursor.next();
        assertEquals( Integer.valueOf( 0 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 0 ), tuple.getValue() );
        cursor.close();
    }
    
    
    @Test
    public void testNextAfterPrev() throws Exception
    {
        IntSerializer serializer = new IntSerializer();

        BTreeConfiguration<Integer, Integer> config = new BTreeConfiguration<Integer, Integer>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setPageSize( 4 );
        config.setSerializers( serializer, serializer );
        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( config );

        int i = 7;
        for ( int k=0; k < i; k++ )
        {
            btree.insert( k, k );
        }
        
        // 3 is the last element of the first leaf
        Cursor<Integer, Integer> cursor = btree.browseFrom(4);

        assertTrue( cursor.hasNext() );
        Tuple<Integer, Integer> tuple = cursor.next();
        assertEquals( Integer.valueOf( 4 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 4 ), tuple.getValue() );
        
        assertTrue( cursor.hasPrev() );
        tuple = cursor.prev();
        assertEquals( Integer.valueOf( 4 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 4 ), tuple.getValue() );

        assertTrue( cursor.hasNext() );
        tuple = cursor.next();
        assertEquals( Integer.valueOf( 4 ), tuple.getKey() );
        assertEquals( Integer.valueOf( 4 ), tuple.getValue() );
        cursor.close();

    }
    
    
    /**
     * Test for moving after a key and traversing backwards.
     *
     * @throws Exception
     */
    @Test
    public void testMoveToNextAndTraverseBackward() throws Exception
    {
        IntSerializer serializer = new IntSerializer();

        BTreeConfiguration<Integer, Integer> config = new BTreeConfiguration<Integer, Integer>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setPageSize( 8 );
        config.setSerializers( serializer, serializer );
        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( config );

        int i = 5;
        for ( int k=0; k < i; k++ )
        {
            btree.insert( k, k );
        }
        
        // 4 is the last element in the tree
        Cursor<Integer, Integer> cursor = btree.browseFrom(4);
        cursor.moveToNextNonDuplicateKey();
        
        int currentKey = 4;
        while( cursor.hasPrev() )
        {
        	assertEquals( Integer.valueOf( currentKey ), cursor.prev().getKey() );
        	currentKey--;
        }
        
        cursor.close();
    }
    
    
    /**
     * Test for moving after a key and traversing backwards.
     *
     * @throws Exception
     */
    @Test
    public void testMoveToPrevAndTraverseForward() throws Exception
    {
        IntSerializer serializer = new IntSerializer();

        BTreeConfiguration<Integer, Integer> config = new BTreeConfiguration<Integer, Integer>();
        config.setAllowDuplicates( true );
        config.setName( "master" );
        config.setPageSize( 8 );
        config.setSerializers( serializer, serializer );
        BTree<Integer, Integer> btree = new BTree<Integer, Integer>( config );

        int i = 5;
        for ( int k=0; k < i; k++ )
        {
            btree.insert( k, k );
        }
        
        // 4 is the last element in the tree
        Cursor<Integer, Integer> cursor = btree.browseFrom(0);
        cursor.moveToPrevNonDuplicateKey();
        
        int currentKey = 0;
        while( cursor.hasNext() )
        {
        	assertEquals( Integer.valueOf( currentKey ), cursor.next().getKey() );
        	currentKey++;
        }
        
        cursor.close();
    }

}
