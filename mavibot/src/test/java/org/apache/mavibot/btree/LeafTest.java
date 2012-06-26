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

import org.apache.mavibot.btree.comparator.LongComparator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

/**
 * A unit test class for Leaf
 * 
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class LeafTest
{
    private BTree<Long, String> btree = null;
    
    /**
     * Create a btree
     */
    @Before
    public void setup() throws IOException
    {
        btree = new BTree<Long, String>( new LongComparator() );
        btree.setPageSize( 8 );
    }

    
    /**
     * A helper method to insert elements in a Leaf
     */
    private Leaf<Long, String> insert( Leaf<Long, String> leaf, long key, String value )
    {
        InsertResult<Long, String> result = leaf.insert( 1L, key, value );
        
        return (Leaf<Long, String>)((ModifyResult)result).modifiedPage;
    }
    

    /**
     * Test that deleting an entry from an empty page returns a NOT_PRESENT result
     * @throws IOException
     */
    @Test
    public void testDeleteFromEmptyLeaf() throws IOException
    {
        Leaf<Long, String> leaf = new Leaf<Long, String>( btree );
        
        DeleteResult<Long, String> result = leaf.delete( 1L, 1L, null, -1 );
        
        assertEquals( NotPresentResult.NOT_PRESENT, result );
    }


    /**
     * Test that deleting an entry which is not present in the leaf works
     * @throws IOException
     */
    @Test
    public void testDeleteNotPresentElementFromRootLeaf() throws IOException
    {
        Leaf<Long, String> leaf = new Leaf<Long, String>( btree );
        leaf = insert( leaf, 1L, "v1" );
        leaf = insert( leaf, 2L, "v2" );
        leaf = insert( leaf, 3L, "v3" );
        leaf = insert( leaf, 4L, "v4" );
        
        DeleteResult<Long, String> result = leaf.delete( 2L, 5L, null, -1 );
        
        assertEquals( NotPresentResult.NOT_PRESENT, result );
    }


    /**
     * Test that deleting an entry which is present in the leaf works
     * @throws IOException
     */
    @Test
    public void testDeletePresentElementFromRootLeaf() throws IOException
    {
        Leaf<Long, String> leaf = new Leaf<Long, String>( btree );
        leaf = insert( leaf, 1L, "v1" );
        leaf = insert( leaf, 2L, "v2" );
        leaf = insert( leaf, 3L, "v3" );
        leaf = insert( leaf, 4L, "v4" );
        
        DeleteResult<Long, String> result = leaf.delete( 4L, 3L, null, -1 );
        
        assertTrue( result instanceof RemoveResult );
        
        Tuple<Long, String> removedElement = ((RemoveResult)result).removedElement;
        Page<Long, String> newLeaf = ((RemoveResult)result).modifiedPage;
        
        assertEquals( Long.valueOf( 3L), removedElement.getKey() );
        assertEquals( "v3", removedElement.getValue() );
        assertEquals( 3, newLeaf.getNbElems() );
        
        assertEquals( "v1", newLeaf.find( 1L ) );
        assertEquals( "v2", newLeaf.find( 2L ) );
        assertNull( newLeaf.find( 3L ) );
        assertEquals( "v4", newLeaf.find( 4L ) );
    }
}
