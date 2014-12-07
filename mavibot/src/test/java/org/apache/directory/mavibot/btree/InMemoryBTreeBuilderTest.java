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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Test cases for BTreeBuilder.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryBTreeBuilderTest
{
    @Test
    @Ignore
    public void testIntegerTree() throws IOException, KeyNotFoundException
    {
        List<Tuple<Integer, Integer>> sortedTuple = new ArrayList<Tuple<Integer, Integer>>();

        for ( int i = 1; i < 8; i++ )
        {
            Tuple<Integer, Integer> t = new Tuple<Integer, Integer>( i, i );
            sortedTuple.add( t );
        }

        IntSerializer ser = IntSerializer.INSTANCE;
        InMemoryBTreeBuilder<Integer, Integer> bb = new InMemoryBTreeBuilder<Integer, Integer>( "master", 4, ser, ser );

        // contains 1, 2, 3, 4, 5, 6, 7
        BTree<Integer, Integer> btree = bb.build( sortedTuple.iterator() );

        assertEquals( 1, btree.getRootPage().getNbElems() );

        assertEquals( 7, btree.getRootPage().findRightMost().getKey().intValue() );

        assertEquals( 1, btree.getRootPage().findLeftMost().getKey().intValue() );

        TupleCursor<Integer, Integer> cursor = btree.browse();
        int i = 0;

        while ( cursor.hasNext() )
        {
            Tuple<Integer, Integer> expected = sortedTuple.get( i++ );
            Tuple<Integer, Integer> actual = cursor.next();
            assertEquals( expected.getKey(), actual.getKey() );
            assertEquals( expected.getValue(), actual.getValue() );
        }

        cursor.close();
        btree.close();
    }

}
