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


import static org.apache.mavibot.btree.BTreeFactory.createLeaf;
import static org.apache.mavibot.btree.BTreeFactory.createNode;
import static org.apache.mavibot.btree.BTreeFactory.setKey;
import static org.apache.mavibot.btree.BTreeFactory.setValue;

import java.io.IOException;

import org.apache.mavibot.btree.serializer.IntSerializer;


/**
 * A BTree builder that builds a tree from the bottom.
 *
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class BTreeBuilder<K, V>
{
    public BTreeBuilder()
    {
    }

    public void build() throws IOException
    {
        IntSerializer keySer = new IntSerializer();
        
        BTree<Integer, Integer> btree = new BTree<Integer, Integer>("master", keySer, keySer);
        btree.init();
        Leaf<Integer, Integer> leaf1 = createLeaf( btree, 0, 4 );
        fillLeaf( new int[]
            { 1, 2, 3, 4 }, leaf1, btree );

        Leaf<Integer, Integer> leaf2 = createLeaf( btree, 0, 3 );
        fillLeaf( new int[]
            { 5, 6, 7 }, leaf2, btree );

        Node<Integer, Integer> node = ( Node<Integer, Integer> ) createNode( btree, 0, 1 );

        setKey( node, 0, 5 );
        node.children[0] = btree.createHolder( leaf1 );
        node.children[1] = btree.createHolder( leaf2 );

        btree.rootPage = node;

        Cursor<Integer, Integer> cursor = btree.browse();
        while(cursor.hasNext())
        {
            Tuple<Integer, Integer> t = cursor.next();
            System.out.println( t );
        }
        cursor.close();
    }


    private void fillLeaf( int[] arr, Leaf<Integer, Integer> leaf, BTree<Integer, Integer> btree )
    {
        for ( int i = 0; i < arr.length; i++ )
        {
            setKey( leaf, i, arr[i] );

            MemoryHolder<Integer, Integer> eh = new MemoryHolder<Integer, Integer>( btree, arr[i] );

            setValue( leaf, i, eh );
        }
    }


    public static void main( String[] args ) throws IOException
    {
        BTreeBuilder<Integer, Integer> bb = new BTreeBuilder<Integer, Integer>();
        bb.build();
    }
}
