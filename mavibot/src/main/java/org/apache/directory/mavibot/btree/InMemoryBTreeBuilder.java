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


import static org.apache.directory.mavibot.btree.InMemoryBTreeFactory.createLeaf;
import static org.apache.directory.mavibot.btree.InMemoryBTreeFactory.createNode;
import static org.apache.directory.mavibot.btree.InMemoryBTreeFactory.setKey;
import static org.apache.directory.mavibot.btree.InMemoryBTreeFactory.setValue;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * A BTree builder that builds a tree from the bottom.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryBTreeBuilder<K, V>
{
    private String name;

    private int numKeysInNode;

    private ElementSerializer<K> keySerializer;

    private ElementSerializer<V> valueSerializer;


    public InMemoryBTreeBuilder( String name, int numKeysInNode, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer )
    {
        this.name = name;
        this.numKeysInNode = numKeysInNode;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }


    @SuppressWarnings("unchecked")
    public BTree<K, V> build( Iterator<Tuple<K, V>> sortedTupleItr ) throws IOException
    {
        BTree<K, V> btree = new InMemoryBTree<K, V>( name, keySerializer, valueSerializer );
        btree.init();

        List<Page<K, V>> lstLeaves = new ArrayList<Page<K, V>>();

        int totalTupleCount = 0;

        InMemoryLeaf<K, V> leaf1 = createLeaf( btree, 0, numKeysInNode );
        lstLeaves.add( leaf1 );

        int leafIndex = 0;
        while ( sortedTupleItr.hasNext() )
        {
            Tuple<K, V> tuple = sortedTupleItr.next();

            setKey( leaf1, leafIndex, tuple.getKey() );

            InMemoryValueHolder<V> eh = new InMemoryValueHolder<V>( btree, tuple.getValue() );

            setValue( leaf1, leafIndex, eh );

            leafIndex++;
            totalTupleCount++;
            if ( ( totalTupleCount % numKeysInNode ) == 0 )
            {
                leafIndex = 0;
                leaf1 = createLeaf( btree, 0, numKeysInNode );
                lstLeaves.add( leaf1 );
            }
        }

        if ( lstLeaves.isEmpty() )
        {
            return btree;
        }

        // remove null keys and values from the last leaf and resize
        InMemoryLeaf<K, V> lastLeaf = (InMemoryLeaf<K, V> ) lstLeaves.get( lstLeaves.size() - 1 );
        
        for ( int i = 0; i < lastLeaf.getNbElems(); i++ )
        {
            if ( lastLeaf.getKeys()[i] == null )
            {
                int n = i;
                lastLeaf.setNbElems( n );
                KeyHolder<K>[] keys = lastLeaf.getKeys();

                lastLeaf.setKeys( ( KeyHolder[] ) Array.newInstance( KeyHolder.class, n ) );
                System.arraycopy( keys, 0, lastLeaf.getKeys(), 0, n );

                ValueHolder<V>[] values = lastLeaf.values;
                lastLeaf.values = (InMemoryValueHolder<V>[] ) Array.newInstance( InMemoryValueHolder.class, n );
                System.arraycopy( values, 0, lastLeaf.values, 0, n );

                break;
            }
        }

        Page<K, V> rootPage = attachNodes( lstLeaves, btree );

        System.out.println("built rootpage : " + rootPage);
        
        ((AbstractBTree<K, V>)btree).setRootPage( rootPage );

        return btree;
    }


    @SuppressWarnings("unchecked")
    private Page<K, V> attachNodes( List<Page<K, V>> children, BTree<K, V> btree ) throws IOException
    {
        if ( children.size() == 1 )
        {
            return children.get( 0 );
        }

        List<Page<K, V>> lstNodes = new ArrayList<Page<K, V>>();

        int numChildren = numKeysInNode + 1;

        InMemoryNode<K, V> node = createNode( btree, 0, numKeysInNode );
        lstNodes.add( node );
        int i = 0;
        int totalNodes = 0;

        for ( Page<K, V> p : children )
        {
            if ( i != 0 )
            {
                setKey( node, i - 1, p.getLeftMostKey() );
            }

            node.setPageHolder( i, new PageHolder<K, V>( btree, p ) );

            i++;
            totalNodes++;

            if ( ( totalNodes % numChildren ) == 0 )
            {
                i = 0;
                node = createNode( btree, 0, numKeysInNode );
                lstNodes.add( node );
            }
        }

        // remove null keys and values from the last node and resize
        AbstractPage<K, V> lastNode = ( AbstractPage<K, V> ) lstNodes.get( lstNodes.size() - 1 );

        for ( int j = 0; j < lastNode.getNbElems(); j++ )
        {
            if ( lastNode.getKey( j ) == null )
            {
                int n = j;
                lastNode.setNbElems( n );
                KeyHolder<K>[] keys = lastNode.getKeys();

                lastNode.setKeys( ( KeyHolder[] ) Array.newInstance( KeyHolder.class, n ) );
                System.arraycopy( keys, 0, lastNode.getKeys(), 0, n );

                break;
            }
        }

        return attachNodes( lstNodes, btree );
    }
}
