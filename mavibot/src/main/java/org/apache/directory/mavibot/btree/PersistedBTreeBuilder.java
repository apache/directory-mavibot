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


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * A B-tree builder that builds a tree from the bottom.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PersistedBTreeBuilder<K, V>
{
    private String name;

    private int numKeysInNode;

    private ElementSerializer<K> keySerializer;

    private ElementSerializer<V> valueSerializer;

    private RecordManager rm;


    public PersistedBTreeBuilder( RecordManager rm, String name, int numKeysInNode, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer )
    {
        this.rm = rm;
        this.name = name;
        this.numKeysInNode = numKeysInNode;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }


    @SuppressWarnings("unchecked")
    public BTree<K, V> build( Iterator<Tuple<K, V>> sortedTupleItr ) throws Exception
    {
        BTree<K, V> btree = BTreeFactory.createPersistedBTree( name, keySerializer, valueSerializer );

        rm.manage( btree );

        List<Page<K, V>> lstLeaves = new ArrayList<Page<K, V>>();

        int totalTupleCount = 0;

        PersistedLeaf<K, V> leaf1 = ( PersistedLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0, numKeysInNode );
        lstLeaves.add( leaf1 );

        int leafIndex = 0;

        while ( sortedTupleItr.hasNext() )
        {
            Tuple<K, V> tuple = sortedTupleItr.next();

            BTreeFactory.setKey( btree, leaf1, leafIndex, tuple.getKey() );

            PersistedValueHolder<V> eh = new PersistedValueHolder<V>( btree, tuple.getValue() );

            BTreeFactory.setValue( btree, leaf1, leafIndex, eh );

            leafIndex++;
            totalTupleCount++;
            if ( ( totalTupleCount % numKeysInNode ) == 0 )
            {
                leafIndex = 0;

                PageHolder<K, V> pageHolder = rm.writePage( btree, leaf1, 1 );

                leaf1 = ( PersistedLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0, numKeysInNode );
                lstLeaves.add( leaf1 );
            }

            //TODO build the whole tree in chunks rather than processing *all* leaves at first
        }

        if ( lstLeaves.isEmpty() )
        {
            return btree;
        }

        // remove null keys and values from the last leaf and resize
        PersistedLeaf<K, V> lastLeaf = ( PersistedLeaf<K, V> ) lstLeaves.get( lstLeaves.size() - 1 );
        for ( int i = 0; i < lastLeaf.getNbElems(); i++ )
        {
            if ( lastLeaf.getKey( i ) == null )
            {
                int n = i;
                lastLeaf.setNbElems( n );
                KeyHolder<K>[] keys = lastLeaf.getKeys();

                lastLeaf.setKeys( ( KeyHolder[] ) Array.newInstance( PersistedKeyHolder.class, n ) );
                System.arraycopy( keys, 0, lastLeaf.getKeys(), 0, n );

                ValueHolder<V>[] values = lastLeaf.values;
                lastLeaf.values = ( PersistedValueHolder<V>[] ) Array.newInstance( PersistedValueHolder.class, n );
                System.arraycopy( values, 0, lastLeaf.values, 0, n );

                PageHolder<K, V> pageHolder = rm.writePage( btree, lastLeaf, 1 );

                break;
            }
        }

        // make sure either one of the root pages is reclaimed, cause when we call rm.manage()
        // there is already a root page created
        Page<K, V> rootPage = attachNodes( lstLeaves, btree );

        //System.out.println("built rootpage : " + rootPage);
        ( ( PersistedBTree<K, V> ) btree ).setNbElems( totalTupleCount );

        rm.updateBtreeHeader( btree, ( ( AbstractPage<K, V> ) rootPage ).getOffset() );

        rm.freePages( btree, btree.getRootPage().getRevision(), Arrays.asList( btree.getRootPage() ) );

        ( ( AbstractBTree<K, V> ) btree ).setRootPage( rootPage );

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

        PersistedNode<K, V> node = ( PersistedNode<K, V> ) BTreeFactory.createNode( btree, 0, numKeysInNode );
        lstNodes.add( node );
        int i = 0;
        int totalNodes = 0;

        for ( Page<K, V> page : children )
        {
            if ( i != 0 )
            {
                BTreeFactory.setKey( btree, node, i - 1, page.getLeftMostKey() );
            }

            BTreeFactory.setPage( btree, node, i, page );

            i++;
            totalNodes++;

            if ( ( totalNodes % numChildren ) == 0 )
            {
                i = 0;

                rm.writePage( btree, node, 1 );

                node = ( PersistedNode<K, V> ) BTreeFactory.createNode( btree, 0, numKeysInNode );
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

                rm.writePage( btree, lastNode, 1 );

                break;
            }
        }

        return attachNodes( lstNodes, btree );
    }
}
