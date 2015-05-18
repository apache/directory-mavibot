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
    /** The Btree configuration */
    private InMemoryBTreeConfiguration<K, V> btreeConfiguration;


    /**
     * Creates a new instance of InMemoryBTreeBuilder.
     *
     * @param name The BTree name
     * @param numKeysInNode The number of keys per node
     * @param keySerializer The key serializer
     * @param valueSerializer The value  serializer
     */
    public InMemoryBTreeBuilder( String name, int numKeysInNode, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer )
    {
        btreeConfiguration = new InMemoryBTreeConfiguration<K, V>();
        btreeConfiguration.setName( name );
        btreeConfiguration.setPageSize( numKeysInNode );
        btreeConfiguration.setKeySerializer( keySerializer );
        btreeConfiguration.setValueSerializer( valueSerializer );
    }


    /**
     * Creates a new instance of InMemoryBTreeBuilder.
     *
     * @param btreeConfiguration The Btree configuration
     */
    public InMemoryBTreeBuilder( InMemoryBTreeConfiguration<K, V> btreeConfiguration )
    {
        this.btreeConfiguration = btreeConfiguration;
    }


    @SuppressWarnings("unchecked")
    public BTree<K, V> build( Iterator<Tuple<K, V>> sortedTupleItr ) throws IOException
    {
        BTree<K, V> btree = BTreeFactory.createInMemoryBTree( btreeConfiguration );
        int pageSize = btree.getPageSize();
        int maxElements = ( pageSize + 1 ) * pageSize;

        // The stack used to store all the levels. No need to have more than 16 levels, 
        // it will allow the storage of 2^64 elements with pages containing 4 elements each.
        List<InMemoryNode<K, V>>[] pageStack = new ArrayList[15];

        for ( int i = 0; i < 15; i++ )
        {
            pageStack[i] = new ArrayList<InMemoryNode<K, V>>( maxElements );
        }

        // The number of added elements
        int nbAdded = 0;

        // The btree height
        int btreeHeight = 0;

        // An array containing the number of elements necessary to fulfill a layer :
        // pageSize * (pageSize + 1)
        List<Tuple<K, V>> tuples = new ArrayList<Tuple<K, V>>( maxElements );

        // A list of nodes that are going to be created
        List<InMemoryNode<K, V>> nodes = new ArrayList<InMemoryNode<K, V>>();

        // Now, loop on all the elements
        while ( sortedTupleItr.hasNext() )
        {
            nbAdded++;

            // Get the tuple to inject
            Tuple<K, V> tuple = sortedTupleItr.next();
            tuples.add( tuple );

            if ( tuples.size() == maxElements )
            {
                // We have enough elements to create pageSize leaves, and the upper node
                InMemoryNode<K, V> node = ( InMemoryNode<K, V> ) addLeaves( btree, tuples, maxElements );
                int level = 0;

                if ( node != null )
                {
                    while ( true )
                    {
                        pageStack[level].add( node );

                        // If the node list has enough elements to fulfill a parent node,
                        // then process 
                        if ( pageStack[level].size() > btree.getPageSize() )
                        {
                            node = createParentNode( btree, pageStack[level], btree.getPageSize() );
                            pageStack[level].clear();
                            level++;
                        }
                        else
                        {
                            break;
                        }
                    }

                    ( ( AbstractBTree<K, V> ) btree ).setRootPage( pageStack[level].get( 0 ) );

                    // Update the btree height
                    if ( btreeHeight < level )
                    {
                        btreeHeight = level;
                    }
                }

                tuples.clear();
            }
        }

        if ( tuples.size() > 0 )
        {
            // Let's deal with the remaining elements
            Page<K, V> page = addLeaves( btree, tuples, maxElements );

            if ( page instanceof InMemoryNode )
            {
                nodes.add( ( InMemoryNode<K, V> ) page );

                // If the node list has enough elements to fulfill a parent node,
                // then process 
                if ( nodes.size() == maxElements )
                {
                    Page<K, V> rootPage = createParentNode( btree, nodes, maxElements );

                    ( ( AbstractBTree<K, V> ) btree ).setRootPage( rootPage );
                }
            }
            else
            {
                InMemoryLeaf<K, V> leaf = ( InMemoryLeaf<K, V> ) page;

                // Its a leaf. That means we may have to balance the btree
                if ( pageStack[0].size() != 0 )
                {
                    // We have some leaves in level 0, which means we just have to add the new leaf
                    // there, after having check we don't have to borrow some elements from the last leaf
                    if ( leaf.getNbElems() < btree.getPageSize() / 2 )
                    {
                        // Not enough elements in the added leaf. Borrow some from the left.
                        // TODO
                    }
                    else
                    {
                        // Enough elements in the added leaf (at least N/2). We just have to update
                        // the parent's node.
                        // TODO
                    }
                }
            }
        }

        // Update the number of elements
        ( ( AbstractBTree<K, V> ) btree ).getBtreeHeader().setNbElems( nbAdded );

        return btree;
    }


    /**
     * Creates all the nodes using the provided node pages, and update the upper laye
     */
    private InMemoryNode<K, V> createParentNode( BTree<K, V> btree, List<InMemoryNode<K, V>> nodes, int maxElements )
    {
        // We have enough tuples to fulfill the upper node.
        // First, create the new node
        InMemoryNode<K, V> parentNode = ( InMemoryNode<K, V> ) BTreeFactory.createNode( btree, 0,
            btreeConfiguration.getPageSize() );

        int nodePos = 0;

        // Then iterate on the tuples, creating the needed pages
        for ( InMemoryNode<K, V> node : nodes )
        {
            if ( nodePos != 0 )
            {
                K key = node.getLeftMostKey();
                BTreeFactory.setKey( btree, parentNode, nodePos - 1, key );
            }

            PageHolder<K, V> pageHolder = new PageHolder<K, V>( btree, node );
            parentNode.setPageHolder( nodePos, pageHolder );
            nodePos++;
        }

        // And return the node
        return parentNode;
    }


    /**
     * Creates all the leaves using the provided tuples, and update the upper layer if needed
     */
    private Page<K, V> addLeaves( BTree<K, V> btree, List<Tuple<K, V>> tuples, int maxElements )
    {
        if ( tuples.size() == 0 )
        {
            // No element to inject in the BTree
            return null;
        }

        // The insertion position in the leaf
        int leafPos = 0;

        // Deal with special cases : 
        // First, everything will fit in a single page
        if ( tuples.size() < btree.getPageSize() )
        {
            // The leaf will be the rootPage
            // creates a first leaf
            InMemoryLeaf<K, V> leaf = ( InMemoryLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0,
                btreeConfiguration.getPageSize() );

            // Iterate on the tuples
            for ( Tuple<K, V> tuple : tuples )
            {
                injectTuple( btree, leaf, leafPos, tuple );
                leafPos++;
            }

            return leaf;
        }

        // Second, the data will fit into a 2 level tree
        if ( tuples.size() < maxElements )
        {
            // We will just create the necessary leaves and the upper node if needed
            // We have enough tuples to fulfill the uper node.
            // First, create the new node
            InMemoryNode<K, V> node = ( InMemoryNode<K, V> ) BTreeFactory.createNode( btree, 0,
                btreeConfiguration.getPageSize() );

            // creates a first leaf
            InMemoryLeaf<K, V> leaf = ( InMemoryLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0,
                btreeConfiguration.getPageSize() );

            int nodePos = 0;

            // Then iterate on the tuples, creating the needed pages
            for ( Tuple<K, V> tuple : tuples )
            {
                if ( leafPos == btree.getPageSize() )
                {
                    // The leaf is full, we need to attach it to its parent's node
                    // and to create a new leaf
                    BTreeFactory.setKey( btree, node, nodePos, tuple.getKey() );
                    PageHolder<K, V> pageHolder = new PageHolder<K, V>( btree, leaf );
                    node.setPageHolder( nodePos, pageHolder );
                    nodePos++;

                    // When done, we need to create a new leaf
                    leaf = ( InMemoryLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0,
                        btree.getPageSize() );

                    // and inject the tuple in the leaf
                    injectTuple( btree, leaf, 0, tuple );
                    leafPos = 1;
                }
                else
                {
                    // Inject the tuple in the leaf
                    injectTuple( btree, leaf, leafPos, tuple );
                    leafPos++;
                }
            }

            // Last, not least, deal with the last created leaf, which has to be injected in its parent's node
            if ( leafPos > 0 )
            {
                PageHolder<K, V> pageHolder = new PageHolder<K, V>( btree, leaf );
                node.setPageHolder( nodePos, pageHolder );
            }

            return node;
        }
        else
        {
            // We have enough tuples to fulfill the upper node.
            // First, create the new node
            InMemoryNode<K, V> node = ( InMemoryNode<K, V> ) BTreeFactory.createNode( btree, 0,
                btreeConfiguration.getPageSize() );

            // creates a first leaf
            InMemoryLeaf<K, V> leaf = ( InMemoryLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0,
                btreeConfiguration.getPageSize() );

            int nodePos = 0;

            // Then iterate on the tuples, creating the needed pages
            for ( Tuple<K, V> tuple : tuples )
            {
                if ( leafPos == btree.getPageSize() )
                {
                    // The leaf is full, we need to attach it to its parent's node
                    // and to create a new node
                    BTreeFactory.setKey( btree, node, nodePos, tuple.getKey() );
                    PageHolder<K, V> pageHolder = new PageHolder<K, V>( btree, leaf );
                    node.setPageHolder( nodePos, pageHolder );
                    nodePos++;

                    // When done, we need to create a new leaf
                    leaf = ( InMemoryLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0,
                        btree.getPageSize() );

                    // and inject the tuple in the leaf
                    injectTuple( btree, leaf, 0, tuple );
                    leafPos = 1;
                }
                else
                {
                    // Inject the tuple in the leaf
                    injectTuple( btree, leaf, leafPos, tuple );
                    leafPos++;
                }
            }

            // Last, not least, deal with the last created leaf, which has to be injected in its parent's node
            if ( leafPos > 0 )
            {
                PageHolder<K, V> pageHolder = new PageHolder<K, V>( btree, leaf );
                node.setPageHolder( nodePos, pageHolder );
            }

            // And return the node
            return node;
        }
    }


    private void injectTuple( BTree<K, V> btree, InMemoryLeaf<K, V> leaf, int leafPos, Tuple<K, V> tuple )
    {
        BTreeFactory.setKey( btree, leaf, leafPos, tuple.getKey() );
        ValueHolder<V> valueHolder = new InMemoryValueHolder<V>( btree, tuple.getValue() );
        BTreeFactory.setValue( btree, leaf, leafPos, valueHolder );
    }


    private int add( BTree<K, V> btree, Page<K, V>[] pageStack, int level, Page<K, V> page, Tuple<K, V> tuple )
    {
        if ( page == null )
        {
            // No existing page at this level, create a new one
            if ( level == 0 )
            {
                // creates a leaf
                InMemoryLeaf<K, V> leaf = ( InMemoryLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0,
                    btreeConfiguration.getPageSize() );

                // Store the new leaf in the stack
                pageStack[level] = leaf;

                // Inject the tuple in the leaf
                BTreeFactory.setKey( btree, leaf, 0, tuple.getKey() );
                ValueHolder<V> valueHolder = new InMemoryValueHolder<V>( btree, tuple.getValue() );
                BTreeFactory.setValue( btree, leaf, 0, valueHolder );
            }
            else
            {
                // Create a node
                InMemoryNode<K, V> node = ( InMemoryNode<K, V> ) BTreeFactory.createNode( btree, 0,
                    btreeConfiguration.getPageSize() );

                // Inject the tuple key in the node
                BTreeFactory.setKey( btree, node, 0, tuple.getKey() );
                PageHolder<K, V> pageHolder = new PageHolder<K, V>( btree, pageStack[level - 1] );
                node.setPageHolder( 0, pageHolder );
            }
        }
        else
        {
            // Check first if the current page is full
            if ( page.getNbElems() == btree.getPageSize() )
            {
                // Ok, it's full. We need to create a new page and to propagate the
                // added element to the upper level
                //TODO
            }
            else
            {
                // We just have to inject the tuple in the current page
                // be it a leaf or a node
                if ( page.isLeaf() )
                {
                    // It's a leaf
                    BTreeFactory.setKey( btree, page, page.getNbElems(), tuple.getKey() );
                    ValueHolder<V> valueHolder = new InMemoryValueHolder<V>( btree, tuple.getValue() );
                    BTreeFactory.setValue( btree, page, page.getNbElems(), valueHolder );
                }
                else
                {
                    // It's a node
                    BTreeFactory.setKey( btree, page, page.getNbElems(), tuple.getKey() );
                    PageHolder<K, V> pageHolder = new PageHolder<K, V>( btree, pageStack[level - 1] );
                    ( ( InMemoryNode<K, V> ) page ).setPageHolder( page.getNbElems(), pageHolder );
                }
            }
        }

        return level;
    }


    @SuppressWarnings("unchecked")
    private Page<K, V> attachNodes( List<Page<K, V>> children, BTree<K, V> btree ) throws IOException
    {
        if ( children.size() == 1 )
        {
            return children.get( 0 );
        }

        List<Page<K, V>> lstNodes = new ArrayList<Page<K, V>>();

        int numChildren = btreeConfiguration.getPageSize() + 1;

        InMemoryNode<K, V> node = ( InMemoryNode<K, V> ) BTreeFactory.createNode( btree, 0,
            btreeConfiguration.getPageSize() );
        lstNodes.add( node );
        int i = 0;
        int totalNodes = 0;

        for ( Page<K, V> p : children )
        {
            if ( i != 0 )
            {
                BTreeFactory.setKey( btree, node, i - 1, p.getLeftMostKey() );
            }

            node.setPageHolder( i, new PageHolder<K, V>( btree, p ) );

            i++;
            totalNodes++;

            if ( ( totalNodes % numChildren ) == 0 )
            {
                i = 0;
                node = ( InMemoryNode<K, V> ) BTreeFactory.createNode( btree, 0, btreeConfiguration.getPageSize() );
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
