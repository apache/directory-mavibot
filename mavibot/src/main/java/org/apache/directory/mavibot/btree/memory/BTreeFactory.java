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
package org.apache.directory.mavibot.btree.memory;


import java.io.IOException;
import java.util.LinkedList;

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * This class construct a BTree from a serialized version of a BTree. We need it
 * to avoid exposing all the methods of the BTree class.<br>
 * 
 * All its methods are static.
 *  
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BTreeFactory
{
    /**
     * Create a new BTree.
     * 
     * @return The created BTree
     */
    public static <K, V> BTree<K, V> createBTree()
    {
        BTree<K, V> btree = new BTree<K, V>();

        return btree;
    }


    /**
     * Create a new Node for the give BTree.
     * 
     * @param btree The BTree which will contain this node
     * @param revision The Node's revision
     * @param nbElems The number or elements in this node
     * @return A Node instance
     */
    public static <K, V> Node<K, V> createNode( BTree<K, V> btree, long revision, int nbElems )
    {
        Node<K, V> node = new Node<K, V>( btree, revision, nbElems );

        return node;
    }


    /**
     * Create a new Leaf for the give BTree.
     * 
     * @param btree The BTree which will contain this leaf
     * @param revision The Leaf's revision
     * @param nbElems The number or elements in this leaf
     * @return A Leaf instance
     */
    public static <K, V> Leaf<K, V> createLeaf( BTree<K, V> btree, long revision, int nbElems )
    {
        Leaf<K, V> leaf = new Leaf<K, V>( btree, revision, nbElems );

        return leaf;
    }


    /**
     * Set the new root page for this tree. Used for debug purpose only. The revision
     * will always be 0;
     * 
     * @param root the new root page.
     */
    public static <K, V> void setRoot( BTree<K, V> btree, Page<K, V> root )
    {
        btree.setRoot( root );
    }


    /**
     * Return the BTree root page
     * 
     * @param btree The Btree we want to root page from
     * @return The root page
     */
    public static <K, V> Page<K, V> getRoot( BTree<K, V> btree )
    {
        return btree.rootPage;
    }


    /**
     * @param nbElems the nbElems to set
     */
    public static <K, V> void setNbElems( BTree<K, V> btree, long nbElems )
    {
        btree.setNbElems( nbElems );
    }


    /**
     * @param revision the revision to set
     */
    public static <K, V> void setRevision( BTree<K, V> btree, long revision )
    {
        btree.setRevision( revision );
    }


    /**
     * @param rootPageOffset the rootPageOffset to set
     */
    public static <K, V> void setRootPageOffset( BTree<K, V> btree, long rootPageOffset )
    {
        btree.setRootPageOffset( rootPageOffset );
    }


    /**
     * @param nextBTreeOffset the nextBTreeOffset to set
     */
    public static <K, V> void setNextBTreeOffset( BTree<K, V> btree, long nextBTreeOffset )
    {
        btree.setNextBTreeOffset( nextBTreeOffset );
    }


    /**
     * @param name the name to set
     */
    public static <K, V> void setName( BTree<K, V> btree, String name )
    {
        btree.setName( name );
    }


    /**
     * Sets the KeySerializer into the BTree
     *  
     * @param btree The BTree to update
     * @param keySerializerFqcn the Key serializer FQCN to set
     * @throws ClassNotFoundException
     * @throws InstantiationException 
     * @throws IllegalAccessException
     */
    public static <K, V> void setKeySerializer( BTree<K, V> btree, String keySerializerFqcn )
        throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        Class<?> keySerializer = Class.forName( keySerializerFqcn );
        @SuppressWarnings("unchecked")
        ElementSerializer<K> instance = ( ElementSerializer<K> ) keySerializer.newInstance();
        btree.setKeySerializer( instance );
    }


    /**
     * Sets the ValueSerializer into the BTree
     *  
     * @param btree The BTree to update
     * @param valueSerializerFqcn the Value serializer FQCN to set
     * @throws ClassNotFoundException
     * @throws InstantiationException 
     * @throws IllegalAccessException
     */
    public static <K, V> void setValueSerializer( BTree<K, V> btree, String valueSerializerFqcn )
        throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        Class<?> valueSerializer = Class.forName( valueSerializerFqcn );
        @SuppressWarnings("unchecked")
        ElementSerializer<V> instance = ( ElementSerializer<V> ) valueSerializer.newInstance();
        btree.setValueSerializer( instance );
    }


    /**
     * Set the maximum number of elements we can store in a page.
     * 
     * @param pageSize The requested page size
     */
    public static <K, V> void setPageSize( BTree<K, V> btree, int pageSize )
    {
        btree.setPageSize( pageSize );
    }


    /**
     * Set the key at a give position
     * @param pos The position in the keys array
     * @param key the key to inject
     */
    public static <K, V> void setKey( Page<K, V> page, int pos, K key )
    {
        ( ( AbstractPage<K, V> ) page ).setKey( pos, key );
    }


    /**
     * Set the value at a give position
     * @param pos The position in the values array
     * @param value the value to inject
     */
    public static <K, V> void setValue( Leaf<K, V> page, int pos, ElementHolder<V, K, V> value )
    {
        page.setValue( pos, value );
    }


    /**
     * Set the value at a give position
     * @param pos The position in the values array
     * @param value the value to inject
     */
    public static <K, V> void setValue( Node<K, V> page, int pos, ElementHolder<Page<K, V>, K, V> value )
    {
        page.setValue( pos, value );
    }


    /**
     * Includes the intermediate nodes in the path up to and including the right most leaf of the tree
     * 
     * @param btree the btree
     * @return a LinkedList of all the nodes and the final leaf
     * @throws IOException
     */
    public static <K, V> LinkedList<ParentPos<K, V>> getPathToRightMostLeaf( BTree<K, V> btree ) throws IOException
    {
        LinkedList<ParentPos<K, V>> stack = new LinkedList<ParentPos<K, V>>();

        ParentPos<K, V> last = new ParentPos<K, V>( btree.rootPage, btree.rootPage.getNbElems() );
        stack.push( last );

        if ( btree.rootPage instanceof Leaf )
        {
            InternalUtil.setLastDupsContainer( last, btree );
        }
        else
        {
            Node<K, V> node = ( Node<K, V> ) btree.rootPage;

            while ( true )
            {
                Page<K, V> p = node.children[node.getNbElems()].getValue( btree );

                last = new ParentPos<K, V>( p, p.getNbElems() );
                stack.push( last );

                if ( p instanceof Leaf )
                {
                    InternalUtil.setLastDupsContainer( last, btree );
                    break;
                }
            }
        }

        return stack;
    }
}
