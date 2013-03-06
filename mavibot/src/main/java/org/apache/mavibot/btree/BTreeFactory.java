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


import org.apache.mavibot.btree.serializer.ElementSerializer;


/**
 * This cless construct a BTree from a serialized version of a BTree. We need it
 * to avoid exposing all the methods of the BTree class.<br>
 * 
 * All its methods are static.
 *  
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class BTreeFactory
{
    /**
     * Create a new BTree.
     * 
     * @return The created BTree
     */
    public static BTree createBTree()
    {
        BTree btree = new BTree();

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
    public static Page createNode( BTree btree, long revision, int nbElems )
    {
        Page node = new Node( btree, revision, nbElems );

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
    public static Page createLeaf( BTree btree, long revision, int nbElems )
    {
        Page leaf = new Leaf( btree, revision, nbElems );

        return leaf;
    }


    /**
     * Set the new root page for this tree. Used for debug purpose only. The revision
     * will always be 0;
     * 
     * @param root the new root page.
     */
    public static void setRoot( BTree<?, ?> btree, Page root )
    {
        btree.setRoot( root );
    }


    /**
     * @param nbElems the nbElems to set
     */
    public static void setNbElems( BTree<?, ?> btree, long nbElems )
    {
        btree.setNbElems( nbElems );
    }


    /**
     * @param revision the revision to set
     */
    public static void setRevision( BTree<?, ?> btree, long revision )
    {
        btree.setRevision( revision );
    }


    /**
     * @param name the name to set
     */
    public static void setName( BTree<?, ?> btree, String name )
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
    public static void setKeySerializer( BTree<?, ?> btree, String keySerializerFqcn )
        throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        Class<?> keySerializer = Class.forName( keySerializerFqcn );
        btree.setKeySerializer( ( ElementSerializer ) keySerializer.newInstance() );
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
    public static void setValueSerializer( BTree<?, ?> btree, String valueSerializerFqcn )
        throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        Class<?> valueSerializer = Class.forName( valueSerializerFqcn );
        btree.setValueSerializer( ( ElementSerializer ) valueSerializer.newInstance() );
    }


    /**
     * Set the maximum number of elements we can store in a page.
     * 
     * @param pageSize The requested page size
     */
    public static void setPageSize( BTree<?, ?> btree, int pageSize )
    {
        btree.setPageSize( pageSize );
    }
}
