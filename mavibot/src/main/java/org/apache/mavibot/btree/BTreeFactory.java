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
import java.util.LinkedList;

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
    public static Node createNode( BTree btree, long revision, int nbElems )
    {
        Node node = new Node( btree, revision, nbElems );

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
    public static Leaf createLeaf( BTree btree, long revision, int nbElems )
    {
        Leaf leaf = new Leaf( btree, revision, nbElems );

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
     * Return the BTree root page
     * 
     * @param btree The Btree we want to root page from
     * @return The root page
     */
    public static Page getRoot( BTree<?, ?> btree )
    {
        return btree.rootPage;
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
     * @param rootPageOffset the rootPageOffset to set
     */
    public static void setRootPageOffset( BTree<?, ?> btree, long rootPageOffset )
    {
        btree.setRootPageOffset( rootPageOffset );
    }


    /**
     * @param nextBTreeOffset the nextBTreeOffset to set
     */
    public static void setNextBTreeOffset( BTree<?, ?> btree, long nextBTreeOffset )
    {
        btree.setNextBTreeOffset( nextBTreeOffset );
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
        ElementSerializer instance = ( ElementSerializer ) keySerializer.newInstance();
        btree.setKeySerializer( instance );

        btree.setComparator( instance.getComparator() );
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


    /**
     * Set the RecordManager
     * 
     * @param recordManager The injected RecordManager
     */
    public static void setRecordManager( BTree<?, ?> btree, RecordManager recordManager )
    {
        btree.setRecordManager( recordManager );
    }


    /**
     * Set the key at a give position
     * @param pos The position in the keys array
     * @param key the key to inject
     */
    public static void setKey( Page page, int pos, Object key )
    {
        ( ( AbstractPage ) page ).setKey( pos, key );
    }


    /**
     * Set the value at a give position
     * @param pos The position in the values array
     * @param value the value to inject
     */
    public static void setValue( Leaf page, int pos, ElementHolder value )
    {
        page.setValue( pos, value );
    }


    /**
     * Set the value at a give position
     * @param pos The position in the values array
     * @param value the value to inject
     */
    public static void setValue( Node page, int pos, ElementHolder value )
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
    public static LinkedList getPathToRightMostLeaf( BTree btree ) throws IOException
    {
        LinkedList<ParentPos> stack = new LinkedList<ParentPos>();
        
        ParentPos last = new ParentPos( btree.rootPage, btree.rootPage.getNbElems() );
        stack.push( last );
        
        
        if( btree.rootPage instanceof Leaf )
        {
            InternalUtil.setLastDupsContainer( last, btree );
        }
        else
        {
            Node node = ( Node ) btree.rootPage;
            
            while( true )
            {
                Page p = ( Page ) node.children[node.getNbElems()].getValue( btree );
                
                last = new ParentPos( p, p.getNbElems() );
                stack.push( last );
                
                if( p instanceof Leaf )
                {
                    InternalUtil.setLastDupsContainer( last, btree );
                    break;
                }
            }
        }
        
        return stack;
    }
}
