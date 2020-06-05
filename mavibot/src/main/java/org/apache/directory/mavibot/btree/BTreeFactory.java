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


import java.util.LinkedList;

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * This class construct a b-tree from a serialized version of a b-tree. We need it
 * to avoid exposing all the methods of the b-tree class.<br>
 *
 * All its methods are static.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 *
 * @param <K> The b-tree key type
 * @param <V> The b-tree value type
 */
public class BTreeFactory<K, V>
{
    /**
     * Private constructor to forbid the creation of an instance
     */
    private BTreeFactory()
    {
        // Does nothing
    }
    
    
    //--------------------------------------------------------------------------------------------
    // Create b-tree
    //--------------------------------------------------------------------------------------------
    /**
     * Creates a new b-tree, with no initialization.
     *
     * @return a new b-tree instance
     */
    public static <K, V> BTree<K, V> createBTree()
    {
        return new BTree<>();
    }


    /**
     * Creates a new typed b-tree, with no initialization. The type is one
     * of {@link BTreeTypeEnum}
     *
     * @return a new b-tree instance
     */
    public static <K, V> BTree<K, V> createBTree( BTreeTypeEnum type )
    {
        BTree<K, V> btree = new BTree<>( );
        btree.setBtreeInfo( new BTreeInfo<>() );
        btree.setType( type );

        return btree;
    }


    /**
     * Creates a new b-tree using the BTreeConfiguration to initialize the
     * b-tree
     *
     * @param configuration The configuration to use
     * @return a new b-tree instance
     */
    public static <K, V> BTree<K, V> createBTree( Transaction transaction, BTreeConfiguration<K, V> configuration )
    {
        return new BTree<>( transaction, configuration );
    }


    /**
     * Creates a new b-tree using the parameters to initialize the b-tree
     *
     * @param name The b-tree's name
     * @param keySerializer Key serializer
     * @param valueSerializer Value serializer
     * @return a new b-tree instance
     */
    public static <K, V> BTree<K, V> createBTree( Transaction transaction, String name, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer )
    {
        BTreeConfiguration<K, V> configuration = new BTreeConfiguration<>();

        configuration.setName( name );
        configuration.setKeySerializer( keySerializer );
        configuration.setValueSerializer( valueSerializer );
        configuration.setPageNbElem( BTree.DEFAULT_PAGE_NBELEM );

        return new BTree<>( transaction, configuration );
    }


    /**
     * Creates a new b-tree using the parameters to initialize the b-tree
     *
     * @param name The b-tree's name
     * @param keySerializer Key serializer
     * @param valueSerializer Value serializer
     * @param allowDuplicates Tells if the b-tree allows multiple value for a given key
     * @param cacheSize The size to be used for this b-tree cache
     * @return a new b-tree instance
     */
    public static <K, V> BTree<K, V> createBTree( Transaction transaction, String name, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer, int cacheSize )
    {
        BTreeConfiguration<K, V> configuration = new BTreeConfiguration<>();

        configuration.setName( name );
        configuration.setKeySerializer( keySerializer );
        configuration.setValueSerializer( valueSerializer );
        configuration.setPageNbElem( BTree.DEFAULT_PAGE_NBELEM );

        return new BTree<>( transaction, configuration );
    }


    /**
     * Creates a new b-tree using the parameters to initialize the b-tree
     *
     * @param name The b-tree's name
     * @param pageNbElem Number of elements per page
     * @param keySerializer Key serializer
     * @param valueSerializer Value serializer
     * @return a new b-tree instance
     */
    public static <K, V> BTree<K, V> createBTree( Transaction transaction, String name, int pageNbElem, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer )
    {
        BTreeConfiguration<K, V> configuration = new BTreeConfiguration<>();

        configuration.setName( name );
        configuration.setKeySerializer( keySerializer );
        configuration.setValueSerializer( valueSerializer );
        configuration.setPageNbElem( pageNbElem );

        return new BTree<>( transaction, configuration );
    }


    /**
     * Creates a new b-tree using the parameters to initialize the b-tree
     *
     * @param name The b-tree's name
     * @param pageNbElem Number of elements per page
     * @param keySerializer Key serializer
     * @param valueSerializer Value serializer
     * @param cacheSize The size to be used for this b-tree cache
     * @return a new b-tree instance
     */
    public static <K, V> BTree<K, V> createBTree( Transaction transaction, String name, int pageNbElem, 
        ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer, int cacheSize )
    {
        BTreeConfiguration<K, V> configuration = new BTreeConfiguration<>();

        configuration.setName( name );
        configuration.setKeySerializer( keySerializer );
        configuration.setValueSerializer( valueSerializer );
        configuration.setPageNbElem( pageNbElem );

        return new BTree<>( transaction, configuration );
    }


    /**
     * Sets the btreeHeader offset for a BTree
     *
     * @param btree The btree to update
     * @param btreeHeaderOffset The offset
     */
    public static <K, V> void setBtreeHeaderOffset( BTree<K, V> btree, long btreeHeaderOffset )
    {
        btree.setBtreeHeaderOffset( btreeHeaderOffset );
    }


    //--------------------------------------------------------------------------------------------
    // Create Pages
    //--------------------------------------------------------------------------------------------
    /**
     * Create a new Leaf for the given b-tree.
     *
     * @param btree The b-tree which will contain this leaf
     * @param revision The Leaf's revision
     * @param nbElems The number or elements in this leaf
     *
     * @return A Leaf instance
     */
    /* no qualifier*/static <K, V> Page<K, V> createLeaf( BTreeInfo<K, V> btreeInfo, long revision, int nbElems )
    {
        return new Leaf<>( btreeInfo, revision, nbElems );
    }


    /**
     * Create a new Node for the given b-tree.
     *
     * @param btree The b-tree which will contain this node
     * @param revision The Node's revision
     * @param nbElems The number or elements in this node
     * @return A Node instance
     */
    /* no qualifier*/static <K, V> Page<K, V> createNode( BTreeInfo<K, V> btreeInfo, long revision, int nbElems )
    {
        //System.out.println( "Creating a node with nbElems : " + nbElems );
        return new Node<>( btreeInfo, revision, nbElems );
    }


    //--------------------------------------------------------------------------------------------
    // Update pages
    //--------------------------------------------------------------------------------------------
    /**
     * Set the key at a give position
     *
     * @param btree The b-tree to update
     * @param page The page to update
     * @param pos The position in the keys array
     * @param key The key to inject
     */
    /* no qualifier*/static <K, V> void setKey( BTreeInfo<K, V> btreeInfo, Page<K, V> page, int pos, K key )
    {
        KeyHolder<K> keyHolder = new KeyHolder<>( btreeInfo.getKeySerializer(), key );

        ( ( AbstractPage<K, V> ) page ).setKey( pos, keyHolder );
    }


    /**
     * Set the value at a give position
     *
     * @param btree The b-tree to update
     * @param page The page to update
     * @param pos The position in the values array
     * @param value the value to inject
     */
    /* no qualifier*/static <K, V> void setValue( Page<K, V> page, int pos, ValueHolder<V> value )
    {
        ( ( Leaf<K, V> ) page ).setValue( pos, value );
    }


    /**
     * Set the page at a give position
     *
     * @param btree The b-tree to update
     * @param page The page to update
     * @param pos The position in the values array
     * @param child the child page to inject
     */
    /* no qualifier*/static <K, V> void setPage( BTreeInfo<K, V> btreeInfo, Page<K, V> page, int pos, Page<K, V> child )
    {
        ( ( Node<K, V> ) page ).setValue( pos, child.getOffset() );
    }


    //--------------------------------------------------------------------------------------------
    // Update b-tree
    //--------------------------------------------------------------------------------------------
    /**
     * Sets the KeySerializer into the b-tree
     *
     * @param btree The b-tree to update
     * @param keySerializerFqcn the Key serializer FQCN to set
     * @throws ClassNotFoundException If the key serializer class cannot be found
     * @throws InstantiationException If the key serializer class cannot be instanciated
     * @throws IllegalAccessException If the key serializer class cannot be accessed
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalArgumentException
     */
    /* no qualifier*/static <K, V> void setKeySerializer( BTree<K, V> btree, String keySerializerFqcn )
        throws ClassNotFoundException, IllegalAccessException, InstantiationException, IllegalArgumentException,
        SecurityException, NoSuchFieldException
    {
        Class<?> keySerializer = Class.forName( keySerializerFqcn );
        @SuppressWarnings("unchecked")
        ElementSerializer<K> instance = null;
        try
        {
            instance = ( ElementSerializer<K> ) keySerializer.getDeclaredField( "INSTANCE" ).get( null );
        }
        catch ( NoSuchFieldException e )
        {
            // ignore
        }

        if ( instance == null )
        {
            instance = ( ElementSerializer<K> ) keySerializer.newInstance();
        }

        btree.setKeySerializer( instance );
    }


    /**
     * Sets the ValueSerializer into the b-tree
     *
     * @param btree The b-tree to update
     * @param valueSerializerFqcn the Value serializer FQCN to set
     * @throws ClassNotFoundException If the value serializer class cannot be found
     * @throws InstantiationException If the value serializer class cannot be instanciated
     * @throws IllegalAccessException If the value serializer class cannot be accessed
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalArgumentException
     */
    /* no qualifier*/static <K, V> void setValueSerializer( BTree<K, V> btree, String valueSerializerFqcn )
        throws ClassNotFoundException, IllegalAccessException, InstantiationException, IllegalArgumentException,
        SecurityException, NoSuchFieldException
    {
        Class<?> valueSerializer = Class.forName( valueSerializerFqcn );
        @SuppressWarnings("unchecked")
        ElementSerializer<V> instance = null;
        try
        {
            instance = ( ElementSerializer<V> ) valueSerializer.getDeclaredField( "INSTANCE" ).get( null );
        }
        catch ( NoSuchFieldException e )
        {
            // ignore
        }

        if ( instance == null )
        {
            instance = ( ElementSerializer<V> ) valueSerializer.newInstance();
        }

        btree.setValueSerializer( instance );
    }


    /**
     * Set the new root page for this tree. Used for debug purpose only. The revision
     * will always be 0;
     *
     * @param btree The b-tree to update
     * @param root the new root page.
     */
    /* no qualifier*/static <K, V> void setRootPage( BTree<K, V> btree, Page<K, V> root )
    {
        ( ( BTree<K, V> ) btree ).setRootPage( root );
    }


    /**
     * Return the b-tree root page
     *
     * @param btree The b-tree we want to root page from
     * @return The root page
     */
    /* no qualifier */static <K, V> Page<K, V> getRootPage( BTree<K, V> btree )
    {
        return (( BTree<K, V> ) btree ).getRootPage();
    }


    /**
     * Update the b-tree number of elements
     *
     * @param btree The b-tree to update
     * @param nbElems the nbElems to set
     */
    /* no qualifier */static <K, V> void setNbElems( BTree<K, V> btree, long nbElems )
    {
        ( ( BTree<K, V> ) btree ).setNbElems( nbElems );
    }


    /**
     * Update the b-tree revision
     *
     * @param btree The b-tree to update
     * @param revision the revision to set
     */
    /* no qualifier*/static <K, V> void setRevision( BTree<K, V> btree, long revision )
    {
        ( ( BTree<K, V> ) btree ).setRevision( revision );
    }


    /**
     * Set the b-tree name
     *
     * @param btree The b-tree to update
     * @param name the name to set
     */
    /* no qualifier */static <K, V> void setName( BTree<K, V> btree, String name )
    {
        btree.setName( name );
    }


    /**
     * Set the maximum number of elements we can store in a page.
     *
     * @param btree The b-tree to update
     * @param pageSize The requested page size
     */
    /* no qualifier */static <K, V> void setPageNbElem( BTree<K, V> btree, int pageNbElem )
    {
        btree.setPageNbElem( pageNbElem );
    }


    //--------------------------------------------------------------------------------------------
    // Utility method
    //--------------------------------------------------------------------------------------------


    /**
     * Set the RecordManager
     *
     * @param btree The b-tree to update
     * @param recordManager The injected RecordManager
     */
    /* no qualifier*/static <K, V> void setRecordManager( BTree<K, V> btree, RecordManager recordManager )
    {
        if ( btree instanceof BTree )
        {
            ( ( BTree<K, V> ) btree ).setRecordManager( recordManager );
        }
        else
        {
            throw new IllegalArgumentException( "The b-tree must be a PersistedBTree" );
        }
    }


    /**
     * Set the key at a give position
     *
     * @param btree The b-tree to update
     * @param page The page to update
     * @param pos The position of this key in the page
     * @param buffer The byte[] containing the serialized key
     */
    /* no qualifier*/static <K, V> void setKey( BTreeInfo<K, V> btreeInfo, Page<K, V> page, int pos, byte[] buffer )
    {
        KeyHolder<K> keyHolder = new KeyHolder<>( btreeInfo.getKeySerializer(), buffer );
        ( ( AbstractPage<K, V> ) page ).setKey( pos, keyHolder );
    }
}
