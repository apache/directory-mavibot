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
package org.apache.directory.mavibot.btree;


import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * The B+Tree Configuration. This class can be used to store all the configurable
 * parameters used by the B-tree class
 *
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PersistedBTreeConfiguration<K, V>
{
    /** Number of entries in each Page. */
    private int pageSize = BTree.DEFAULT_PAGE_SIZE;

    /** The size of the buffer used to write data in disk */
    private int writeBufferSize = BTree.DEFAULT_WRITE_BUFFER_SIZE;

    /** The Key and Value serializer used for this tree. If none is provided,
     * the B-tree will deduce the serializer to use from the generic type, and
     * use the default Java serialization  */
    private ElementSerializer<K> keySerializer;
    private ElementSerializer<V> valueSerializer;

    /** The B-tree name */
    private String name;

    /** The path where the B-tree file will be stored. Default to the local
     * temporary directory.
     */
    private String filePath;

    /**
     * The maximum delay to wait before a revision is considered as unused.
     * This delay is necessary so that a read that does not ends does not
     * hold a revision in memory forever.
     * The default value is 10000 (10 seconds). If the value is 0 or below,
     * the delay is considered as infinite
     */
    private long readTimeOut = PersistedBTree.DEFAULT_READ_TIMEOUT;

    /** Flag to enable duplicate key support */
    private boolean allowDuplicates;

    /** The B-tree type */
    private BTreeTypeEnum btreeType = BTreeTypeEnum.PERSISTED;

    /** The cache size, if it's <= 0, we don't have cache */
    private int cacheSize;

    /** The inherited B-tree if we create a sub B-tree */
    private BTree<?, V> parentBTree;


    /**
     * @return the pageSize
     */
    public int getPageSize()
    {
        return pageSize;
    }


    /**
     * @param pageSize the pageSize to set
     */
    public void setPageSize( int pageSize )
    {
        this.pageSize = pageSize;
    }


    /**
     * @return the key serializer
     */
    public ElementSerializer<K> getKeySerializer()
    {
        return keySerializer;
    }


    /**
     * @return the value serializer
     */
    public ElementSerializer<V> getValueSerializer()
    {
        return valueSerializer;
    }


    /**
     * @param keySerializer the key serializer to set
     * @param valueSerializer the value serializer to set
     */
    public void setSerializers( ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer )
    {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }


    /**
     * @param serializer the key serializer to set
     */
    public void setKeySerializer( ElementSerializer<K> keySerializer )
    {
        this.keySerializer = keySerializer;
    }


    /**
     * @param serializer the key serializer to set
     */
    public void setValueSerializer( ElementSerializer<V> valueSerializer )
    {
        this.valueSerializer = valueSerializer;
    }


    /**
     * @return the readTimeOut
     */
    public long getReadTimeOut()
    {
        return readTimeOut;
    }


    /**
     * @param readTimeOut the readTimeOut to set
     */
    public void setReadTimeOut( long readTimeOut )
    {
        this.readTimeOut = readTimeOut;
    }


    /**
     * @return the filePath
     */
    public String getFilePath()
    {
        return filePath;
    }


    /**
     * @param filePath the filePath to set
     */
    public void setFilePath( String filePath )
    {
        this.filePath = filePath;
    }


    /**
     * @return the writeBufferSize
     */
    public int getWriteBufferSize()
    {
        return writeBufferSize;
    }


    /**
     * @param writeBufferSize the writeBufferSize to set
     */
    public void setWriteBufferSize( int writeBufferSize )
    {
        this.writeBufferSize = writeBufferSize;
    }


    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }


    /**
     * @param name the name to set
     */
    public void setName( String name )
    {
        this.name = name.trim();
    }


    /**
     * @return true if duplicate key support is enabled
     */
    public boolean isAllowDuplicates()
    {
        return allowDuplicates;
    }


    /**
     * enable duplicate key support
     *
     * @param allowDuplicates
     * @throws IllegalStateException if the B-tree was already initialized or when tried to turn off duplicate suport on
     * an existing B-tree containing duplicate keys
     */
    public void setAllowDuplicates( boolean allowDuplicates )
    {
        this.allowDuplicates = allowDuplicates;
    }


    /**
     * @return the cacheSize
     */
    public int getCacheSize()
    {
        return cacheSize;
    }


    /**
     * @param cacheSize the cacheSize to set.
     */
    public void setCacheSize( int cacheSize )
    {
        this.cacheSize = cacheSize;
    }


    /**
     * @return the cache
     */
    public BTree<?, V> getParentBTree()
    {
        return parentBTree;
    }


    /**
     * @param cache the cache to set.
     */
    public void setParentBTree( BTree<?, V> parentBTree )
    {
        this.parentBTree = parentBTree;
    }


    /**
     * @return The BtreeType for this Btree
     */
    public BTreeTypeEnum getBtreeType()
    {
        return btreeType;
    }


    /**
     * @param btreeType The BtreeType
     */
    public void setBtreeType( BTreeTypeEnum btreeType )
    {
        this.btreeType = btreeType;
    }
}
