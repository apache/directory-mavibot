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
 * parameters used by the BTree class
 * 
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryBTreeConfiguration<K, V>
{
    /** Number of entries in each Page. */
    private int pageSize = InMemoryBTree.DEFAULT_PAGE_SIZE;

    /** The size of the buffer used to write data in disk */
    private int writeBufferSize = InMemoryBTree.DEFAULT_WRITE_BUFFER_SIZE;

    /** The Key and Value serializer used for this tree. If none is provided, 
     * the BTree will deduce the serializer to use from the generic type, and
     * use the default Java serialization  */
    private ElementSerializer<K> keySerializer;
    private ElementSerializer<V> valueSerializer;

    /** The BTree name */
    private String name;

    /** The path where the BTree file will be stored. Default to the local 
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
    private long readTimeOut = InMemoryBTree.DEFAULT_READ_TIMEOUT;

    /** The maximal size of the journal. When this size is reached, the tree is 
     * flushed on disk.
     * The default size is 10 Mb
     */
    private long journalSize = 10 * 1024 * 1024L;

    /**
     * The journal's name. Default to "mavibot.log".
     */
    private String journalName = InMemoryBTree.DEFAULT_JOURNAL;

    /** 
     * The delay between two checkpoints. When we reach the maximum delay,
     * the BTree is flushed on disk, but only if we have had some modifications.
     * The default value is 60 seconds.
     */
    private long checkPointDelay = 60 * 1000L;

    /** Flag to enable duplicate key support */
    private boolean allowDuplicates;

    /** the type of BTree */
    private BTreeTypeEnum type;


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
     * @return the journalSize
     */
    public long getJournalSize()
    {
        return journalSize;
    }


    /**
     * @param journalSize the journalSize to set
     */
    public void setJournalSize( long journalSize )
    {
        this.journalSize = journalSize;
    }


    /**
     * @return the checkPointDelay
     */
    public long getCheckPointDelay()
    {
        return checkPointDelay;
    }


    /**
     * @param checkPointDelay the checkPointDelay to set
     */
    public void setCheckPointDelay( long checkPointDelay )
    {
        this.checkPointDelay = checkPointDelay;
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
     * @return the journal name
     */
    public String getJournalName()
    {
        return journalName;
    }


    /**
     * @param journalName the journal name to set
     */
    public void setJournalName( String journalName )
    {
        this.journalName = journalName;
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
     * @throws IllegalStateException if the btree was already initialized or when tried to turn off duplicate suport on
     *                               an existing btree containing duplicate keys
     */
    public void setAllowDuplicates( boolean allowDuplicates )
    {
        this.allowDuplicates = allowDuplicates;
    }


    /**
     * @return the type of BTree
     */
    public BTreeTypeEnum getType()
    {
        return type;
    }


    /**
     * Sets the type of the BTree
     * 
     * @param type the type of the tree
     */
    public void setType( BTreeTypeEnum type )
    {
        this.type = type;
    }
}
