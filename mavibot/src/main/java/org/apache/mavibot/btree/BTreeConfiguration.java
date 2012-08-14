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


import java.util.Comparator;

import org.apache.mavibot.btree.serializer.Serializer;


/**
 * The B+Tree Configuration. This class can be used to store all the configurable
 * parameters used by the BTree class
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 * 
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 */
public class BTreeConfiguration<K, V>
{
    /** Number of entries in each Page. */
    private int pageSize = BTree.DEFAULT_PAGE_SIZE;

    /** Comparator used to order entries. */
    private Comparator<K> comparator;

    /** The Key and Value serializer used for this tree. If none is provided, 
     * the BTree will deduce the serializer to use from the generic type, and
     * use the default Java serialization  */
    private Serializer<K, V> serializer;

    /** The path where the BTree file will be stored. Default to the local 
     * temporary directory.
     */
    private String filePath;

    /**
     * The BTree file's name. Default to "mavibot".
     */
    private String filePrefix = "mavibot";

    /**
     * The BTree file's suffix. Default to "data".
     */
    private String fileSuffix = "data";

    /** 
     * The maximum delay to wait before a revision is considered as unused.
     * This delay is necessary so that a read that does not ends does not 
     * hold a revision in memory forever.
     * The default value is 10000 (10 seconds). If the value is 0 or below,
     * the delay is considered as infinite
     */
    private long readTimeOut = BTree.DEFAULT_READ_TIMEOUT;

    /** The maximal size of the journal. When this size is reached, the tree is 
     * flushed on disk.
     * The default size is 10 Mb
     */
    private long journalSize = 10 * 1024 * 1024L;

    /** The path where the journal will be stored. Default to the local 
     * temporary directory.
     */
    private String journalPath;

    /**
     * The journal's name. Default to "mavibot".
     */
    private String journalPrefix = "mavibot";

    /**
     * The journal's suffix. Default to "log".
     */
    private String journalSuffix = "log";

    /** The delay between two checkpoints. When we reach the maximum delay,
     * the BTree is flushed on disk, but only if we have had some modifications.
     * The default value is 60 seconds.
     */
    private long checkPointDelay = 60 * 1000L;


    /**
     * @return the comparator
     */
    public Comparator<K> getComparator()
    {
        return comparator;
    }


    /**
     * @param comparator the comparator to set
     */
    public void setComparator( Comparator<K> comparator )
    {
        this.comparator = comparator;
    }


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
     * @return the serializer
     */
    public Serializer<K, V> getSerializer()
    {
        return serializer;
    }


    /**
     * @param serializer the serializer to set
     */
    public void setSerializer( Serializer<K, V> serializer )
    {
        this.serializer = serializer;
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
     * @return the filePrefix
     */
    public String getFilePrefix()
    {
        return filePrefix;
    }


    /**
     * @param filePrefix the filePrefix to set
     */
    public void setFilePrefix( String filePrefix )
    {
        this.filePrefix = filePrefix;
    }


    /**
     * @return the fileSuffix
     */
    public String getFileSuffix()
    {
        return fileSuffix;
    }


    /**
     * @param fileSuffix the fileSuffix to set
     */
    public void setFileSuffix( String fileSuffix )
    {
        this.fileSuffix = fileSuffix;
    }


    /**
     * @return the journalPath
     */
    public String getJournalPath()
    {
        return journalPath;
    }


    /**
     * @param journalPath the journalPath to set
     */
    public void setJournalPath( String journalPath )
    {
        this.journalPath = journalPath;
    }


    /**
     * @return the journalPrefix
     */
    public String getJournalPrefix()
    {
        return journalPrefix;
    }


    /**
     * @param journalPrefix the journalPrefix to set
     */
    public void setJournalPrefix( String journalPrefix )
    {
        this.journalPrefix = journalPrefix;
    }


    /**
     * @return the journalSuffix
     */
    public String getJournalSuffix()
    {
        return journalSuffix;
    }


    /**
     * @param journalSuffix the journalSuffix to set
     */
    public void setJournalSuffix( String journalSuffix )
    {
        this.journalSuffix = journalSuffix;
    }
}
