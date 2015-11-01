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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * The TransactionContext class holds all the modified pages in memory, up to the
 * moment we have to flush them on disk, when a cmmit is applied, or discard them,
 * if a rallback is called.
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class TransactionContext
{
    /** The map containing all the modified pages, using their offset as a key */
    private Map<Long, WALObject> pageMap = new HashMap<Long, WALObject>();
    
    /** The incremental counter used to identify pages */
    private long pageId;
    
    /** The last offset on disk */
    private long lastOffset;
    
    /**
     * A constructor for the TransactionContext, which initialize the last offset to
     * the file size.
     * 
     * @param lastOffset The last offset fo the associated file
     */
    public TransactionContext( long lastOffset )
    {
        this.lastOffset = lastOffset;
        pageId = RecordManager.NO_PAGE;
    }


    /**
     * Retrieve a Page if it has been stored in the context
     * @param offset the offset of the Page we are looking for
     * @return The found Page, or null if it does not exist
     */
    public WALObject getPage( Long offset )
    {
        return pageMap.get( offset );
    }
    
    
    /**
     * Add a page into the TransactionContext
     * 
     * @param offset The Page's offset
     * @param pageIo The Page to add
     */
    public void addPage( Long offset, WALObject page )
    {
        pageMap.put( offset, page );
    }
    
    
    /**
     * @return The list of stored Pages 
     */
    public Collection<WALObject> getPages()
    {
        return pageMap.values();
    }

    
    /**
     * @return the lastOffset
     */
    public long getLastOffset()
    {
        return lastOffset;
    }


    /**
     * @param lastOffset the lastOffset to set
     */
    public void setLastOffset( long lastOffset )
    {
        this.lastOffset = lastOffset;
    }


    /**
     * @param addedSize the size to add
     */
    public void addLastOffset( long addedSize )
    {
        this.lastOffset += addedSize;
    }
    
    
    /**
     * @return The unique page ID
     */
    public long getPageId()
    {
        pageId--;
        
        return pageId;
    }
}
