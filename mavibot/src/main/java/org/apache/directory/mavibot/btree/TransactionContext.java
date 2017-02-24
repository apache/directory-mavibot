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
 * moment we have to flush them on disk, when a commit is applied, or discard them,
 * if a rollback is called.
 * We also store the BOB Header and the CPB Header, as we will copy them to create a 
 * new version, and we don't want any read to be impacted by such change.
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class TransactionContext
{
    /** The map containing all the modified pages, using their offset as a key */
    private Map<Long, WALObject> pageMap = new HashMap<>();
    
    /** The map containing all the copied pages, using their offset as a key */
    private Map<Long, WALObject> copiedPageMap = new HashMap<>();
    
    /** The global revision */
    private long revision;
    
    /** The last offset on disk */
    private long lastOffset;
    
    /** The RecordManager header for the transaction */
    private RecordManagerHeader recordManagerHeader;
    
    /** The BOB header */
    private BTreeHeader<NameRevision, Long> btreeOfBtreeHeader;
    
    /** The CPB header */
    private BTreeHeader copiedPageBtreeHeader;
    
    /**
     * A constructor for the TransactionContext, which initialize the last offset to
     * the file size.
     * 
     * @param lastOffset The last offset of the associated file
     */
    public TransactionContext( RecordManagerHeader recordManagerHeader )
    {
        this.recordManagerHeader = recordManagerHeader;
    }
    
    
    /**
     * A constructor for the TransactionContext, which initialize the last offset to
     * the file size. This is a Read Transaction context, we don't have to keep the copied page
     * BTreeHeader, nor the last offset.
     * 
     * @param revision The current revision used for this transaction
     * @param btreeOfBtreeHeader The BOB header
     */
    public TransactionContext( long revision, BTreeHeader btreeOfBtreeHeader )
    {
        this.revision = revision;
        this.btreeOfBtreeHeader = btreeOfBtreeHeader;
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
     * Retrieve a Page if it has been copied in the context
     * @param offset the offset of the Page we are looking for
     * @return The found Page, or null if it does not exist
     */
    public WALObject getCopiedPage( Long offset )
    {
        return copiedPageMap.get( offset );
    }
    
    
    /**
     * Add a copied page into the TransactionContext
     * 
     * @param offset The Page's offset
     * @param pageIo The copied Page to add
     */
    public void addCopiedPage( Long offset, WALObject page )
    {
        copiedPageMap.put( offset, page );
    }

    
    /**
     * @return The list of copied Pages 
     */
    public Collection<WALObject> getCopiedPages()
    {
        return copiedPageMap.values();
    }


    /**
     * @return the copiedPageBtreeHeader
     */
    public BTreeHeader getCopiedPageBtreeHeader() {
        return copiedPageBtreeHeader;
    }


    /**
     * @param copiedPageBtreeHeader the copiedPageBtreeHeader to set
     */
    public void setCopiedPageBtreeHeader(BTreeHeader copiedPageBtreeHeader) {
        this.copiedPageBtreeHeader = copiedPageBtreeHeader;
    }

    
    /**
     * @return the btreeOfBtreeHeader
     */
    public BTreeHeader<NameRevision, Long> getBtreeOfBtreeHeader() {
        return btreeOfBtreeHeader;
    }


    /**
     * @param btreeOfBtreeHeader the btreeOfBtreeHeader to set
     */
    public void setBtreeOfBtreeHeader(BTreeHeader btreeOfBtreeHeader) {
        this.btreeOfBtreeHeader = btreeOfBtreeHeader;
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
     * @return The current revision
     */
    public long getRevision()
    {
        return revision;
    }
    
    
    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "Context[" ).append( revision ).append( "]\n" );
        sb.append( "    Pages in context : {" );
        
        if ( pageMap.size() > 0 )
        {
            boolean isFirst = true;
            
            for ( long offset : pageMap.keySet() )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }
                
                sb.append( "0x" ).append( Long.toHexString( offset ) );
            }
        }
        
        sb.append( "}\n" );
        
        sb.append( "    CopiedPages : {" );
        
        if ( pageMap.size() > 0 )
        {
            boolean isFirst = true;
            
            for ( long offset : copiedPageMap.keySet() )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }
                
                sb.append( "0x" ).append( Long.toHexString( offset ) );
            }
        }
        
        sb.append( "}\n" );

        return sb.toString();
    }
}
