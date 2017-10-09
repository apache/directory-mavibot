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


import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Store in memory the information associated with a b-tree. <br>
 * A B-tree Header on disk contains the following elements :
 * <pre>
 * +--------------------+-------------+
 * | PageID             | 8 bytes     |
 * +--------------------+-------------+
 * | revision           | 8 bytes     |
 * +--------------------+-------------+
 * | nbElems            | 8 bytes     |
 * +--------------------+-------------+
 * | rootPageOffset     | 8 bytes     |
 * +--------------------+-------------+
 * | BtreeHeaderOffset  | 8 bytes     |
 * +--------------------+-------------+
 * </pre>
 * Each b-tree Header will be written starting on a new page.
 * In memory, a B-tree Header store a bit more of information :
 * <ul>
 *   <li>rootPage : the associated rootPage in memory</li>
 *   <li>nbUsers : the number of readThreads using this revision</li>
 *   <li>offset : the offset of this B-tre header</li>
 * </ul>
 *
 * @param <K> The b-tree key type
 * @param <V> The b-tree value type
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class BTreeHeader<K, V> extends AbstractWALObject<K, V> implements Cloneable
{
    /** The number of elements in this B-tree */
    private long nbElems = 0L;

    // Those are data which aren't serialized : they are in memory only */
    /** A Map containing the rootPage for this tree */
    private Page<K, V> rootPage;
    
    /** The number of users for this BtreeHeader */
    private AtomicInteger nbUsers = new AtomicInteger( 0 );


    /**
     * Creates a BTreeHeader instance
     */
    public BTreeHeader( BTreeInfo<K, V> btreeInfo )
    {
        super( btreeInfo );
    }


    /**
     * @return the B-tree info Offset
     */
    public long getBTreeInfoOffset()
    {
        return btreeInfo.getOffset();
    }


    /**
     * Clone the BTreeHeader
     * 
     * @return The cloned BTreeHeader
     */
    public BTreeHeader<K, V> clone()
    {
        try
        {
            BTreeHeader<K, V> copy = ( BTreeHeader<K, V> ) super.clone();

            return copy;
        }
        catch ( CloneNotSupportedException cnse )
        {
            return null;
        }
    }


    /**
     * Copy the current B-tree header and return the copy
     * @return The copied B-tree header
     */
    /* no qualifier */BTreeHeader<K, V> copy( RecordManagerHeader recordManagerHeader )
    {
        BTreeHeader<K, V> copy = clone();

        // Clear the fields that should not be copied
        copy.rootPage = null;
        copy.offset = -1L;
        copy.nbUsers.set( 0 );
        copy.pageIOs = null;
        copy.id = recordManagerHeader.idCounter++;

        return copy;
    }


    /**
     * @return the rootPageOffset
     */
    public long getRootPageOffset()
    {
        if ( rootPage != null )
        {
            return rootPage.getOffset();
        }
        else
        {
            return BTreeConstants.NO_PAGE;
        }
    }


    /**
     * Set the new revision
     * 
     * @param revision the revision to set
     */
    /* no qualifier */void setRevision( long revision )
    {
        this.revision = revision;
    }


    /**
     * @return the nbElems
     */
    public long getNbElems()
    {
        return nbElems;
    }


    /**
     * @param nbElems the nbElems to set
     */
    /* no qualifier */void setNbElems( long nbElems )
    {
        this.nbElems = nbElems;
    }


    /**
     * Increment the number of elements
     */
    /* no qualifier */void incrementNbElems()
    {
        nbElems++;
    }


    /**
     * Decrement the number of elements
     */
    /* no qualifier */void decrementNbElems()
    {
        nbElems--;
    }


    /**
     * Get the root page
     * @return the rootPage
     */
    /* no qualifier */Page<K, V> getRootPage()
    {
        return rootPage;
    }


    /**
     * Set the root page
     * @param rootPage the rootPage to set
     */
    /* no qualifier */void setRootPage( Page<K, V> rootPage )
    {
        this.rootPage = rootPage;
    }


    /**
     * Get the number of users
     * 
     * @return the nbUsers
     */
    /* no qualifier */int getNbUsers()
    {
        return nbUsers.get();
    }


    /**
     * Increment the number of users
     */
    /* no qualifier */void incrementNbUsers()
    {
        nbUsers.incrementAndGet();
    }


    /**
     * Decrement the number of users
     */
    /* no qualifier */void decrementNbUsers()
    {
        nbUsers.decrementAndGet();
    }


    /**
     * Serialize a BTreeHeader. It will contain the following data :<br/>
     * <ul>
     *   <li>the Page ID : a long</li>
     *   <li>the revision : a long</li>
     *   <li>the number of elements in the B-tree : a long</li>
     *   <li>the root page offset : a long</li>
     *   <li>the B-tree info page offset : a long</li>
     * </ul>
     * {@inheritDoc}
     */
    @Override
    public PageIO[] serialize( WriteTransaction transaction ) throws IOException
    {
        int bufferSize =
            BTreeConstants.LONG_SIZE + // The page ID
                BTreeConstants.LONG_SIZE + // The revision
                BTreeConstants.LONG_SIZE + // the number of element
                BTreeConstants.LONG_SIZE + // The root page offset
                BTreeConstants.LONG_SIZE; // The B-tree info page offset
        
        RecordManager recordManager = transaction.getRecordManager();
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();

        // We are done. Allocate the pages we need to store the data, if we don't have
        // a pageIOs already.
        if ( pageIOs == null ) 
        {
            pageIOs = recordManager.getFreePageIOs( recordManagerHeader, bufferSize );
        }

        // Now store the B-tree data in the pages :
        // - the Page ID
        // - the B-tree revision
        // - the B-tree number of elements
        // - the B-tree root page offset
        // - the B-tree info page offset
        // Starts at 0
        long position = 0L;

        // The page ID
        position = recordManager.store( recordManagerHeader, position, id, pageIOs );
        
        // The B-tree current revision
        position = recordManager.store( recordManagerHeader, position, getRevision(), pageIOs );

        // The nb elems in the tree
        position = recordManager.store( recordManagerHeader, position, getNbElems(), pageIOs );

        // Now, we can inject the B-tree rootPage offset into the B-tree header
        position = recordManager.store( recordManagerHeader, position, getRootPageOffset(), pageIOs );

        // The B-tree info page offset
        recordManager.store( recordManagerHeader, position, btreeInfo.getOffset(), pageIOs );

        offset = pageIOs[0].getOffset();

        /*
        LOG.debug( "Serializing the new '{}' btree header", btree.getName() );

        if ( LOG_PAGES.isDebugEnabled() )
        {
            LOG_PAGES.debug( "Writing BTreeHeader revision {} for {}", btreeHeader.getRevision(), btree.getName() );
            StringBuilder sb = new StringBuilder();

            sb.append( "Offset : " ).append( Long.toHexString( btreeInfo.getOffset() ) ).append( "\n" );
            sb.append( "    ID       : " ).append( id ).append( "\n" );
            sb.append( "    Revision : " ).append( getRevision() ).append( "\n" );
            sb.append( "    NbElems  : " ).append( getNbElems() ).append( "\n" );
            sb.append( "    RootPage : 0x" ).append( Long.toHexString( getRootPageOffset() ) ).append( "\n" );
            sb.append( "    Info     : 0x" ).append( Long.toHexString( btreeInfo.getOffset() ) ).append( "\n" );

            LOG_PAGES.debug( "Btree Header[{}]\n{}", getRevision(), sb.toString() );
        }
        */

        return pageIOs;
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public String prettyPrint()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "{Header(" ).append( id ).append( ")@" );
        
        if ( offset == BTreeConstants.NO_PAGE )
        {
            sb.append( "---" );
        }
        else
        {
            sb.append( String.format( "0x%4X", offset ) );
        }
        
        sb.append( ",<" );
        sb.append( getName() ).append( ':' ).append( getRevision() );
        sb.append( ">}" );

        return sb.toString();
    }


    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "B-treeHeader " );
        sb.append( ", offset[0x" ).append( Long.toHexString( offset ) ).append( "]" );
        sb.append( ", name[" );
        sb.append( getName() );
        sb.append( ':' );
        sb.append( revision );
        sb.append( "]" );
        sb.append( ", revision[" ).append( revision ).append( "]" );
        sb.append( ", btreeInfoOffset[0x" )
            .append( Long.toHexString( btreeInfo.getOffset() ) ).append( "]" );
        sb.append( ", rootPageOffset[0x" ).append( Long.toHexString( getRootPageOffset() ) ).append( "]" );
        sb.append( ", nbElems[" ).append( nbElems ).append( "]" );
        sb.append( ", ID[" ).append( id ).append( "]" );

        return sb.toString();
    }
}
