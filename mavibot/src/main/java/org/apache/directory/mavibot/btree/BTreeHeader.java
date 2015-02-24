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


import java.util.concurrent.atomic.AtomicInteger;


/**
 * Store in memory the information associated with a B-tree. <br>
 * A B-tree Header on disk contains the following elements :
 * <pre>
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
 * Each B-tree Header will be written starting on a new page.
 * In memory, a B-tree Header store a bit more of information :
 * <li>
 * <ul>rootPage : the associated rootPage in memory</lu>
 * <ul>nbUsers : the number of readThreads using this revision</lu>
 * <ul>offset : the offset of this B-tre header</lu>
 * </li>
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class BTreeHeader<K, V> implements Cloneable
{
    /** The current revision */
    private long revision = 0L;

    /** The number of elements in this B-tree */
    private Long nbElems = 0L;

    /** The offset of the B-tree RootPage */
    private long rootPageOffset;

    /** The position of the B-tree header in the file */
    private long btreeHeaderOffset = RecordManager.NO_PAGE;

    // Those are data which aren't serialized : they are in memory only */
    /** A Map containing the rootPage for this tree */
    private Page<K, V> rootPage;

    /** The number of users for this BtreeHeader */
    private AtomicInteger nbUsers = new AtomicInteger( 0 );

    /** The B-tree this header is associated with */
    private BTree<K, V> btree;


    /**
     * Creates a BTreeHeader instance
     */
    public BTreeHeader()
    {
    }


    /**
     * @return the B-tree info Offset
     */
    public long getBTreeInfoOffset()
    {
        return ( ( PersistedBTree<K, V> ) btree ).getBtreeInfoOffset();
    }


    /**
     * @return the B-tree header Offset
     */
    public long getBTreeHeaderOffset()
    {
        return btreeHeaderOffset;
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
    /* no qualifier */BTreeHeader<K, V> copy()
    {
        BTreeHeader<K, V> copy = clone();

        // Clear the fields that should not be copied
        copy.rootPage = null;
        copy.rootPageOffset = -1L;
        copy.btreeHeaderOffset = -1L;
        copy.nbUsers.set( 0 );

        return copy;
    }


    /**
     * Set the B-tree header offset
     * 
     * @param btreeOffset the B-tree header Offset to set
     */
    /* no qualifier */void setBTreeHeaderOffset( long btreeHeaderOffset )
    {
        this.btreeHeaderOffset = btreeHeaderOffset;
    }


    /**
     * @return the rootPageOffset
     */
    public long getRootPageOffset()
    {
        return rootPageOffset;
    }


    /**
     * Set the Root Page offset
     * 
     * @param rootPageOffset the rootPageOffset to set
     */
    /* no qualifier */void setRootPageOffset( long rootPageOffset )
    {
        this.rootPageOffset = rootPageOffset;
    }


    /**
     * @return the revision
     */
    public long getRevision()
    {
        return revision;
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
        this.rootPageOffset = ( ( AbstractPage<K, V> ) rootPage ).getOffset();
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
     * @return the B-tree
     */
    /* no qualifier */BTree<K, V> getBtree()
    {
        return btree;
    }


    /**
     * Associate a B-tree with this BTreeHeader instance
     * 
     * @param btree the B-tree to set
     */
    /* no qualifier */void setBtree( BTree<K, V> btree )
    {
        this.btree = btree;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "B-treeHeader " );
        sb.append( ", offset[0x" ).append( Long.toHexString( btreeHeaderOffset ) ).append( "]" );
        sb.append( ", name[" ).append( btree.getName() ).append( "]" );
        sb.append( ", revision[" ).append( revision ).append( "]" );
        sb.append( ", btreeInfoOffset[0x" )
            .append( Long.toHexString( ( ( PersistedBTree<K, V> ) btree ).getBtreeInfoOffset() ) ).append( "]" );
        sb.append( ", rootPageOffset[0x" ).append( Long.toHexString( rootPageOffset ) ).append( "]" );
        sb.append( ", nbElems[" ).append( nbElems ).append( "]" );
        sb.append( ", nbUsers[" ).append( nbUsers.get() ).append( "]" );

        return sb.toString();
    }
}
