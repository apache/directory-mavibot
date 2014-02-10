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
 * | nextBtreeHeader    | 8 bytes     |
 * +--------------------+-------------+
 * | pageSize           | 4 bytes     |
 * +--------------------+-------------+
 * | name               | 4 bytes + N |
 * +--------------------+-------------+
 * | keySerializeFQCN   | 4 bytes + N |
 * +--------------------+-------------+
 * | valueSerializeFQCN | 4 bytes + N |
 * +--------------------+-------------+
 * </pre>
 * Each BtreeHeader will be written starting on a new page.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class BTreeHeader<K, V>
{
    /** The current revision */
    private long revision = 0L;

    /** The number of elements in this B-tree */
    private Long nbElems = 0L;

    /** The offset of the B-tree RootPage */
    private long rootPageOffset;

    // Those are data which aren't serialized : they are in memory only */
    /** The position in the file */
    private long btreeOffset;

    /** A Map containing the rootPage for this tree */
    private Page<K, V> rootPage;

    /** The number of users for this BtreeHeader */
    private AtomicInteger nbUsers = new AtomicInteger( 0 );


    /**
     * Creates a BTreeHeader instance
     */
    public BTreeHeader()
    {
    }


    /**
     * @return the btreeOffset
     */
    public long getBTreeOffset()
    {
        return btreeOffset;
    }


    /**
     * @param btreeOffset the btreeOffset to set
     */
    /* no qualifier */void setBTreeOffset( long btreeOffset )
    {
        this.btreeOffset = btreeOffset;
    }


    /**
     * @return the rootPageOffset
     */
    public long getRootPageOffset()
    {
        return rootPageOffset;
    }


    /**
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
     * @return the rootPage
     */
    /* no qualifier */ Page<K, V> getRootPage()
    {
        return rootPage;
    }


    /**
     * @param rootPage the rootPage to set
     */
    /* no qualifier */ void setRootPage( Page<K, V> rootPage )
    {
        this.rootPage = rootPage;
    }


    /**
     * @return the nbUsers
     */
    /* no qualifier */ int getNbUsers()
    {
        return nbUsers.get();
    }


    /**
     * Increment the number of users
     */
    /* no qualifier */ void incrementNbUsers()
    {
        nbUsers.incrementAndGet();
    }


    /**
     * Decrement the number of users
     */
    /* no qualifier */ void decrementNbUsers()
    {
        nbUsers.decrementAndGet();
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "B-treeHeader " );
        sb.append( ", revision[" ).append( revision ).append( "]" );
        sb.append( ", btreeOffset[" ).append( btreeOffset ).append( "]" );
        sb.append( ", rootPageOffset[" ).append( rootPageOffset ).append( "]" );
        sb.append( ", nbElems[" ).append( nbElems ).append( "]" );
        sb.append( ", nbUsers[" ).append( nbUsers.get() ).append( "]" );

        return sb.toString();
    }
}
