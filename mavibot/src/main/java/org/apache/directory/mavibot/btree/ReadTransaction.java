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


import java.util.Date;


/**
 * The Transaction is used to protect the BTree against concurrent modification,
 * and insure that a read is always done against one single revision. It's also
 * used to gather many modifications under one single revision, if needed.
 * <p/>
 * A Transaction should be closed when the user is done with it, otherwise the
 * pages associated with the given revision, and all the referenced pages, will
 * remain on the storage.
 * <p/>
 * A Transaction can be hold for quite a long time, for instance while doing
 * a browse against a big BTree. At some point, transactions which are pending
 * for too long will be closed by the transaction manager.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 */
public class ReadTransaction<K, V>
{
    /** The associated revision */
    private long revision;

    /** The date of creation */
    private long creationDate;

    /** The revision on which we are having a transaction */
    private volatile Page<K, V> rootPage;

    /** The associated B-tree header */
    private BTreeHeader<K, V> btreeHeader;

    /** A flag used to tell if a transaction is closed or not */
    private volatile boolean closed;


    /**
     * Creates a new transaction instance
     *
     * @param btreeHeader The BtreeHeader we will use for this read transaction
     */
    public ReadTransaction( BTreeHeader<K, V> btreeHeader )
    {
        if ( btreeHeader != null )
        {
            this.revision = btreeHeader.getRevision();
            this.creationDate = System.currentTimeMillis();
            this.rootPage = btreeHeader.getRootPage();
            this.btreeHeader = btreeHeader;
            closed = false;
        }
    }


    /**
     * @return the associated revision
     */
    public long getRevision()
    {
        return revision;
    }


    /**
     * @return the associated rootPage
     */
    public Page<K, V> getRootPage()
    {
        return rootPage;
    }


    /**
     * @return the creationDate
     */
    public long getCreationDate()
    {
        return creationDate;
    }


    /**
     * @return the btreeHeader
     */
    public BTreeHeader<K, V> getBtreeHeader()
    {
        return btreeHeader;
    }


    /**
     * Close the transaction, releasing the revision it was using.
     */
    public void close()
    {
        rootPage = null;
        closed = true;
    }


    /**
     * @return true if this transaction has been closed
     */
    public boolean isClosed()
    {
        return closed;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "Transaction[" + revision + ":" + new Date( creationDate ) + ", closed :" + closed + "]";
    }
}
