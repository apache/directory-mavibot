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


import java.util.Date;


/**
 * The Transaction is used to protect the BTree against concurrent modifcation,
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
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 */
public class Transaction<K, V>
{
    /** The associated revision */
    private long revision;

    /** The date of creation */
    private long creationDate;

    /** The revision on which we are having a transaction */
    private Page<K, V> page;


    /**
     * Creates a new transaction instance
     * @param revision The revision this transaction is using
     * @param creationDate The creation date for this transaction
     */
    public Transaction( Page<K, V> page, long revision, long creationDate )
    {
        this.revision = revision;
        this.creationDate = creationDate;
        this.page = page;
    }


    /**
     * @return the revision
     */
    public long getRevision()
    {
        return revision;
    }


    /**
     * @return the creationDate
     */
    public long getCreationDate()
    {
        return creationDate;
    }


    /**
     * Close the transaction, releasing the revision it was using.
     */
    public void close()
    {
        page = null;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "Transaction[" + revision + ":" + new Date( creationDate ) + "]";
    }
}
