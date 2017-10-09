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

import java.io.Closeable;
import java.io.IOException;

/**
 * <p>
 * The Transaction is used to protect b-trees against concurrent modifications,
 * and insure that a read is always done against one single revision. It's also
 * used to apply many modifications under one single revision.
 * </p
 * <p>>
 * A read Transaction should be committed or aborted when the user is done with it, 
 * otherwise the pages associated with the given revision, and all the referenced pages, 
 * will remain, leading to a database expansion.
 * </p>
 * <p>
 * A Transaction can be hold for quite a long time, for instance while doing
 * a browse against a big b-tree. At some point, transactions which are pending
 * for too long will be closed by the transaction manager, unless an infinite
 * timeout is used. By default, a transaction will last 30 seconds. 
 * </p>
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface Transaction extends Closeable
{
    /** The default transaction timeout : 30 seconds */
    static final long DEFAULT_TIMEOUT = 30000L;
    
    /**
     * Commit a write transaction. It will apply the changes on 
     * the database.Last, not least, a new version will be created.
     * If called by a Read transaction, it will simply close it.
     */
    void commit() throws IOException;
    
    
    /**
     * Abort a transaction. If it's a {@link ReadTransaction}, it will unlink this transaction
     * from the version it used. If it's a {@link WriteTransaction}; it will drop all the pending
     * changes. The latest version will remain the same.
     */
    void abort() throws IOException;
    
    
    /**
     * @return The current transaction revision
     */
    long getRevision();
    
    
    /**
     * Retrieve a Page from the cache, or read it from the disk.
     * 
     * @param btreeInfo The {@link BtreeInfo} reference
     * @param offset The page offset
     * @return The found {@link Page}
     * @throws IOException If we weren't able to fetch a page
     */
    <K, V> Page<K, V> getPage( BTreeInfo<K, V> btreeInfo , long offset ) throws IOException;


    /**
     * @return the creationDate
     */
    long getCreationDate();

    
    /**
     * Tells if the transaction has been committed/aborted or not.
     *  
     * @return <tt>true</tt> if the transaction has been completed.
     */
    boolean isClosed();
    
    
    /**
     * @return The associated {@link RecordManager}
     */
    RecordManager getRecordManager();
    
    
    /**
     * @return The associated {@link RecordManagerHeader}
     */
    RecordManagerHeader getRecordManagerHeader();


    /**
     * Get a managed B-tree, knowing its name. It will return the B-tree version
     * of the given transaction.
     *
     * @param name The B-tree we are looking for
     * @return The found B-tree, or a BTreeNotFoundException if the B-tree does not exist.
     */
    <K, V> BTree<K, V> getBTree( String name );
}
