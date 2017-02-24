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


/**
 * An interface used to manage the transactions mechanism in b-trees. Transactions span across
 * all the managed b-trees. 
 * <br>
 * On can start a read or a write transaction, then commit or abort it. 
 * <br>
 * Many read transactions can be executed at the same time, and each one of them
 * will use the version in use when creating during all their existence. That means we may have
 * different read transactions using different versions.
 * <br>
 * One single write transaction can be executed, which means the caller will be blocked
 * until the previous write transaction is completed.
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface TransactionManager
{
    /**
     * Starts a Read transaction. One can start as many read transaction as needed. One can 
     * start a read transaction even if a write transaction has been started.
     * <br>
     * A read transaction will use the latest revision available, and will work on this revision
     * during all its life. New revision might be created in the mean time, but they won't be visible.
     * <br>
     * With respect to this rule, it's likely that what we get back from one B-tree might not be up to date.
     * <br>
     * When done, close the transaction by calling the {@link Transaction#commit()} or 
     * {@link Transaction#abort()} method.
     * <br>
     * Note : holding a read revision for a long period of time might lead to a database expansion, as
     * the hold revision will forbid the cleanup thread to reclaim any page that are used by this revision.
     * Be sure to commit (or abort) your transaction when done.
     * <br>
     * We have a timeout that is used to avoid long lasting transaction, which defaults to 30 seconds. It's 
     * possible to change this timeout by passing a specific timeout when starting a read transaction. 
     * <br>
     * The transaction is valid across all the b-trees.
     * 
     * @return The created read {@link Transaction}
     */
    Transaction beginReadTransaction();


    /**
     * Starts a read transaction that has a limited live time. The default is 30 seconds.
     *  
     * @param timeout The delay after which the transaction will be automatically closed, in ms. 
     * Passing 0 or a negative value will make the read transaction to last forever.
     * @return The created read {@link Transaction}
     */
    Transaction beginReadTransaction( long timeout );


    /**
     * Starts a Write transaction. One can only start one single write transaction. Any
     * thread starting a write transaction will be blocked by an existing write transaction.
     * <br>
     * Once committed, a new revision will be available for the next read or write transaction.
     * <br>
     * The write transaction spans across all the b-trees.
     * 
     * @return The created write {@link Transaction}
     */
    WriteTransaction beginWriteTransaction();
}
