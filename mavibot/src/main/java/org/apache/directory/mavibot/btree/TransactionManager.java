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
 * An interface used to manage the transactions mechanism in B-trees. Transactions are cross
 * B-trees.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface TransactionManager<K, V>
{
    /**
     * Starts a transaction
     */
    void beginTransaction();


    /**
     * Commits a transaction
     */
    void commit();


    /**
     * Rollback a transaction
     */
    void rollback();


    /**
     * Gets the current BtreeHeader for a given BTree.
     * 
     * @param btreeName The Btree name we are looking the BtreeHeader for
     * @return the current BTreeHeader
     */
    BTreeHeader<K, V> getBTreeHeader( String btreeName );


    /**
     * Updates the map of new BTreeHeaders
     * 
     * @param btreeHeader The new BtreeHeader
     */
    void updateNewBTreeHeaders( BTreeHeader<K, V> btreeHeader );
}
