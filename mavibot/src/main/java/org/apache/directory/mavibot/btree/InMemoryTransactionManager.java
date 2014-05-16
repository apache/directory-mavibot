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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An implementation of a TransactionManager for in-memory B-trees
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class InMemoryTransactionManager extends AbstractTransactionManager
{
    /** A lock to protect the transaction handling */
    private Lock transactionLock = new ReentrantLock();
    
    /** A ThreadLocalStorage used to store the current transaction */
    private static final ThreadLocal<Integer> context = new ThreadLocal<Integer>();
    
    /** A Map keeping the latest revisions for each managed BTree */
    private Map<String, BTreeHeader<?, ?>> currentBTreeHeaders = new HashMap<String, BTreeHeader<?, ?>>();

    /** A Map storing the new revisions when some change have been made in some BTrees */
    private Map<String, BTreeHeader<?, ?>> newBTreeHeaders = new HashMap<String, BTreeHeader<?, ?>>();
    
    /** A lock to protect the BtreeHeader maps */
    private ReadWriteLock btreeHeadersLock = new ReentrantReadWriteLock();

    /**
     * {@inheritDoc}
     */
    @Override
    public void beginTransaction()
    {
        // First, take the lock
        transactionLock.lock();
        
        // Now, check the TLS state
        Integer nbTxnLevel = context.get();
        
        if ( nbTxnLevel == null )
        {
            context.set( 1 );
        }
        else
        {
            // And increment the counter of inner txn.
            context.set( nbTxnLevel + 1 );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void commit()
    {
        int nbTxnStarted = context.get();
        
        if ( nbTxnStarted == 0 )
        {
            // The transaction was rollbacked, quit immediatelly
            transactionLock.unlock();
            
            return;
        }
        else
        {
            
            // And decrement the number of started transactions
            context.set( nbTxnStarted - 1 );
        }

        // Finally, release the global lock
        transactionLock.unlock();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback()
    {
        // Reset the counter
        context.set( 0 );

        // Finally, release the global lock
        transactionLock.unlock();
    }
    
    
    /**
     * Get the current BTreeHeader for a given Btree. It might not exist
     */
    public BTreeHeader getBTreeHeader( String name )
    {
        // Get a lock
        btreeHeadersLock.readLock().lock();
        
        // get the current BTree Header for this BTree and revision
        BTreeHeader<?, ?> btreeHeader = currentBTreeHeaders.get( name );
        
        // And unlock 
        btreeHeadersLock.readLock().unlock();

        return btreeHeader;
    }


    
    
    /**
     * {@inheritDoc}
     */
    public void updateNewBTreeHeaders( BTreeHeader btreeHeader )
    {
        newBTreeHeaders.put( btreeHeader.getBtree().getName(), btreeHeader );
    }
}
