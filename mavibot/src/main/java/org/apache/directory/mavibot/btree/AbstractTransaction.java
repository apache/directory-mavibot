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
import java.util.Date;

import org.apache.directory.mavibot.btree.exception.BTreeNotFoundException;


/**
 * A read transaction is used to insure that reads are always done against 
 * one single revision.
 * <br>
 * A read transaction should be closed when the user is done with it, otherwise the
 * pages associated with the given revision, and all the referenced pages, will
 * remain on the storage. 
 * <p/>
 * A Transaction can be hold for quite a long time, for instance while doing
 * a browse against a big b-tree. At some point, transactions which are pending
 * for too long will be closed by the transaction manager (the default is to keep a 
 * read transaction opened for 30 s). If one need to keep a read transaction opened
 * for a longer time, pass a longer timeout to the constructor. 
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public abstract class AbstractTransaction implements Transaction
{
    /** The date of creation */
    private long creationDate;
    
    /** The timeout */
    private long timeout = Transaction.DEFAULT_TIMEOUT;

    /** A flag used to tell if a transaction is closed or not */
    private volatile boolean closed;
    
    /** A flag set to <tt>true</tt> if an exception has been raised */ 
    protected volatile boolean aborted = false;
    
    /** The reference to the recordManager */
    protected RecordManager recordManager;
    
    /** The reference to the recordManagerHeader */
    protected RecordManagerHeader recordManagerHeader;
    
    /**
     * Creates a new read transaction, with a default 30 seconds timeout.
     *
     * @param recordManager The associated RecordManager
     */
    /* No qualifier*/ AbstractTransaction( RecordManager recordManager )
    {
        this( recordManager, DEFAULT_TIMEOUT );
    }
    
    
    /**
     * Creates a new read transaction, with a specific tiemout. It create a new copy of
     * the recordManager header
     *
     * @param recordManager The asscoated RecordManager
     * @param timeout The transaction's timrout
     */
    /* No qualifier*/ AbstractTransaction( RecordManager recordManager, long timeout )
    {
        assert recordManager != null;
        this.recordManager = recordManager;
        
        this.timeout = timeout;
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> Page<K, V> getPage( BTreeInfo<K, V> btreeInfo, long offset ) throws IOException
    {
        try
        {
            return ( Page<K, V> ) recordManager.getPage( btreeInfo, recordManagerHeader.pageSize, offset );
        }
        catch ( IOException ioe )
        {
            aborted = true;
            throw ioe;
        }
        catch ( Exception e )
        {
            System.out.println( Long.toHexString( offset ) );
            throw e;
        }
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RecordManager getRecordManager()
    {
        return recordManager;
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RecordManagerHeader getRecordManagerHeader()
    {
        return recordManagerHeader;
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public long getRevision()
    {
        return recordManagerHeader.getRevision();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getCreationDate()
    {
        return creationDate;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
        closed = true;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed()
    {
        return closed;
    }

    
    /**
     * @return the timeout
     */
    public long getTimeout()
    {
        return timeout;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void abort() throws IOException
    {
        aborted = true;

        // Decrement the counter
        int txnNumber = recordManagerHeader.txnCounter.getAndDecrement();

        if ( ( txnNumber == 0 ) && ( recordManager.activeTransactionsList.peek().revision != recordManagerHeader.revision ) )
        {
            // We can cleanup 
            recordManager.activeTransactionsList.remove( recordManagerHeader );
            recordManager.deadTransactionsList.add( recordManagerHeader );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() throws IOException
    {
        closed = true;
    }
    
    
    /**
     * {@inheritDoc}
     * @param name The B-tree we are looking for
     * @return The found B-tree, or a BTreeNotFoundException if teh B-tree does not exist.
     */
    @Override
    public <K, V> BTree<K, V> getBTree( String name )
    {
        BTree<K, V> btree = recordManagerHeader.getBTree( name );
        
        if ( btree == null )
        {
            aborted = true;
            throw new BTreeNotFoundException( "Cannot find btree " + name );
        }
        
        return btree;
    }


    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "[rev:" );
        sb.append( getRevision() );
        sb.append( ", " );
        sb.append( new Date( creationDate ) );
        sb.append( ", " );
        sb.append( timeout );
        
        if ( closed )
        {
            sb.append( ", CLOSED" );
        }
        
        sb.append( "]" );
        
        return sb.toString();
    }
}
