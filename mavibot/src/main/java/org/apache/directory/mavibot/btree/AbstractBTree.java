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
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * A BTree abstract class containing the methods shared by the PersistedBTree or the InMemoryBTree
 * implementations.
 *
 * @param <K> The Key type
 * @param <V> The Value type
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/abstract class AbstractBTree<K, V> implements BTree<K, V>
{
    /** The read transaction timeout */
    protected long readTimeOut = DEFAULT_READ_TIMEOUT;

    /** The copied Header for a managed BTree */
    //protected BTreeHeader<K, V> newBtreeHeader = new BTreeHeader<K, V>();

    /** The Key serializer used for this tree.*/
    protected ElementSerializer<K> keySerializer;

    /** The Value serializer used for this tree. */
    protected ElementSerializer<V> valueSerializer;

    /** The list of read transactions being executed */
    protected ConcurrentLinkedQueue<ReadTransaction<K, V>> readTransactions;

    /** The size of the buffer used to write data in disk */
    protected int writeBufferSize;

    /** Flag to enable duplicate key support */
    protected boolean allowDuplicates;

    /** The number of elements in a page for this B-tree */
    protected int pageSize;

    /** The BTree name */
    protected String name;

    /** The FQCN of the Key serializer */
    protected String keySerializerFQCN;

    /** The FQCN of the Value serializer */
    protected String valueSerializerFQCN;

    /** The thread responsible for the cleanup of timed out reads */
    protected Thread readTransactionsThread;

    /** The BTree type : either in-memory, disk backed or persisted */
    protected BTreeTypeEnum btreeType;

    /** The current transaction */
    protected WriteTransaction writeTransaction;

    /** The map of all the used BtreeHeaders */
    protected Map<Long, BTreeHeader<K, V>> btreeRevisions = new ConcurrentHashMap<Long, BTreeHeader<K, V>>();

    /** The current revision */
    protected AtomicLong currentRevision = new AtomicLong( 0L );

    /**
     * Starts a Read Only transaction. If the transaction is not closed, it will be
     * automatically closed after the timeout
     *
     * @return The created transaction
     */
    protected ReadTransaction<K, V> beginReadTransaction()
    {
        BTreeHeader<K, V> btreeHeader = getBtreeHeader();

        ReadTransaction<K, V> readTransaction = new ReadTransaction<K, V>( btreeHeader );

        readTransactions.add( readTransaction );

        return readTransaction;
    }


    /**
     * Starts a Read Only transaction. If the transaction is not closed, it will be
     * automatically closed after the timeout
     *
     * @return The created transaction
     */
    protected ReadTransaction<K, V> beginReadTransaction( long revision )
    {
        ReadTransaction<K, V> readTransaction = new ReadTransaction<K, V>( getBtreeHeader( revision ) );

        readTransactions.add( readTransaction );

        return readTransaction;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browse() throws IOException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        ParentPos<K, V>[] stack = (ParentPos<K, V>[]) Array.newInstance( ParentPos.class, 32 );

        TupleCursor<K, V> cursor = transaction.getRootPage().browse( transaction, stack, 0 );

        // Set the position before the first element
        cursor.beforeFirst();

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browse( long revision ) throws IOException, KeyNotFoundException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        ParentPos<K, V>[] stack = (ParentPos<K, V>[]) Array.newInstance( ParentPos.class, 32 );

        // And get the cursor
        TupleCursor<K, V> cursor = transaction.getRootPage().browse( transaction, stack, 0 );

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browseFrom( K key ) throws IOException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        ParentPos<K, V>[] stack = (ParentPos<K, V>[]) Array.newInstance( ParentPos.class, 32 );

        TupleCursor<K, V> cursor = transaction.getRootPage().browse( key, transaction, stack, 0 );

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browseFrom( long revision, K key ) throws IOException, KeyNotFoundException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        ParentPos<K, V>[] stack = (ParentPos<K, V>[]) Array.newInstance( ParentPos.class, 32 );

        // And get the cursor
        TupleCursor<K, V> cursor = transaction.getRootPage().browse( key, transaction, stack, 0 );

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( K key, V value ) throws IOException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        try
        {
            return transaction.getRootPage().contains( key, value );
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( long revision, K key, V value ) throws IOException, KeyNotFoundException
    {
        // Fetch the root page for this revision
        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        try
        {
            return transaction.getRootPage().contains( key, value );
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    public Tuple<K, V> delete( K key ) throws IOException
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        Tuple<K, V> deleted = delete( key, currentRevision.get() + 1 );

        return deleted;
    }


    /**
     * {@inheritDoc}
     */
    public Tuple<K, V> delete( K key, V value ) throws IOException
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        if ( value == null )
        {
            throw new IllegalArgumentException( "Value must not be null" );
        }

        Tuple<K, V> deleted = delete( key, value, currentRevision.get() + 1 );

        return deleted;
    }


    /**
     * Delete the entry which key is given as a parameter. If the entry exists, it will
     * be removed from the tree, the old tuple will be returned. Otherwise, null is returned.
     *
     * @param key The key for the entry we try to remove
     * @return A Tuple<K, V> containing the removed entry, or null if it's not found.
     */
    /*no qualifier*/Tuple<K, V> delete( K key, long revision ) throws IOException
    {
        return delete( key, null, revision );
    }


    /*no qualifier*/abstract Tuple<K, V> delete( K key, V value, long revision ) throws IOException;


    /**
     * {@inheritDoc}
     */
    public V insert( K key, V value ) throws IOException
    {
        V existingValue = null;

        InsertResult<K, V> result = insert( key, value, -1L );

        if ( result instanceof ModifyResult )
        {
            existingValue = ( ( ModifyResult<K, V> ) result ).getModifiedValue();
        }

        return existingValue;
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */abstract InsertResult<K, V> insert( K key, V value, long revision ) throws IOException;


    /**
     * Flush the latest revision to disk. We will replace the current file by the new one, as
     * we flush in a temporary file.
     */
    public void flush() throws IOException
    {
    }


    /**
     * {@inheritDoc}
     */
    public V get( K key ) throws IOException, KeyNotFoundException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        try
        {
            return transaction.getRootPage().get( key );
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    public V get( long revision, K key ) throws IOException, KeyNotFoundException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        try
        {
            return transaction.getRootPage().get( key );
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    public abstract Page<K, V> getRootPage();


    /**
     * {@inheritDoc}
     */
    /* no qualifier */abstract void setRootPage( Page<K, V> root );


    /**
     * {@inheritDoc}
     */
    public ValueCursor<V> getValues( K key ) throws IOException, KeyNotFoundException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        try
        {
            return transaction.getRootPage().getValues( key );
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    public boolean hasKey( K key ) throws IOException
    {
        if ( key == null )
        {
            return false;
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        try
        {
            return transaction.getRootPage().hasKey( key );
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    public boolean hasKey( long revision, K key ) throws IOException, KeyNotFoundException
    {
        if ( key == null )
        {
            return false;
        }

        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        try
        {
            return transaction.getRootPage().hasKey( key );
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    public ElementSerializer<K> getKeySerializer()
    {
        return keySerializer;
    }


    /**
     * {@inheritDoc}
     */
    public void setKeySerializer( ElementSerializer<K> keySerializer )
    {
        this.keySerializer = keySerializer;
        keySerializerFQCN = keySerializer.getClass().getName();
    }


    /**
     * {@inheritDoc}
     */
    public String getKeySerializerFQCN()
    {
        return keySerializerFQCN;
    }


    /**
     * {@inheritDoc}
     */
    public ElementSerializer<V> getValueSerializer()
    {
        return valueSerializer;
    }


    /**
     * {@inheritDoc}
     */
    public void setValueSerializer( ElementSerializer<V> valueSerializer )
    {
        this.valueSerializer = valueSerializer;
        valueSerializerFQCN = valueSerializer.getClass().getName();
    }


    /**
     * {@inheritDoc}
     */
    public String getValueSerializerFQCN()
    {
        return valueSerializerFQCN;
    }


    /**
     * {@inheritDoc}
     */
    public long getRevision()
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        try
        {
            return transaction.getRevision();
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setRevision( long revision )
    {
        getBtreeHeader().setRevision( revision );
    }


    /**
     * Store the new revision in the map of btrees, increment the current revision
     */
    protected void storeRevision( BTreeHeader<K, V> btreeHeader )
    {
        long revision = btreeHeader.getRevision();

        synchronized ( btreeRevisions )
        {
            btreeRevisions.put( revision, btreeHeader );
        }

        currentRevision.set( revision );
    }


    /**
     * {@inheritDoc}
     */
    public long getReadTimeOut()
    {
        return readTimeOut;
    }


    /**
     * {@inheritDoc}
     */
    public void setReadTimeOut( long readTimeOut )
    {
        this.readTimeOut = readTimeOut;
    }


    /**
     * {@inheritDoc}
     */
    public long getNbElems()
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        try
        {
            return transaction.getBtreeHeader().getNbElems();
        }
        finally
        {
            transaction.close();
        }
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setNbElems( long nbElems )
    {
        getBtreeHeader().setNbElems( nbElems );
    }


    /**
     * {@inheritDoc}
     */
    public int getPageSize()
    {
        return pageSize;
    }


    /**
     * {@inheritDoc}
     */
    public void setPageSize( int pageSize )
    {
        if ( pageSize <= 2 )
        {
            this.pageSize = DEFAULT_PAGE_SIZE;
        }
        else
        {
            this.pageSize = getPowerOf2( pageSize );
        }
    }


    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return name;
    }


    /**
     * {@inheritDoc}
     */
    public void setName( String name )
    {
        this.name = name;
    }


    /**
     * {@inheritDoc}
     */
    public Comparator<K> getComparator()
    {
        return keySerializer.getComparator();
    }


    /**
     * {@inheritDoc}
     */
    public int getWriteBufferSize()
    {
        return writeBufferSize;
    }


    /**
     * {@inheritDoc}
     */
    public void setWriteBufferSize( int writeBufferSize )
    {
        this.writeBufferSize = writeBufferSize;
    }


    /**
     * {@inheritDoc}
     */
    public boolean isAllowDuplicates()
    {
        return allowDuplicates;
    }


    /**
     * {@inheritDoc}
     */
    public void setAllowDuplicates( boolean allowDuplicates )
    {
        this.allowDuplicates = allowDuplicates;
    }


    /**
     * {@inheritDoc}
     */
    public BTreeTypeEnum getType()
    {
        return btreeType;
    }


    /**
     * @param type the type to set
     */
    public void setType( BTreeTypeEnum type )
    {
        this.btreeType = type;
    }


    /**
     * Gets the number which is a power of 2 immediately above the given positive number.
     */
    private int getPowerOf2( int size )
    {
        int newSize = --size;
        newSize |= newSize >> 1;
        newSize |= newSize >> 2;
        newSize |= newSize >> 4;
        newSize |= newSize >> 8;
        newSize |= newSize >> 16;
        newSize++;

        return newSize;
    }


    /**
     * @return The current BtreeHeader
     */
    protected BTreeHeader<K, V> getBtreeHeader()
    {
        synchronized ( btreeRevisions )
        {
            long revision = currentRevision.get();
            BTreeHeader<K, V> btreeHeader = btreeRevisions.get( revision );

            return btreeHeader;
        }
    }


    /**
     * @return The current BtreeHeader
     */
    protected BTreeHeader<K, V> getBtreeHeader( long revision )
    {
        return btreeRevisions.get( revision );
    }


    /**
     * Create a thread that is responsible of cleaning the transactions when
     * they hit the timeout
     */
    /*no qualifier*/void createTransactionManager()
    {
        Runnable readTransactionTask = new Runnable()
        {
            public void run()
            {
                try
                {
                    ReadTransaction<K, V> transaction = null;

                    while ( !Thread.currentThread().isInterrupted() )
                    {
                        long timeoutDate = System.currentTimeMillis() - readTimeOut;
                        long t0 = System.currentTimeMillis();
                        int nbTxns = 0;

                        // Loop on all the transactions from the queue
                        while ( ( transaction = readTransactions.peek() ) != null )
                        {
                            nbTxns++;

                            if ( transaction.isClosed() )
                            {
                                // The transaction is already closed, remove it from the queue
                                readTransactions.poll();
                                continue;
                            }

                            // Check if the transaction has timed out
                            if ( transaction.getCreationDate() < timeoutDate )
                            {
                                transaction.close();
                                readTransactions.poll();

                                synchronized ( btreeRevisions )
                                {
                                    btreeRevisions.remove( transaction.getRevision() );
                                }

                                continue;
                            }

                            // We need to stop now
                            break;
                        }

                        long t1 = System.currentTimeMillis();

                        if ( nbTxns > 0 )
                        {
                            System.out.println( "Processing old txn : " + nbTxns + ", " + ( t1 - t0 ) + "ms" );
                        }

                        // Wait until we reach the timeout
                        Thread.sleep( readTimeOut );
                    }
                }
                catch ( InterruptedException ie )
                {
                    //System.out.println( "Interrupted" );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        };

        readTransactionsThread = new Thread( readTransactionTask );
        readTransactionsThread.setDaemon( true );
        readTransactionsThread.start();
    }
}
