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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

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

    /** The Header for a managed BTree */
    protected BTreeHeader btreeHeader;

    /** The current rootPage */
    protected volatile Page<K, V> rootPage;

    /** The Key serializer used for this tree.*/
    protected ElementSerializer<K> keySerializer;

    /** The Value serializer used for this tree. */
    protected ElementSerializer<V> valueSerializer;

    /** The list of read transactions being executed */
    protected ConcurrentLinkedQueue<ReadTransaction<K, V>> readTransactions;

    /** The size of the buffer used to write data in disk */
    protected int writeBufferSize;

    /** A lock used to protect the write operation against concurrent access */
    protected ReentrantLock writeLock;

    /** Flag to enable duplicate key support */
    private boolean allowDuplicates;

    /** The thread responsible for the cleanup of timed out reads */
    protected Thread readTransactionsThread;

    /** The BTree type : either in-memory, disk backed or persisted */
    private BTreeTypeEnum type;

    /** The current transaction */
    protected WriteTransaction writeTransaction;


    /**
     * Starts a Read Only transaction. If the transaction is not closed, it will be
     * automatically closed after the timeout
     *
     * @return The created transaction
     */
    protected ReadTransaction<K, V> beginReadTransaction()
    {
        ReadTransaction<K, V> readTransaction = new ReadTransaction<K, V>( rootPage, btreeHeader.getRevision() - 1,
            System.currentTimeMillis() );

        readTransactions.add( readTransaction );

        return readTransaction;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browse() throws IOException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        ParentPos<K, V>[] stack = (ParentPos<K, V>[]) Array.newInstance( ParentPos.class, 32 );

        TupleCursor<K, V> cursor = rootPage.browse( transaction, stack, 0 );

        // Set the position before the first element
        cursor.beforeFirst();

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browse( long revision ) throws IOException, KeyNotFoundException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        ParentPos<K, V>[] stack = (ParentPos<K, V>[]) Array.newInstance( ParentPos.class, 32 );

        // And get the cursor
        TupleCursor<K, V> cursor = revisionRootPage.browse( transaction, stack, 0 );

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browseFrom( K key ) throws IOException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        ParentPos<K, V>[] stack = (ParentPos<K, V>[]) Array.newInstance( ParentPos.class, 32 );

        TupleCursor<K, V> cursor = rootPage.browse( key, transaction, stack, 0 );

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browseFrom( long revision, K key ) throws IOException, KeyNotFoundException
    {
        ReadTransaction<K, V> transaction = beginReadTransaction();

        // Fetch the rootPage for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        ParentPos<K, V>[] stack = (ParentPos<K, V>[]) Array.newInstance( ParentPos.class, 32 );

        // And get the cursor
        TupleCursor<K, V> cursor = revisionRootPage.browse( key, transaction, stack, 0 );

        return cursor;
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( K key, V value ) throws IOException
    {
        return rootPage.contains( key, value );
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( long revision, K key, V value ) throws IOException, KeyNotFoundException
    {
        // Fetch the root page for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        return revisionRootPage.contains( key, value );
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

        long revision = generateRevision();

        Tuple<K, V> deleted = delete( key, revision );

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

        long revision = generateRevision();

        Tuple<K, V> deleted = delete( key, value, revision );

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
        long revision = generateRevision();

        V existingValue = null;

        try
        {
            if ( writeTransaction == null )
            {
                writeLock.lock();
            }

            InsertResult<K, V> result = insert( key, value, revision );

            if ( result instanceof ModifyResult )
            {
                existingValue = ( ( ModifyResult<K, V> ) result ).getModifiedValue();
            }
        }
        finally
        {
            // See above
            if ( writeTransaction == null )
            {
                writeLock.unlock();
            }
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
        return rootPage.get( key );
    }


    /**
     * {@inheritDoc}
     */
    public V get( long revision, K key ) throws IOException, KeyNotFoundException
    {
        // Fetch the root page for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        return revisionRootPage.get( key );
    }


    /**
     * {@inheritDoc}
     */
    public Page<K, V> getRootPage()
    {
        return rootPage;
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setRootPage( Page<K, V> root )
    {
        rootPage = root;
    }


    /**
     * {@inheritDoc}
     */
    public ValueCursor<V> getValues( K key ) throws IOException, KeyNotFoundException
    {
        return rootPage.getValues( key );
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

        return rootPage.hasKey( key );
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

        // Fetch the root page for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        return revisionRootPage.hasKey( key );
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
        btreeHeader.setKeySerializerFQCN( keySerializer.getClass().getName() );
    }


    /**
     * {@inheritDoc}
     */
    public String getKeySerializerFQCN()
    {
        return btreeHeader.getKeySerializerFQCN();
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
        btreeHeader.setValueSerializerFQCN( valueSerializer.getClass().getName() );
    }


    /**
     * {@inheritDoc}
     */
    public String getValueSerializerFQCN()
    {
        return btreeHeader.getValueSerializerFQCN();
    }


    /**
     * {@inheritDoc}
     */
    public long getRevision()
    {
        return btreeHeader.getRevision();
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setRevision( long revision )
    {
        btreeHeader.setRevision( revision );
    }


    /**
     * Generates a new revision number. It's only used by the Page instances.
     *
     * @return a new incremental revision number
     */
    /* no qualifier */long generateRevision()
    {
        return btreeHeader.incrementRevision();
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
        return btreeHeader.getNbElems();
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setNbElems( long nbElems )
    {
        btreeHeader.setNbElems( nbElems );
    }


    /**
     * {@inheritDoc}
     */
    public int getPageSize()
    {
        return btreeHeader.getPageSize();
    }


    /**
     * {@inheritDoc}
     */
    public void setPageSize( int pageSize )
    {
        if ( pageSize <= 2 )
        {
            btreeHeader.setPageSize( DEFAULT_PAGE_SIZE );
        }
        else
        {
            btreeHeader.setPageSize( getPowerOf2( pageSize ) );
        }
    }


    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return btreeHeader.getName();
    }


    /**
     * {@inheritDoc}
     */
    public void setName( String name )
    {
        btreeHeader.setName( name );
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
        return btreeHeader.isAllowDuplicates();
    }


    /**
     * {@inheritDoc}
     */
    public void setAllowDuplicates( boolean allowDuplicates )
    {
        btreeHeader.setAllowDuplicates( allowDuplicates );
    }


    /**
     * {@inheritDoc}
     */
    public BTreeTypeEnum getType()
    {
        return type;
    }


    /**
     * @param type the type to set
     */
    public void setType( BTreeTypeEnum type )
    {
        this.type = type;
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
