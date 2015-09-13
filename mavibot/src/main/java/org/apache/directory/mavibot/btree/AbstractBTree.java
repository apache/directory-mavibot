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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.directory.mavibot.btree.exception.BTreeCreationException;
import org.apache.directory.mavibot.btree.exception.DuplicateValueNotAllowedException;
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

    /** The current Header for a managed BTree */
    protected BTreeHeader<K, V> currentBtreeHeader;

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
    protected AtomicBoolean transactionStarted = new AtomicBoolean( false );

    /** The map of all the used BtreeHeaders */
    protected Map<Long, BTreeHeader<K, V>> btreeRevisions = new ConcurrentHashMap<Long, BTreeHeader<K, V>>();

    /** The current revision */
    protected AtomicLong currentRevision = new AtomicLong( 0L );

    /** The TransactionManager used for this BTree */
    protected TransactionManager transactionManager;

    /** The size of the stack to use to manage tree searches */
    private final static int MAX_STACK_DEPTH = 32;


    /**
     * Starts a Read Only transaction. If the transaction is not closed, it will be
     * automatically closed after the timeout
     *
     * @return The created transaction
     */
    protected abstract ReadTransaction<K, V> beginReadTransaction();


    /**
     * Starts a Read Only transaction. If the transaction is not closed, it will be
     * automatically closed after the timeout
     *
     * @return The created transaction
     */
    protected abstract ReadTransaction<K, V> beginReadTransaction( long revision );


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browse() throws IOException, KeyNotFoundException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        if ( transaction == null )
        {
            return new EmptyTupleCursor<K, V>();
        }
        else
        {
            ParentPos<K, V>[] stack = ( ParentPos<K, V>[] ) Array.newInstance( ParentPos.class, MAX_STACK_DEPTH );

            TupleCursor<K, V> cursor = getRootPage().browse( transaction, stack, 0 );

            // Set the position before the first element
            cursor.beforeFirst();

            return cursor;
        }
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browse( long revision ) throws IOException, KeyNotFoundException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        if ( transaction == null )
        {
            return new EmptyTupleCursor<K, V>();
        }
        else
        {
            ParentPos<K, V>[] stack = ( ParentPos<K, V>[] ) Array.newInstance( ParentPos.class, MAX_STACK_DEPTH );

            // And get the cursor
            TupleCursor<K, V> cursor = getRootPage( transaction.getRevision() ).browse( transaction, stack, 0 );

            return cursor;
        }
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browseFrom( K key ) throws IOException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        ParentPos<K, V>[] stack = ( ParentPos<K, V>[] ) Array.newInstance( ParentPos.class, MAX_STACK_DEPTH );

        try
        {
            TupleCursor<K, V> cursor = getRootPage( transaction.getRevision() ).browse( key, transaction, stack, 0 );

            return cursor;
        }
        catch ( KeyNotFoundException e )
        {
            throw new IOException( e.getMessage() );
        }
    }


    /**
     * {@inheritDoc}
     */
    public TupleCursor<K, V> browseFrom( long revision, K key ) throws IOException, KeyNotFoundException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        if ( transaction == null )
        {
            return new EmptyTupleCursor<K, V>();
        }
        else
        {
            ParentPos<K, V>[] stack = ( ParentPos<K, V>[] ) Array.newInstance( ParentPos.class, MAX_STACK_DEPTH );

            // And get the cursor
            TupleCursor<K, V> cursor = getRootPage( transaction.getRevision() ).browse( key, transaction, stack, 0 );

            return cursor;
        }
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( K key, V value ) throws IOException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        if ( transaction == null )
        {
            return false;
        }
        else
        {
            try
            {
                return getRootPage( transaction.getRevision() ).contains( key, value );
            }
            catch ( KeyNotFoundException knfe )
            {
                throw new IOException( knfe.getMessage() );
            }
            finally
            {
                transaction.close();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( long revision, K key, V value ) throws IOException, KeyNotFoundException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        // Fetch the root page for this revision
        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        if ( transaction == null )
        {
            return false;
        }
        else
        {
            try
            {
                return getRootPage( transaction.getRevision() ).contains( key, value );
            }
            finally
            {
                transaction.close();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    public Tuple<K, V> delete( K key ) throws IOException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        // Take the lock if it's not already taken by another thread
        transactionManager.beginTransaction();

        try
        {
            Tuple<K, V> deleted = delete( key, currentRevision.get() + 1 );

            // Commit now
            transactionManager.commit();

            return deleted;
        }
        catch ( IOException ioe )
        {
            // We have had an exception, we must rollback the transaction
            transactionManager.rollback();

            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    public Tuple<K, V> delete( K key, V value ) throws IOException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        if ( value == null )
        {
            throw new IllegalArgumentException( "Value must not be null" );
        }

        transactionManager.beginTransaction();

        try
        {
            Tuple<K, V> deleted = delete( key, value, currentRevision.get() + 1 );

            transactionManager.commit();

            return deleted;
        }
        catch ( IOException ioe )
        {
            transactionManager.rollback();

            throw ioe;
        }
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
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        V existingValue = null;

        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        // Take the lock if it's not already taken by another thread and if we 
        // aren't on a sub-btree
        if ( btreeType != BTreeTypeEnum.PERSISTED_SUB )
        {
            transactionManager.beginTransaction();
        }

        try
        {
            InsertResult<K, V> result = insert( key, value, -1L );

            if ( result instanceof ExistsResult )
            {
                existingValue = value;
            }
            else if ( result instanceof ModifyResult )
            {
                existingValue = ( ( ModifyResult<K, V> ) result ).getModifiedValue();
            }

            // Commit now if it's not a sub-btree
            if ( btreeType != BTreeTypeEnum.PERSISTED_SUB )
            {
                //FIXME when result type is ExistsResult then we should avoid writing the headers
                transactionManager.commit();
            }

            return existingValue;
        }
        catch ( IOException ioe )
        {
            // We have had an exception, we must rollback the transaction
            // if it's not a sub-btree
            if ( btreeType != BTreeTypeEnum.PERSISTED_SUB )
            {
                transactionManager.rollback();
            }

            return null;
        }
        catch ( DuplicateValueNotAllowedException e )
        {
            // We have had an exception, we must rollback the transaction
            // if it's not a sub-btree
            if ( btreeType != BTreeTypeEnum.PERSISTED_SUB )
            {
                transactionManager.rollback();
            }

            throw e;
        }
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
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        if ( transaction == null )
        {
            return null;
        }
        else
        {
            try
            {
                return getRootPage( transaction.getRevision() ).get( key );
            }
            finally
            {
                transaction.close();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    public V get( long revision, K key ) throws IOException, KeyNotFoundException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        if ( transaction == null )
        {
            return null;
        }
        else
        {
            try
            {
                return getRootPage( transaction.getRevision() ).get( key );
            }
            finally
            {
                transaction.close();
            }
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
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        if ( transaction == null )
        {
            return new EmptyValueCursor<V>( 0L );
        }
        else
        {
            try
            {
                return getRootPage( transaction.getRevision() ).getValues( key );
            }
            finally
            {
                transaction.close();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    public boolean hasKey( K key ) throws IOException, KeyNotFoundException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        if ( key == null )
        {
            return false;
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        if ( transaction == null )
        {
            return false;
        }
        else
        {
            try
            {
                return getRootPage( transaction.getRevision() ).hasKey( key );
            }
            finally
            {
                transaction.close();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    public boolean hasKey( long revision, K key ) throws IOException, KeyNotFoundException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        if ( key == null )
        {
            return false;
        }

        ReadTransaction<K, V> transaction = beginReadTransaction( revision );

        if ( transaction == null )
        {
            return false;
        }
        else
        {
            try
            {
                return getRootPage( transaction.getRevision() ).hasKey( key );
            }
            finally
            {
                transaction.close();
            }
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
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        if ( transaction == null )
        {
            return -1L;
        }
        else
        {
            try
            {
                return transaction.getRevision();
            }
            finally
            {
                transaction.close();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setRevision( long revision )
    {
        transactionManager.getBTreeHeader( getName() ).setRevision( revision );
    }


    /**
     * Store the new revision in the map of btrees, increment the current revision
     */
    protected void storeRevision( BTreeHeader<K, V> btreeHeader, boolean keepRevisions )
    {
        long revision = btreeHeader.getRevision();

        if ( keepRevisions )
        {
            synchronized ( btreeRevisions )
            {
                btreeRevisions.put( revision, btreeHeader );
            }
        }

        currentRevision.set( revision );
        currentBtreeHeader = btreeHeader;

        // And update the newBTreeHeaders map
        if ( btreeHeader.getBtree().getType() != BTreeTypeEnum.PERSISTED_SUB )
        {
            transactionManager.updateNewBTreeHeaders( btreeHeader );
        }
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
        currentBtreeHeader = btreeHeader;

        // And update the newBTreeHeaders map
        if ( btreeHeader.getBtree().getType() != BTreeTypeEnum.PERSISTED_SUB )
        {
            transactionManager.updateNewBTreeHeaders( btreeHeader );
        }
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
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ReadTransaction<K, V> transaction = beginReadTransaction();

        if ( transaction == null )
        {
            return -1L;
        }
        else
        {
            try
            {
                return transaction.getBtreeHeader().getNbElems();
            }
            finally
            {
                transaction.close();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setNbElems( long nbElems )
    {
        transactionManager.getBTreeHeader( getName() ).setNbElems( nbElems );
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
    public Comparator<K> getKeyComparator()
    {
        return keySerializer.getComparator();
    }


    /**
     * {@inheritDoc}
     */
    public Comparator<V> getValueComparator()
    {
        return valueSerializer.getComparator();
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
        return currentBtreeHeader;
    }


    /**
     * @return The current BtreeHeader
     */
    protected BTreeHeader<K, V> getBtreeHeader( long revision )
    {
        return btreeRevisions.get( revision );
    }


    /**
     * {@inheritDoc}
     */
    public KeyCursor<K> browseKeys() throws IOException, KeyNotFoundException
    {
        // Check that we have a TransactionManager
        if ( transactionManager == null )
        {
            throw new BTreeCreationException( "We don't have a Transaction Manager" );
        }

        ReadTransaction transaction = beginReadTransaction();

        if ( transaction == null )
        {
            return new KeyCursor<K>();
        }
        else
        {
            ParentPos<K, K>[] stack = ( ParentPos<K, K>[] ) Array.newInstance( ParentPos.class, MAX_STACK_DEPTH );

            KeyCursor<K> cursor = getRootPage().browseKeys( transaction, stack, 0 );

            // Set the position before the first element
            cursor.beforeFirst();

            return cursor;
        }
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
