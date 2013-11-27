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
package org.apache.directory.mavibot.btree.managed;


import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.ehcache.Cache;
import net.sf.ehcache.config.CacheConfiguration;

import org.apache.directory.mavibot.btree.BTreeHeader;
import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.TupleCursor;
import org.apache.directory.mavibot.btree.ValueCursor;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The B+Tree MVCC data structure.
 * 
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BTree<K, V> implements Closeable
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( BTree.class );

    /** The Header for a managed BTree */
    private BTreeHeader btreeHeader;

    /** Default page size (number of entries per node) */
    public static final int DEFAULT_PAGE_SIZE = 16;

    /** Default size of the buffer used to write data on disk. Around 1Mb */
    public static final int DEFAULT_WRITE_BUFFER_SIZE = 4096 * 250;

    /** The default journal name */
    public static final String DEFAULT_JOURNAL = "mavibot.log";

    /** The default data file suffix */
    public static final String DATA_SUFFIX = ".db";

    /** The default journal file suffix */
    public static final String JOURNAL_SUFFIX = ".log";

    /** The current rootPage */
    protected volatile Page<K, V> rootPage;

    /** The list of read transactions being executed */
    private ConcurrentLinkedQueue<Transaction<K, V>> readTransactions;

    /** The size of the buffer used to write data in disk */
    private int writeBufferSize;

    /** The Key serializer used for this tree.*/
    private ElementSerializer<K> keySerializer;

    /** The Value serializer used for this tree. */
    private ElementSerializer<V> valueSerializer;

    /** The RecordManager if the BTree is managed */
    private RecordManager recordManager;

    /** A lock used to protect the write operation against concurrent access */
    private ReentrantLock writeLock;

    /** The thread responsible for the cleanup of timed out reads */
    private Thread readTransactionsThread;

    /** Define a default delay for a read transaction. This is 10 seconds */
    public static final long DEFAULT_READ_TIMEOUT = 10 * 1000L;

    /** The read transaction timeout */
    private long readTimeOut = DEFAULT_READ_TIMEOUT;

    /** The cache associated with this BTree */
    private Cache cache;

    /** The cache size, default to 1000 elements */
    private int cacheSize = DEFAULT_CACHE_SIZE;

    /** The default number of pages to keep in memory */
    private static final int DEFAULT_CACHE_SIZE = 1000;
    
    /** A flag indicating if this BTree is a Sub BTree */
    private boolean isSubBtree = false;

    /** The number of stored Values before we switch to a BTree */
    private static final int DEFAULT_VALUE_THRESHOLD_UP = 8;

    /** The number of stored Values before we switch back to an array */
    private static final int DEFAULT_VALUE_THRESHOLD_LOW = 1;

    /** The configuration for the array <-> BTree switch */
    /*No qualifier*/static int valueThresholdUp = DEFAULT_VALUE_THRESHOLD_UP;
    /*No qualifier*/static int valueThresholdLow = DEFAULT_VALUE_THRESHOLD_LOW;

    /**
     * Create a thread that is responsible of cleaning the transactions when
     * they hit the timeout
     */
    private void createTransactionManager()
    {
        Runnable readTransactionTask = new Runnable()
        {
            public void run()
            {
                try
                {
                    Transaction<K, V> transaction = null;

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


    /**
     * Creates a new BTree, with no initialization. 
     */
    public BTree()
    {
        btreeHeader = new BTreeHeader();
    }


    /**
     * Creates a new in-memory BTree using the BTreeConfiguration to initialize the 
     * BTree
     * 
     * @param comparator The comparator to use
     */
    public BTree( BTreeConfiguration<K, V> configuration ) throws IOException
    {
        String name = configuration.getName();

        if ( name == null )
        {
            throw new IllegalArgumentException( "BTree name cannot be null" );
        }

        btreeHeader = new BTreeHeader();
        btreeHeader.setName( name );
        btreeHeader.setPageSize( configuration.getPageSize() );
        isSubBtree = configuration.isSubBtree();

        keySerializer = configuration.getKeySerializer();
        btreeHeader.setKeySerializerFQCN( keySerializer.getClass().getName() );

        valueSerializer = configuration.getValueSerializer();
        btreeHeader.setValueSerializerFQCN( valueSerializer.getClass().getName() );

        readTimeOut = configuration.getReadTimeOut();
        writeBufferSize = configuration.getWriteBufferSize();
        btreeHeader.setAllowDuplicates( configuration.isAllowDuplicates() );
        cacheSize = configuration.getCacheSize();

        if ( keySerializer.getComparator() == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }

        // Create the first root page, with revision 0L. It will be empty
        // and increment the revision at the same time
        rootPage = new Leaf<K, V>( this );
        
        if ( isSubBtree )
        {
            // The subBTree inherit its cache from its parent BTree
            this.cache = configuration.getParentBTree().getCache();
            this.writeLock = configuration.getParentBTree().getWriteLock();
            readTransactions = new ConcurrentLinkedQueue<Transaction<K, V>>();
        }

        // Now, initialize the BTree
        init();
    }


    /**
     * Creates a new in-memory BTree with a default page size and key/value serializers.
     * 
     * @param comparator The comparator to use
     */
    public BTree( String name, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer )
        throws IOException
    {
        this( name, keySerializer, valueSerializer, false );
    }


    public BTree( String name, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer,
        boolean allowDuplicates )
        throws IOException
    {
        this( name, null, keySerializer, valueSerializer, DEFAULT_PAGE_SIZE, allowDuplicates, DEFAULT_CACHE_SIZE );
    }


    public BTree( String name, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer,
        boolean allowDuplicates, int cacheSize )
        throws IOException
    {
        this( name, null, keySerializer, valueSerializer, DEFAULT_PAGE_SIZE, allowDuplicates, cacheSize );
    }


    /**
     * Creates a new in-memory BTree with a default page size and key/value serializers.
     * 
     * @param comparator The comparator to use
     */
    public BTree( String name, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer, int pageSize )
        throws IOException
    {
        this( name, null, keySerializer, valueSerializer, pageSize );
    }


    /**
     * Creates a new BTree with a default page size and a comparator, with an associated file.
     * @param comparator The comparator to use
     * @param serializer The serializer to use
     */
    public BTree( String name, String path, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer )
        throws IOException
    {
        this( name, path, keySerializer, valueSerializer, DEFAULT_PAGE_SIZE );
    }


    /**
     * 
     * Creates a new instance of BTree with the given name and store it under the given dataDir if provided.
     *
     * @param name the name of the BTree
     * @param dataDir the name of the data directory with absolute path
     * @param keySerializer key serializer
     * @param valueSerializer value serializer
     * @param pageSize size of the page
     * @throws IOException
     */
    public BTree( String name, String dataDir, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer,
        int pageSize )
        throws IOException
    {
        this( name, dataDir, keySerializer, valueSerializer, pageSize, false, DEFAULT_CACHE_SIZE );
    }


    public BTree( String name, String dataDir, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer,
        int pageSize, boolean allowDuplicates )
        throws IOException
    {
        this( name, dataDir, keySerializer, valueSerializer, pageSize, allowDuplicates, DEFAULT_CACHE_SIZE );
    }


    public BTree( String name, String dataDir, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer,
        int pageSize, boolean allowDuplicates, int cacheSize )
        throws IOException
    {
        btreeHeader = new BTreeHeader();
        btreeHeader.setName( name );

        setPageSize( pageSize );
        writeBufferSize = DEFAULT_WRITE_BUFFER_SIZE;

        this.cacheSize = cacheSize;

        this.keySerializer = keySerializer;

        btreeHeader.setKeySerializerFQCN( keySerializer.getClass().getName() );

        this.valueSerializer = valueSerializer;

        btreeHeader.setValueSerializerFQCN( valueSerializer.getClass().getName() );

        btreeHeader.setAllowDuplicates( allowDuplicates );

        // Create the first root page, with revision 0L. It will be empty
        // and increment the revision at the same time
        rootPage = new Leaf<K, V>( this );

        // Now, call the init() method
        init();
    }


    /**
     * Initialize the BTree.
     * 
     * @throws IOException If we get some exception while initializing the BTree
     */
    public void init() throws IOException
    {
        if ( !isSubBtree )
        {
            // This is not a subBtree, we have to initialize the cache

            // Create the queue containing the pending read transactions
            readTransactions = new ConcurrentLinkedQueue<Transaction<K, V>>();
    
            writeLock = new ReentrantLock();
    
            // Initialize the caches
            CacheConfiguration cacheConfiguration = new CacheConfiguration();
            cacheConfiguration.setName( "pages" );
            cacheConfiguration.setEternal( true );
            cacheConfiguration.setOverflowToDisk( false );
            cacheConfiguration.setCacheLoaderTimeoutMillis( 0 );
            cacheConfiguration.setMaxElementsInMemory( cacheSize );
            cacheConfiguration.setMemoryStoreEvictionPolicy( "LRU" );
    
            cache = new Cache( cacheConfiguration );
            cache.initialise();
        }

        // Initialize the txnManager thread
        //FIXME we should NOT create a new transaction manager thread for each BTree
        //createTransactionManager();
    }


    /**
     * Return the cache we use in this BTree
     */
    /* No qualifier */Cache getCache()
    {
        return cache;
    }


    /**
     * Return the cache we use in this BTree
     */
    /* No qualifier */ReentrantLock getWriteLock()
    {
        return writeLock;
    }


    /**
     * Return the cache we use in this BTree
     */
    /* No qualifier */ConcurrentLinkedQueue<Transaction<K, V>> getReadTransactions()
    {
        return readTransactions;
    }


    /**
     * Close the BTree, cleaning up all the data structure
     */
    public void close() throws IOException
    {
        // Stop the readTransaction thread
        // readTransactionsThread.interrupt();
        // readTransactions.clear();

        rootPage = null;
    }


    /**
     * @return the btreeOffset
     */
    /* No qualifier*/long getBtreeOffset()
    {
        return btreeHeader.getBTreeOffset();
    }


    /**
     * @param btreeOffset the btreeOffset to set
     */
    /* No qualifier*/void setBtreeOffset( long btreeOffset )
    {
        btreeHeader.setBTreeOffset( btreeOffset );
    }


    /**
     * @return the rootPageOffset
     */
    /* No qualifier*/long getRootPageOffset()
    {
        return btreeHeader.getRootPageOffset();
    }


    /**
     * @param rootPageOffset the rootPageOffset to set
     */
    /* No qualifier*/void setRootPageOffset( long rootPageOffset )
    {
        btreeHeader.setRootPageOffset( rootPageOffset );
    }


    /**
     * @return the nextBTreeOffset
     */
    /* No qualifier*/long getNextBTreeOffset()
    {
        return btreeHeader.getNextBTreeOffset();
    }


    /**
     * @param nextBTreeOffset the nextBTreeOffset to set
     */
    /* No qualifier*/void setNextBTreeOffset( long nextBTreeOffset )
    {
        btreeHeader.setNextBTreeOffset( nextBTreeOffset );
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
     * Set the maximum number of elements we can store in a page. This must be a
     * number greater than 1, and a power of 2. The default page size is 16.
     * <br/>
     * If the provided size is below 2, we will default to DEFAULT_PAGE_SIZE.<br/>
     * If the provided size is not a power of 2, we will select the closest power of 2
     * higher than the given number<br/>
     * 
     * @param pageSize The requested page size
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
     * Set the new root page for this tree. Used for debug purpose only. The revision
     * will always be 0;
     * 
     * @param root the new root page.
     */
    /* No qualifier */void setRoot( Page<K, V> root )
    {
        rootPage = root;
    }


    /**
     * Gets the RecordManager for a managed BTree
     * 
     * @return The recordManager if the BTree is managed
     */
    /* No qualifier */RecordManager getRecordManager()
    {
        return recordManager;
    }


    /**
     * Inject a RecordManager for a managed BTree
     * 
     * @param recordManager The injected RecordManager
     */
    /* No qualifier */void setRecordManager( RecordManager recordManager )
    {
        this.recordManager = recordManager;
    }


    /**
     * @return the pageSize
     */
    public int getPageSize()
    {
        return btreeHeader.getPageSize();
    }


    /**
     * Generates a new revision number. It's only used by the Page instances.
     * 
     * @return a new incremental revision number
     */
    /** No qualifier */
    long generateRevision()
    {
        return btreeHeader.incrementRevision();
    }


    /**
     * Insert an entry in the BTree.
     * <p>
     * We will replace the value if the provided key already exists in the
     * btree.
     *
     * @param key Inserted key
     * @param value Inserted value
     * @return Existing value, if any.
     * @throws IOException TODO
     */
    public V insert( K key, V value ) throws IOException
    {
        long revision = generateRevision();

        V existingValue = null;

        try
        {
            // Commented atm, we will have to play around the idea of transactions later
            writeLock.lock();

            InsertResult<K, V> result = insert( key, value, revision );

            if ( result instanceof ModifyResult )
            {
                existingValue = ( ( ModifyResult<K, V> ) result ).getModifiedValue();
            }
        }
        finally
        {
            // See above
            writeLock.unlock();
        }

        return existingValue;
    }


    /**
     * Delete the entry which key is given as a parameter. If the entry exists, it will
     * be removed from the tree, the old tuple will be returned. Otherwise, null is returned.
     * 
     * @param key The key for the entry we try to remove
     * @return A Tuple<K, V> containing the removed entry, or null if it's not found.
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
     * Delete the value from an entry associated with the given key. If the value
     * If the value is present, it will be deleted first, later if there are no other 
     * values associated with this key(which can happen when duplicates are enabled), 
     * we will remove the key from the tree.
     * 
     * @param key The key for the entry we try to remove
     * @param value The value to delete (can be null)
     * @return A Tuple<K, V> containing the removed entry, or null if it's not found.
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
    private Tuple<K, V> delete( K key, long revision ) throws IOException
    {
        return delete( key, null, revision );
    }


    /**
     * 
     * Deletes the given <key,value> pair if both key and value match. If the given value is null
     * and there is no null value associated with the given key then the entry with the given key
     * will be removed.
     *
     * @param key The key to be removed 
     * @param value The value to be removed (can be null, and when no null value exists the key will be removed irrespective of the value)
     * @param revision The revision to be associated with this operation
     * @return
     * @throws IOException
     */
    private Tuple<K, V> delete( K key, V value, long revision ) throws IOException
    {
        writeLock.lock();

        try
        {
            // If the key exists, the existing value will be replaced. We store it
            // to return it to the caller.
            Tuple<K, V> tuple = null;

            // Try to delete the entry starting from the root page. Here, the root
            // page may be either a Node or a Leaf
            DeleteResult<K, V> result = rootPage.delete( revision, key, value, null, -1 );

            if ( result instanceof NotPresentResult )
            {
                // Key not found.
                return null;
            }

            // Keep the oldRootPage so that we can later access it
            Page<K, V> oldRootPage = rootPage;

            if ( result instanceof RemoveResult )
            {
                // The element was found, and removed
                RemoveResult<K, V> removeResult = ( RemoveResult<K, V> ) result;

                Page<K, V> modifiedPage = removeResult.getModifiedPage();

                // Write the modified page on disk
                // Note that we don't use the holder, the new root page will
                // remain in memory.
                ElementHolder<Page<K, V>, K, V> holder = recordManager.writePage( this, modifiedPage,
                    revision );

                // Store the offset on disk in the page in memory
                ( ( AbstractPage<K, V> ) modifiedPage ).setOffset( ( ( PageHolder<K, V> ) holder )
                    .getOffset() );

                // Store the last offset on disk in the page in memory
                ( ( AbstractPage<K, V> ) modifiedPage )
                    .setLastOffset( ( ( PageHolder<K, V> ) holder )
                        .getLastOffset() );

                // This is a new root
                rootPage = modifiedPage;
                tuple = removeResult.getRemovedElement();
            }

            // Decrease the number of elements in the current tree if the deletion is successful
            if ( tuple != null )
            {
                btreeHeader.decrementNbElems();

                // We have to update the rootPage on disk
                // Update the BTree header now
                recordManager.updateBtreeHeader( this, ( ( AbstractPage<K, V> ) rootPage ).getOffset() );
            }

            recordManager.addFreePages( this, result.getCopiedPages() );

            // Store the created rootPage into the revision BTree, this will be stored in RecordManager only if revisions are set to keep
            recordManager.storeRootPage( this, rootPage );

            // Return the value we have found if it was modified
            return tuple;
        }
        finally
        {
            // See above
            writeLock.unlock();
        }
    }


    /**
     * Find a value in the tree, given its key. If the key is not found,
     * it will throw a KeyNotFoundException. <br/>
     * Note that we can get a null value stored, or many values.
     * 
     * @param key The key we are looking at
     * @return The found value, or null if the key is not present in the tree
     * @throws KeyNotFoundException If the key is not found in the BTree
     * @throws IOException TODO
     */
    public V get( K key ) throws IOException, KeyNotFoundException
    {
        return rootPage.get( key );
    }


    /**
     * @see Page#getValues(Object)
     */
    public ValueCursor<V> getValues( K key ) throws IOException, KeyNotFoundException
    {
        return rootPage.getValues( key );
    }


    /**
     * Find a value in the tree, given its key, at a specific revision. If the key is not found,
     * it will throw a KeyNotFoundException. <br/>
     * Note that we can get a null value stored, or many values.
     * 
     * @param revision The revision for which we want to find a key
     * @param key The key we are looking at
     * @return The found value, or null if the key is not present in the tree
     * @throws KeyNotFoundException If the key is not found in the BTree
     * @throws IOException If there was an issue while fetching data from the disk
     */
    public V get( long revision, K key ) throws IOException, KeyNotFoundException
    {
        // Fetch the root page for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        return revisionRootPage.get( key );
    }


    /**
     * Checks if the given key exists.
     *  
     * @param key The key we are looking at
     * @return true if the key is present, false otherwise
     * @throws IOException If we have an error while trying to access the page
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
     * Checks if the given key exists for a given revision.
     *  
     * @param revision The revision for which we want to find a key
     * @param key The key we are looking at
     * @return true if the key is present, false otherwise
     * @throws IOException If we have an error while trying to access the page
     * @throws KeyNotFoundException If the key is not found in the BTree
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
     * Checks if the BTree contains the given key with the given value.
     * 
     * @param key The key we are looking for
     * @param value The value associated with the given key
     * @return true if the key and value are associated with each other, false otherwise
     */
    public boolean contains( K key, V value ) throws IOException
    {
        return rootPage.contains( key, value );
    }


    /**
     * Checks if the BTree contains the given key with the given value for a given revision
     * 
     * @param revision The revision we would like to browse
     * @param key The key we are looking for
     * @param value The value associated with the given key
     * @return true if the key and value are associated with each other, false otherwise
     * @throws KeyNotFoundException If the key is not found in the BTree
     */
    public boolean contains( long revision, K key, V value ) throws IOException, KeyNotFoundException
    {
        // Fetch the root page for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        return revisionRootPage.contains( key, value );
    }


    /**
     * Creates a cursor starting at the beginning of the tree
     * 
     * @return A cursor on the btree
     * @throws IOException
     */
    public TupleCursor<K, V> browse() throws IOException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        TupleCursor<K, V> cursor = rootPage.browse( transaction, new ParentPos[32], 0 );

        return cursor;
    }


    /**
     * Creates a cursor starting at the beginning of the tree, for a given revision
     * 
     * @param revision The revision we would like to browse
     * @return A cursor on the btree
     * @throws IOException If we had an issue while fetching data from the disk
     * @throws KeyNotFoundException If the key is not found in the BTree
     */
    public TupleCursor<K, V> browse( long revision ) throws IOException, KeyNotFoundException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        // And get the cursor
        TupleCursor<K, V> cursor = revisionRootPage.browse( transaction, new ParentPos[32], 0 );

        return cursor;
    }


    /**
     * Creates a cursor starting on the given key
     * 
     * @param key The key which is the starting point. If the key is not found,
     * then the cursor will always return null.
     * @return A cursor on the btree
     * @throws IOException
     */
    public TupleCursor<K, V> browseFrom( K key ) throws IOException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        @SuppressWarnings("unchecked")
        ParentPos<K, V>[] stack = new ParentPos[32];
        TupleCursor<K, V> cursor = rootPage.browse( key, transaction, stack, 0 );

        return cursor;
    }


    /**
     * Creates a cursor starting on the given key at the given revision
     * 
     * @param The revision we are looking for
     * @param key The key which is the starting point. If the key is not found,
     * then the cursor will always return null.
     * @return A cursor on the btree
     * @throws IOException If wxe had an issue reading the BTree from disk
     * @throws KeyNotFoundException  If we can't find a rootPage for this revision
     */
    public TupleCursor<K, V> browseFrom( long revision, K key ) throws IOException, KeyNotFoundException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the rootPage for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        // And get the cursor
        TupleCursor<K, V> cursor = revisionRootPage.browse( key, transaction, new ParentPos[32], 0 );

        return cursor;
    }


    /**
     * Insert an entry in the BTree.
     * <p>
     * We will replace the value if the provided key already exists in the
     * btree.
     * <p>
     * The revision number is the revision to use to insert the data.
     *
     * @param key Inserted key
     * @param value Inserted value
     * @param revision The revision to use
     * @return an instance of the InsertResult.
     */
    /*No qualifier*/InsertResult<K, V> insert( K key, V value, long revision ) throws IOException
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        // If the key exists, the existing value will be replaced. We store it
        // to return it to the caller.
        V modifiedValue = null;

        // Try to insert the new value in the tree at the right place,
        // starting from the root page. Here, the root page may be either
        // a Node or a Leaf
        InsertResult<K, V> result = rootPage.insert( revision, key, value );

        if ( result instanceof ModifyResult )
        {
            ModifyResult<K, V> modifyResult = ( ( ModifyResult<K, V> ) result );

            Page<K, V> modifiedPage = modifyResult.getModifiedPage();

            // Write the modified page on disk
            // Note that we don't use the holder, the new root page will
            // remain in memory.
            ElementHolder<Page<K, V>, K, V> holder = recordManager.writePage( this, modifiedPage,
                revision );
            
            // The root has just been modified, we haven't split it
            // Get it and make it the current root page
            rootPage = modifiedPage;

            modifiedValue = modifyResult.getModifiedValue();
        }
        else
        {
            // We have split the old root, create a new one containing
            // only the pivotal we got back
            SplitResult<K, V> splitResult = ( ( SplitResult<K, V> ) result );

            K pivot = splitResult.getPivot();
            Page<K, V> leftPage = splitResult.getLeftPage();
            Page<K, V> rightPage = splitResult.getRightPage();
            Page<K, V> newRootPage = null;

            // If the BTree is managed, we have to write the two pages that were created
            // and to keep a track of the two offsets for the upper node
            ElementHolder<Page<K, V>, K, V> holderLeft = recordManager.writePage( this,
                leftPage, revision );

            ElementHolder<Page<K, V>, K, V> holderRight = recordManager.writePage( this,
                rightPage, revision );

            // Create the new rootPage
            newRootPage = new Node<K, V>( this, revision, pivot, holderLeft, holderRight );

            // If the BTree is managed, we now have to write the page on disk
            // and to add this page to the list of modified pages
            ElementHolder<Page<K, V>, K, V> holder = recordManager
                .writePage( this, newRootPage, revision );

            rootPage = newRootPage;
        }

        // Increase the number of element in the current tree if the insertion is successful
        // and does not replace an element
        if ( modifiedValue == null )
        {
            btreeHeader.incrementNbElems();
        }

        // If the BTree is managed, we have to update the rootPage on disk
        // Update the BTree header now
        recordManager.updateBtreeHeader( this, ( ( AbstractPage<K, V> ) rootPage ).getOffset() );

        // Moved the free pages into the list of free pages
        recordManager.addFreePages( this, result.getCopiedPages() );

        // Store the created rootPage into the revision BTree, this will be stored in RecordManager only if revisions are set to keep
        recordManager.storeRootPage( this, rootPage );

        // Return the value we have found if it was modified
        return result;
    }


    /**
     * Starts a Read Only transaction. If the transaction is not closed, it will be 
     * automatically closed after the timeout
     * @return The created transaction
     */
    private Transaction<K, V> beginReadTransaction()
    {
        Transaction<K, V> readTransaction = new Transaction<K, V>( rootPage, btreeHeader.getRevision() - 1,
            System.currentTimeMillis() );

        readTransactions.add( readTransaction );

        return readTransaction;
    }


    /**
     * @return the comparator
     */
    public Comparator<K> getComparator()
    {
        return keySerializer.getComparator();
    }


    /**
     * @param keySerializer the Key serializer to set
     */
    public void setKeySerializer( ElementSerializer<K> keySerializer )
    {
        this.keySerializer = keySerializer;
        btreeHeader.setKeySerializerFQCN( keySerializer.getClass().getName() );
    }


    /**
     * @param valueSerializer the Value serializer to set
     */
    public void setValueSerializer( ElementSerializer<V> valueSerializer )
    {
        this.valueSerializer = valueSerializer;
        btreeHeader.setValueSerializerFQCN( valueSerializer.getClass().getName() );
    }


    /**
     * Write the data in the ByteBuffer, and eventually on disk if needed.
     * 
     * @param channel The channel we want to write to
     * @param bb The ByteBuffer we want to feed
     * @param buffer The data to inject
     * @throws IOException If the write failed
     */
    private void writeBuffer( FileChannel channel, ByteBuffer bb, byte[] buffer ) throws IOException
    {
        int size = buffer.length;
        int pos = 0;

        // Loop until we have written all the data
        do
        {
            if ( bb.remaining() >= size )
            {
                // No flush, as the ByteBuffer is big enough
                bb.put( buffer, pos, size );
                size = 0;
            }
            else
            {
                // Flush the data on disk, reinitialize the ByteBuffer
                int len = bb.remaining();
                size -= len;
                bb.put( buffer, pos, len );
                pos += len;

                bb.flip();

                channel.write( bb );

                bb.clear();
            }
        }
        while ( size > 0 );
    }


    /**
     * Get the rootPzge associated to a give revision.
     * 
     * @param revision The revision we are looking for
     * @return The rootPage associated to this revision
     * @throws IOException If we had an issue while accessing the underlying file
     * @throws KeyNotFoundException If the revision does not exist for this Btree
     */
    private Page<K, V> getRootPage( long revision ) throws IOException, KeyNotFoundException
    {
        return recordManager.getRootPage( this, revision );
    }


    /**
     * Flush the latest revision to disk. We will replace the current file by the new one, as
     * we flush in a temporary file.
     */
    public void flush() throws IOException
    {
    }


    /**
     * @return the readTimeOut
     */
    public long getReadTimeOut()
    {
        return readTimeOut;
    }


    /**
     * @param readTimeOut the readTimeOut to set
     */
    public void setReadTimeOut( long readTimeOut )
    {
        this.readTimeOut = readTimeOut;
    }


    /**
     * @return the name
     */
    public String getName()
    {
        return btreeHeader.getName();
    }


    /**
     * @param name the name to set
     */
    public void setName( String name )
    {
        btreeHeader.setName( name );
    }


    /**
     * @return the writeBufferSize
     */
    public int getWriteBufferSize()
    {
        return writeBufferSize;
    }


    /**
     * @param writeBufferSize the writeBufferSize to set
     */
    public void setWriteBufferSize( int writeBufferSize )
    {
        this.writeBufferSize = writeBufferSize;
    }


    /**
     * Create a ValueHolder depending on the kind of holder we want.
     * 
     * @param value The value to store
     * @return The value holder
     */
    @SuppressWarnings("unchecked")
    /* no qualifier */ValueHolder<V> createValueHolder( V value )
    {
        return new ValueHolder<V>( this, valueSerializer, value );
    }


    /**
     * Create a ValueHolder depending on the kind of holder we want.
     * 
     * @param value The value to store
     * @return The value holder
     */
    /* no qualifier */ElementHolder<Page<K, V>, K, V> createPageHolder( Page<K, V> value )
    {
        return new PageHolder<K, V>( this, value,
            value.getOffset(), value.getLastOffset() );
    }


    /**
     * @return the keySerializer
     */
    public ElementSerializer<K> getKeySerializer()
    {
        return keySerializer;
    }


    /**
     * @return the keySerializer FQCN
     */
    public String getKeySerializerFQCN()
    {
        return btreeHeader.getKeySerializerFQCN();
    }


    /**
     * @return the valueSerializer
     */
    public ElementSerializer<V> getValueSerializer()
    {
        return valueSerializer;
    }


    /**
     * @return the valueSerializer FQCN
     */
    public String getValueSerializerFQCN()
    {
        return btreeHeader.getValueSerializerFQCN();
    }


    /** 
     * @return The current BTree revision
     */
    public long getRevision()
    {
        return btreeHeader.getRevision();
    }


    /**
     * @param revision the revision to set
     */
    /* No qualifier */void setRevision( long revision )
    {
        btreeHeader.setRevision( revision );
    }


    /** 
     * @return The current number of elements in the BTree
     */
    public long getNbElems()
    {
        return btreeHeader.getNbElems();
    }


    /**
     * @param nbElems the nbElems to set
     */
    /* No qualifier */void setNbElems( long nbElems )
    {
        btreeHeader.setNbElems( nbElems );
    }


    /**
     * @return true if this BTree allow duplicate values
     */
    public boolean isAllowDuplicates()
    {
        return btreeHeader.isAllowDuplicates();
    }


    /* No qualifier */void setAllowDuplicates( boolean allowDuplicates )
    {
        btreeHeader.setAllowDuplicates( allowDuplicates );
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Managed BTree" );
        sb.append( "[" ).append( btreeHeader.getName() ).append( "]" );
        sb.append( "( pageSize:" ).append( btreeHeader.getPageSize() );

        if ( rootPage != null )
        {
            sb.append( ", nbEntries:" ).append( btreeHeader.getNbElems() );
        }
        else
        {
            sb.append( ", nbEntries:" ).append( 0 );
        }

        sb.append( ", comparator:" );

        if ( keySerializer.getComparator() == null )
        {
            sb.append( "null" );
        }
        else
        {
            sb.append( keySerializer.getComparator().getClass().getSimpleName() );
        }

        sb.append( ", DuplicatesAllowed: " ).append( btreeHeader.isAllowDuplicates() );

        sb.append( ") : \n" );
        sb.append( rootPage.dumpPage( "" ) );

        return sb.toString();
    }
}
