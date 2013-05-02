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


import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.mavibot.btree.exception.KeyNotFoundException;
import org.apache.mavibot.btree.serializer.BufferHandler;
import org.apache.mavibot.btree.serializer.ElementSerializer;
import org.apache.mavibot.btree.serializer.LongSerializer;


/**
 * The B+Tree MVCC data structure.
 * 
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 *
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class BTree<K, V>
{
    /** The Hader for a managed BTree */
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

    /** Comparator used to index entries. */
    private Comparator<K> comparator;

    /** The current rootPage */
    protected volatile Page<K, V> rootPage;

    /** The list of read transactions being executed */
    private ConcurrentLinkedQueue<Transaction<K, V>> readTransactions;

    /** The size of the buffer used to write data in disk */
    private int writeBufferSize;

    /** The type to use to create the keys */
    protected Class<?> keyType;

    /** The Key serializer used for this tree.*/
    private ElementSerializer<K> keySerializer;

    /** The Value serializer used for this tree. */
    private ElementSerializer<V> valueSerializer;

    /** The associated file. If null, this is an in-memory btree  */
    private File file;

    /** The RecordManager if the BTree is managed */
    private RecordManager recordManager;

    /** The BTree type : either in-memory, persistent or managed */
    private BTreeTypeEnum type;

    /** A flag used to tell the BTree that the journal is activated */
    private boolean withJournal;

    /** The associated journal. If null, this is an in-memory btree  */
    private File journal;

    /** A lock used to protect the write operation against concurrent access */
    private ReentrantLock writeLock;

    /** The thread responsible for the cleanup of timed out reads */
    private Thread readTransactionsThread;

    /** The thread responsible for the journal updates */
    private Thread journalManagerThread;

    /** Define a default delay for a read transaction. This is 10 seconds */
    public static final long DEFAULT_READ_TIMEOUT = 10 * 1000L;

    /** The read transaction timeout */
    private long readTimeOut = DEFAULT_READ_TIMEOUT;

    /** The queue containing all the modifications applied on the bTree */
    private BlockingQueue<Modification<K, V>> modificationsQueue;

    /** Flag to enable duplicate key support */
    private boolean allowDuplicates;

    /** A flag set to true if we want to keep old revisions */
    private boolean keepRevisions = false;

    private File envDir;

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
     * Create a thread that is responsible of writing the modifications in a journal.
     * The journal will contain all the modifications in the order they have been applied
     * to the BTree. We will store Insertions and Deletions. Those operations are injected
     * into a queue, which is read by the thread.
     */
    private void createJournalManager()
    {
        Runnable journalTask = new Runnable()
        {
            private boolean flushModification( FileChannel channel, Modification<K, V> modification )
                throws IOException
            {
                if ( modification instanceof Addition )
                {
                    byte[] keyBuffer = keySerializer.serialize( modification.getKey() );
                    ByteBuffer bb = ByteBuffer.allocateDirect( keyBuffer.length + 1 );
                    bb.put( Modification.ADDITION );
                    bb.put( keyBuffer );
                    bb.flip();

                    channel.write( bb );

                    byte[] valueBuffer = valueSerializer.serialize( modification.getValue() );
                    bb = ByteBuffer.allocateDirect( valueBuffer.length );
                    bb.put( valueBuffer );
                    bb.flip();

                    channel.write( bb );
                }
                else if ( modification instanceof Deletion )
                {
                    byte[] keyBuffer = keySerializer.serialize( modification.getKey() );
                    ByteBuffer bb = ByteBuffer.allocateDirect( keyBuffer.length + 1 );
                    bb.put( Modification.DELETION );
                    bb.put( keyBuffer );
                    bb.flip();

                    channel.write( bb );
                }
                else
                // This is the poison pill, just exit
                {
                    return false;
                }

                // Flush to the disk for real
                channel.force( true );

                return true;
            }


            public void run()
            {
                Modification<K, V> modification = null;
                FileOutputStream stream;
                FileChannel channel = null;

                try
                {
                    stream = new FileOutputStream( journal );
                    channel = stream.getChannel();

                    while ( !Thread.currentThread().isInterrupted() )
                    {
                        modification = modificationsQueue.take();

                        boolean stop = flushModification( channel, modification );

                        if ( stop )
                        {
                            break;
                        }
                    }
                }
                catch ( InterruptedException ie )
                {
                    //System.out.println( "Interrupted" );
                    while ( ( modification = modificationsQueue.peek() ) != null );

                    try
                    {
                        flushModification( channel, modification );
                    }
                    catch ( IOException ioe )
                    {
                        // There is little we can do here...
                    }
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        };

        journalManagerThread = new Thread( journalTask );
        journalManagerThread.setDaemon( true );
        journalManagerThread.start();
    }


    /**
     * Creates a new BTree, with no initialization. 
     */
    public BTree()
    {
        btreeHeader = new BTreeHeader();
        type = BTreeTypeEnum.MANAGED;
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
        
        if( name == null )
        {
            throw new IllegalArgumentException( "BTree name cannot be null" );
        }
        
        String filePath = configuration.getFilePath();
        
        if ( filePath != null )
        {
            envDir = new File( filePath );
        }

        btreeHeader = new BTreeHeader();
        btreeHeader.setName( name );
        btreeHeader.setPageSize( configuration.getPageSize() );
        
        keySerializer = configuration.getKeySerializer();
        btreeHeader.setKeySerializerFQCN( keySerializer.getClass().getName() );

        valueSerializer = configuration.getValueSerializer();
        btreeHeader.setValueSerializerFQCN( valueSerializer.getClass().getName() );

        comparator = keySerializer.getComparator();
        readTimeOut = configuration.getReadTimeOut();
        writeBufferSize = configuration.getWriteBufferSize();
        allowDuplicates = configuration.isAllowDuplicates();
        type = configuration.getType();
        
        if ( comparator == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }

        // Create the first root page, with revision 0L. It will be empty
        // and increment the revision at the same time
        rootPage = new Leaf<K, V>( this );

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
        this( name, null, keySerializer, valueSerializer, DEFAULT_PAGE_SIZE );
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
    public BTree( String name, String dataDir, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer,
        int pageSize )
        throws IOException
    {
        btreeHeader = new BTreeHeader();
        btreeHeader.setName( name );
        
        if( dataDir != null )
        {
            envDir = new File( dataDir );
        }
        
        setPageSize( pageSize );
        writeBufferSize = DEFAULT_WRITE_BUFFER_SIZE;

        this.keySerializer = keySerializer;

        btreeHeader.setKeySerializerFQCN( keySerializer.getClass().getName() );

        this.valueSerializer = valueSerializer;

        btreeHeader.setValueSerializerFQCN( valueSerializer.getClass().getName() );

        comparator = keySerializer.getComparator();

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
        // if not in-memory then default to persist mode instead of managed
        if( ( envDir != null ) && ( type != BTreeTypeEnum.MANAGED ) )
        {
            if( !envDir.exists() )
            {
                boolean created = envDir.mkdirs();
                if( !created )
                {
                    throw new IllegalStateException( "Could not create the directory " + envDir + " for storing data" );
                }
            }
            
            this.file = new File( envDir, btreeHeader.getName() + DATA_SUFFIX );

            this.journal = new File( envDir, file.getName() + JOURNAL_SUFFIX );
            type = BTreeTypeEnum.PERSISTENT;
        }

        // Create the queue containing the pending read transactions
        readTransactions = new ConcurrentLinkedQueue<Transaction<K, V>>();

        // We will extract the Type to use for keys, using the comparator for that
        Class<?> comparatorClass = comparator.getClass();
        Type[] types = comparatorClass.getGenericInterfaces();
        Type[] argumentTypes = ( ( ParameterizedType ) types[0] ).getActualTypeArguments();

        if ( ( argumentTypes != null ) && ( argumentTypes.length > 0 ) && ( argumentTypes[0] instanceof Class<?> ) )
        {
            keyType = ( Class<?> ) argumentTypes[0];
        }

        writeLock = new ReentrantLock();

        // Check the files and create them if missing
        // Create the queue containing the modifications, if it's not a in-memory btree
        if ( type == BTreeTypeEnum.PERSISTENT )
        {
            modificationsQueue = new LinkedBlockingDeque<Modification<K, V>>();
            
            if ( file.length() > 0 )
            {
                // We have some existing file, load it 
                load( file );
            }
            
            withJournal = true;
            
            // If the journal is not empty, we have to read it
            // and to apply all the modifications to the current file
            if ( journal.length() > 0 )
            {
                applyJournal();
            }
            
            // Initialize the Journal manager thread if it's not a in-memory btree
            createJournalManager();
        }
        else if ( type == null )
        {
            type = BTreeTypeEnum.IN_MEMORY;
        }
        
        // Initialize the txnManager thread
        createTransactionManager();
    }


    /**
     * Close the BTree, cleaning up all the data structure
     */
    public void close() throws IOException
    {
        // Stop the readTransaction thread
        readTransactionsThread.interrupt();
        readTransactions.clear();

        if ( type == BTreeTypeEnum.PERSISTENT )
        {
            // Stop the journal manager thread, by injecting a poison pill into
            // the queue this thread is using, so that all the epnding data
            // will be written before it shuts down
            modificationsQueue.add( new PoisonPill<K, V>() );

            // Flush the data
            flush();
        }
        
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
        this.type = BTreeTypeEnum.MANAGED;
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

            if( result instanceof ModifyResult )
            {
                existingValue = ( ( ModifyResult<K, V> ) result ).getModifiedValue();
            }
            
            // If the BTree is managed, we have to update the rootPage on disk
            if ( isManaged() )
            {
                // Update the BTree header now
                recordManager.updateBtreeHeader( this, ( ( AbstractPage<K, V> ) rootPage ).getOffset() );
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

                if ( isManaged() )
                {
                    // Write the modified page on disk
                    // Note that we don't use the holder, the new root page will
                    // remain in memory.
                    ElementHolder<Page<K, V>, K, V> holder = recordManager.writePage( this, modifiedPage,
                        revision );

                    // Store the offset on disk in the page in memory
                    ( ( AbstractPage<K, V> ) modifiedPage ).setOffset( ( ( ReferenceHolder<Page<K, V>, K, V> ) holder )
                        .getOffset() );
                }

                // This is a new root
                rootPage = modifiedPage;
                tuple = removeResult.getRemovedElement();
            }

            if ( type == BTreeTypeEnum.PERSISTENT )
            {
                // Inject the modification into the modification queue
                modificationsQueue.add( new Deletion<K, V>( key ) );
            }

            // Decrease the number of elements in the current tree if the deletion is successful
            if ( tuple != null )
            {
                btreeHeader.decrementNbElems();

                // If the BTree is managed, we have to update the rootPage on disk
                if ( isManaged() )
                {
                    // Update the BTree header now
                    recordManager.updateBtreeHeader( this, ( ( AbstractPage<K, V> ) rootPage ).getOffset() );
                }
            }

            // Store the created rootPage into the revision BTree
            if ( keepRevisions )
            {
                if ( isManaged() )
                {
                    recordManager.storeRootPage( this, rootPage );
                }
                else
                {
                    // Todo
                }
            }

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
    public BTree<V,V> getValues( K key ) throws IOException, KeyNotFoundException
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
    public Cursor<K, V> browse() throws IOException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        LinkedList<ParentPos<K, V>> stack = new LinkedList<ParentPos<K, V>>();

        Cursor<K, V> cursor = rootPage.browse( transaction, stack );

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
    public Cursor<K, V> browse( long revision ) throws IOException, KeyNotFoundException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        // And get the cursor
        LinkedList<ParentPos<K, V>> stack = new LinkedList<ParentPos<K, V>>();
        Cursor<K, V> cursor = revisionRootPage.browse( transaction, stack );

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
    public Cursor<K, V> browseFrom( K key ) throws IOException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        Cursor<K, V> cursor = rootPage.browse( key, transaction, new LinkedList<ParentPos<K, V>>() );

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
    public Cursor<K, V> browseFrom( long revision, K key ) throws IOException, KeyNotFoundException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the rootPage for this revision
        Page<K, V> revisionRootPage = getRootPage( revision );

        // And get the cursor
        LinkedList<ParentPos<K, V>> stack = new LinkedList<ParentPos<K, V>>();
        Cursor<K, V> cursor = revisionRootPage.browse( key, transaction, stack );

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

            if ( isManaged() )
            {
                // Write the modified page on disk
                // Note that we don't use the holder, the new root page will
                // remain in memory.
                ElementHolder<Page<K, V>, K, V> holder = recordManager.writePage( this, modifiedPage,
                    revision );

                // Store the offset on disk in the page in memory
                ( ( AbstractPage<K, V> ) modifiedPage ).setOffset( ( ( ReferenceHolder<Page<K, V>, K, V> ) holder )
                    .getOffset() );
            }

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
            if ( isManaged() )
            {
                ElementHolder<Page<K, V>, K, V> holderLeft = recordManager.writePage( this,
                    ( ( SplitResult ) result ).getLeftPage(), revision );

                // Store the offset on disk in the page
                ( ( AbstractPage ) ( ( SplitResult ) result ).getLeftPage() )
                    .setOffset( ( ( ReferenceHolder ) holderLeft ).getOffset() );

                ElementHolder<Page<K, V>, K, V> holderRight = recordManager.writePage( this,
                    ( ( SplitResult ) result ).getRightPage(),
                    revision );

                // Store the offset on disk in the page
                ( ( AbstractPage<K, V> ) ( ( SplitResult ) result ).getRightPage() )
                    .setOffset( ( ( ReferenceHolder ) holderRight ).getOffset() );

                // Create the new rootPage
                newRootPage = new Node<K, V>( this, revision, pivot, holderLeft, holderRight );
            }
            else
            {
                // Create the new rootPage
                newRootPage = new Node<K, V>( this, revision, pivot, leftPage, rightPage );
            }

            // If the BTree is managed, we now have to write the page on disk
            // and to add this page to the list of modified pages
            if ( isManaged() )
            {
                ElementHolder<Page<K, V>, K, V> holder = recordManager
                    .writePage( this, newRootPage, revision );

                // Store the offset on disk in the page
                ( ( AbstractPage<K, V> ) newRootPage ).setOffset( ( ( ReferenceHolder ) holder ).getOffset() );
            }

            rootPage = newRootPage;
        }

        // Inject the modification into the modification queue
        if ( type == BTreeTypeEnum.PERSISTENT )
        {
            modificationsQueue.add( new Addition<K, V>( key, value ) );
        }

        // Increase the number of element in the current tree if the insertion is successful
        // and does not replace an element
        if ( modifiedValue == null )
        {
            btreeHeader.incrementNbElems();
        }

        // Store the created rootPage into the revision BTree
        if ( keepRevisions )
        {
            if ( isManaged() )
            {
                recordManager.storeRootPage( this, rootPage );
            }
            else
            {
                // Todo
            }
        }

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
     * @return the type for the keys
     */
    /* No qualifier*/Class<?> getKeyType()
    {
        return keyType;
    }


    /**
     * @return the comparator
     */
    public Comparator<K> getComparator()
    {
        return comparator;
    }


    /**
     * @param comparator the comparator to set
     */
    public void setComparator( Comparator<K> comparator )
    {
        this.comparator = comparator;
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
     * Flush the latest revision to disk
     * @param file The file into which the data will be written
     */
    public void flush( File file ) throws IOException
    {
        File parentFile = file.getParentFile();
        File baseDirectory = null;

        if ( parentFile != null )
        {
            baseDirectory = new File( file.getParentFile().getAbsolutePath() );
        }
        else
        {
            baseDirectory = new File( "." );
        }

        // Create a temporary file in the same directory to flush the current btree
        File tmpFileFD = File.createTempFile( "mavibot", null, baseDirectory );
        FileOutputStream stream = new FileOutputStream( tmpFileFD );
        FileChannel ch = stream.getChannel();

        // Create a buffer containing 200 4Kb pages (around 1Mb)
        ByteBuffer bb = ByteBuffer.allocateDirect( writeBufferSize );

        Cursor<K, V> cursor = browse();

        if ( keySerializer == null )
        {
            throw new RuntimeException( "Cannot flush the btree without a Key serializer" );
        }

        if ( valueSerializer == null )
        {
            throw new RuntimeException( "Cannot flush the btree without a Value serializer" );
        }

        // Write the number of elements first
        bb.putLong( btreeHeader.getNbElems() );

        while ( cursor.hasNext() )
        {
            Tuple<K, V> tuple = cursor.next();

            byte[] keyBuffer = keySerializer.serialize( tuple.getKey() );

            writeBuffer( ch, bb, keyBuffer );

            byte[] valueBuffer = valueSerializer.serialize( tuple.getValue() );

            writeBuffer( ch, bb, valueBuffer );
        }

        // Write the buffer if needed
        if ( bb.position() > 0 )
        {
            bb.flip();
            ch.write( bb );
        }

        // Flush to the disk for real
        ch.force( true );
        ch.close();

        // Rename the current file to save a backup
        File backupFile = File.createTempFile( "mavibot", null, baseDirectory );
        file.renameTo( backupFile );

        // Rename the temporary file to the initial file
        tmpFileFD.renameTo( file );

        // We can now delete the backup file
        backupFile.delete();
    }


    /** 
     * Inject all the modification from the journal into the btree
     * 
     * @throws IOException If we had some issue while reading the journal
     */
    private void applyJournal() throws IOException
    {
        long revision = generateRevision();

        if ( !journal.exists() )
        {
            throw new IOException( "The journal does not exist" );
        }

        FileChannel channel =
            new RandomAccessFile( journal, "rw" ).getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 65536 );

        BufferHandler bufferHandler = new BufferHandler( channel, buffer );

        // Loop on all the elements, store them in lists atm
        try
        {
            while ( true )
            {
                // Read the type 
                byte[] type = bufferHandler.read( 1 );

                if ( type[0] == Modification.ADDITION )
                {
                    // Read the key
                    K key = keySerializer.deserialize( bufferHandler );

                    //keys.add( key );

                    // Read the value
                    V value = valueSerializer.deserialize( bufferHandler );

                    //values.add( value );

                    // Inject the data in the tree. (to be replaced by a bulk load)
                    insert( key, value, revision );
                }
                else
                {
                    // Read the key
                    K key = keySerializer.deserialize( bufferHandler );

                    // Remove the key from the tree
                    delete( key, revision );
                }
            }
        }
        catch ( EOFException eofe )
        {
            // Done reading the journal. Delete it and recreate a new one
            journal.delete();
            journal.createNewFile();
        }
    }


    /**
     * Read the data from the disk into this BTree. All the existing data in the 
     * BTree are kept, the read data will be associated with a new revision.
     * 
     * @param file
     * @throws IOException
     */
    public void load( File file ) throws IOException
    {
        long revision = generateRevision();

        if ( !file.exists() )
        {
            throw new IOException( "The file does not exist" );
        }

        FileChannel channel =
            new RandomAccessFile( file, "rw" ).getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 65536 );

        BufferHandler bufferHandler = new BufferHandler( channel, buffer );

        long nbElems = LongSerializer.deserialize( bufferHandler.read( 8 ) );
        btreeHeader.setNbElems( nbElems );

        // Prepare a list of keys and values read from the disk
        //List<K> keys = new ArrayList<K>();
        //List<V> values = new ArrayList<V>();

        // desactivate the journal while we load the file
        boolean isJournalActivated = withJournal;

        withJournal = false;

        // Loop on all the elements, store them in lists atm
        for ( long i = 0; i < nbElems; i++ )
        {
            // Read the key
            K key = keySerializer.deserialize( bufferHandler );

            //keys.add( key );

            // Read the value
            V value = valueSerializer.deserialize( bufferHandler );

            //values.add( value );

            // Inject the data in the tree. (to be replaced by a bulk load)
            insert( key, value, revision );
        }

        // Restore the withJournal value
        withJournal = isJournalActivated;

        // Now, process the lists to create the btree
        // TODO... BulkLoad
    }


    /**
     * Get the rootPzge associated to a give revision.
     * 
     * @param revision The revision we are looking for
     * @return The rootPage associated to this revision
     * @throws IOException If we had an issue while accessing the underlying file
     * @throws KeyNotFoundException If the revision does not exist for this Btree
     */
    @SuppressWarnings("unchecked")
    private Page<K, V> getRootPage( long revision ) throws IOException, KeyNotFoundException
    {
        if ( isManaged() )
        {
            return recordManager.getRootPage( this, revision );
        }
        else
        {
            // Atm, the in-memory BTree does not support searches in many revisions
            return rootPage;
        }
    }


    /**
     * Flush the latest revision to disk. We will replace the current file by the new one, as
     * we flush in a temporary file.
     */
    public void flush() throws IOException
    {
        if ( type == BTreeTypeEnum.PERSISTENT )
        {
            // Then flush the file
            flush( file );

            // And empty the journal
            FileOutputStream stream = new FileOutputStream( journal );
            FileChannel channel = stream.getChannel();
            channel.position( 0 );
            channel.force( true );
        }
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
     * @return the file
     */
    public File getFile()
    {
        return file;
    }


    /**
     * @return the journal
     */
    public File getJournal()
    {
        return journal;
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
     * @return true if the BTree is fully in memory
     */
    public boolean isInMemory()
    {
        return type == BTreeTypeEnum.IN_MEMORY;
    }


    /**
     * @return true if the BTree is persisted on disk
     */
    public boolean isPersistent()
    {
        return type == BTreeTypeEnum.IN_MEMORY;
    }


    /**
     * @return true if the BTree is managed by a RecordManager
     */
    public boolean isManaged()
    {
        return type == BTreeTypeEnum.MANAGED;
    }


    /**
     * Create a ValueHolder depending on the kind of holder we want.
     * 
     * @param value The value to store
     * @return The value holder
     */
    /* no qualifier */ElementHolder createHolder( Object value )
    {
        if ( type == BTreeTypeEnum.MANAGED )
        {
            if ( value instanceof Page )
            {
                return new ReferenceHolder<Page<K, V>, K, V>( this, ( Page<K, V> ) value,
                    ( ( AbstractPage<K, V> ) value ).getOffset() );
            }
            else if ( isAllowDuplicates() )
            {
                return new DuplicateKeyMemoryHolder<K, V>( this, ( V ) value );
            }
            else
            {
                // Atm, keep the values in memory
                return new MemoryHolder<K, V>( this, ( V ) value );
            }
        }
        else
        {
            if ( isAllowDuplicates() && !( value instanceof Page ) )
            {
                return new DuplicateKeyMemoryHolder<K, V>( this, ( V ) value );
            }
            else
            {
                return new MemoryHolder( this, value );
            }
        }
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
        return allowDuplicates;
    }


    /**
     * @return the keepRevisions
     */
    public boolean isKeepRevisions()
    {
        return keepRevisions;
    }


    /**
     * @param keepRevisions the keepRevisions to set
     */
    public void setKeepRevisions( boolean keepRevisions )
    {
        this.keepRevisions = keepRevisions;
    }

    
    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        switch ( type )
        {
            case IN_MEMORY:
                sb.append( "In-memory " );
                break;

            case MANAGED:
                sb.append( "Managed " );
                break;

            case PERSISTENT:
                sb.append( "Persistent " );
                break;

        }

        sb.append( "BTree" );
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

        if ( comparator == null )
        {
            sb.append( "null" );
        }
        else
        {
            sb.append( comparator.getClass().getSimpleName() );
        }

        sb.append( ", DuplicatesAllowed: " ).append( allowDuplicates );

        if ( type == BTreeTypeEnum.PERSISTENT )
        {
            try
            {
                sb.append( ", file : " );

                if ( file != null )
                {
                    sb.append( file.getCanonicalPath() );
                }
                else
                {
                    sb.append( "Unknown" );
                }

                sb.append( ", journal : " );

                if ( journal != null )
                {
                    sb.append( journal.getCanonicalPath() );
                }
                else
                {
                    sb.append( "Unkown" );
                }
            }
            catch ( IOException ioe )
            {
                // There is little we can do here...
            }
        }

        sb.append( ") : \n" );
        sb.append( rootPage.dumpPage( "" ) );

        return sb.toString();
    }
}
