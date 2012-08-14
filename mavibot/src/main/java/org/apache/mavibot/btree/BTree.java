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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.mavibot.btree.serializer.BufferHandler;
import org.apache.mavibot.btree.serializer.LongSerializer;
import org.apache.mavibot.btree.serializer.Serializer;


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
    /** Default page size (number of entries per node) */
    public static final int DEFAULT_PAGE_SIZE = 16;

    /** A field used to generate new revisions in a thread safe way */
    private AtomicLong revision = new AtomicLong( 0 );

    /** A field used to generate new recordId in a thread safe way */
    private transient AtomicLong pageRecordIdGenerator;

    /** Comparator used to index entries. */
    private Comparator<K> comparator;

    /** The current rootPage */
    protected volatile Page<K, V> rootPage;

    /** The list of read transactions being executed */
    //private ConcurrentDoublyLinkedList<Transaction<K, V>> readTransactions;
    private ConcurrentLinkedQueue<Transaction<K, V>> readTransactions;

    /** Number of entries in each Page. */
    protected int pageSize;

    /** The type to use to create the keys */
    protected Class<?> keyType;

    /** The Key and Value serializer used for this tree. If none is provided, 
     * the BTree will deduce the serializer to use from the generic type, and
     * use the default Java serialization  */
    private Serializer<K, V> serializer;

    /** The associated file. If null, this is an in-memory btree  */
    private File file;

    /** The number of elements in the current revision */
    private AtomicLong nbElems = new AtomicLong( 0 );

    /** A lock used to protect the write operation against concurrent access */
    private final ReentrantLock writeLock = new ReentrantLock();

    /** The thread responsible for the cleanup of timed out reads */
    private Thread readTransactionsThread;

    /** Define a default delay for a read transaction. This is 10 seconds */
    public static final long DEFAULT_READ_TIMEOUT = 10 * 1000L;

    /** The read transaction timeout */
    private long readTimeOut = DEFAULT_READ_TIMEOUT;


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

                        // Loop on all the transactions from the queue
                        while ( ( transaction = readTransactions.peek() ) != null )
                        {
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
     * Creates a new in-memory BTree using the BTreeConfiguration to initialize the 
     * BTree
     * 
     * @param comparator The comparator to use
     */
    public BTree( BTreeConfiguration<K, V> configuration ) throws IOException
    {
        String fileName = configuration.getFilePrefix() + "." + configuration.getFileSuffix();

        File btreeFile = new File( configuration.getFilePath(), fileName );

        pageSize = configuration.getPageSize();
        comparator = configuration.getComparator();
        serializer = configuration.getSerializer();
        readTimeOut = configuration.getReadTimeOut();

        if ( comparator == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }

        // Now, initialize the BTree
        init();

        // Last, we load the data from the file, if it exists.
        if ( btreeFile.exists() )
        {
            // The file already exists, load it.
            file = btreeFile;
            load( file );
        }
        else
        {
            // We will create the new file
            file = btreeFile;
        }
    }


    /**
     * Creates a new in-memory BTree with a default page size and a comparator.
     * 
     * @param comparator The comparator to use
     */
    public BTree( Comparator<K> comparator ) throws IOException
    {
        this( null, comparator, null, DEFAULT_PAGE_SIZE );
    }


    /**
     * Creates a new in-memory BTree with a default page size and a comparator.
     * 
     * @param comparator The comparator to use
     * @param serializer The serializer to use
     */
    public BTree( Comparator<K> comparator, Serializer<K, V> serializer ) throws IOException
    {
        this( null, comparator, serializer, DEFAULT_PAGE_SIZE );
    }


    /**
     * Creates a new BTree with a default page size and a comparator, with an associated file.
     * 
     * @param file The file storing the BTree data
     * @param comparator The comparator to use
     */
    public BTree( File file, Comparator<K> comparator ) throws IOException
    {
        this( file, comparator, null, DEFAULT_PAGE_SIZE );
    }


    /**
     * Creates a new BTree with a default page size and a comparator, with an associated file.
     * 
     * @param file The file storing the BTree data
     * @param comparator The comparator to use
     * @param serializer The serializer to use
     */
    public BTree( File file, Comparator<K> comparator, Serializer<K, V> serializer ) throws IOException
    {
        this( file, comparator, serializer, DEFAULT_PAGE_SIZE );
    }


    /**
     * Creates a new in-memory BTree with a specific page size and a comparator.
     * 
     * @param comparator The comparator to use
     * @param pageSize The number of elements we can store in a page
     */
    public BTree( Comparator<K> comparator, int pageSize ) throws IOException
    {
        this( null, comparator, null, pageSize );
    }


    /**
     * Creates a new in-memory BTree with a specific page size and a comparator.
     * 
     * @param comparator The comparator to use
     * @param serializer The serializer to use
     * @param pageSize The number of elements we can store in a page
     */
    public BTree( Comparator<K> comparator, Serializer<K, V> serializer, int pageSize ) throws IOException
    {
        this( null, comparator, serializer, pageSize );
    }


    /**
     * Creates a new in-memory BTree with a specific page size and a comparator.
     * 
     * @param file The file storing the BTree data
     * @param comparator The comparator to use
     * @param pageSize The number of elements we can store in a page
     */
    public BTree( File file, Comparator<K> comparator, int pageSize ) throws IOException
    {
        this( file, comparator, null, pageSize );
    }


    /**
     * Creates a new BTree with a specific page size and a comparator, with an associated file.
     * 
     * @param file The file storing the BTree data
     * @param comparator The comparator to use
     * @param serializer The serializer to use
     * @param pageSize The number of elements we can store in a page
     */
    public BTree( File file, Comparator<K> comparator, Serializer<K, V> serializer, int pageSize )
        throws IOException
    {
        if ( comparator == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }

        this.comparator = comparator;
        this.file = file;
        setPageSize( pageSize );
        this.serializer = serializer;

        // Now, call the init() method
        init();
    }


    /**
     * Initialize the BTree.
     * 
     * @throws IOException If we get some exceptio while initializing the BTree
     */
    public void init() throws IOException
    {
        // Create the queue containing the pending read transactions
        //readTransactions = new ConcurrentDoublyLinkedList<Transaction<K, V>>();
        readTransactions = new ConcurrentLinkedQueue<Transaction<K, V>>();

        // Initialize the PageId counter
        pageRecordIdGenerator = new AtomicLong( 0 );

        // Initialize the revision counter
        revision = new AtomicLong( 0 );

        // Create the first root page, with revision 0L. It will be empty
        // and increment the revision at the same time
        rootPage = new Leaf<K, V>( this );

        // We will extract the Type to use for keys, using the comparator for that
        Class<?> comparatorClass = comparator.getClass();
        Type[] types = comparatorClass.getGenericInterfaces();
        Type[] argumentTypes = ( ( ParameterizedType ) types[0] ).getActualTypeArguments();

        if ( ( argumentTypes != null ) && ( argumentTypes.length > 0 ) && ( argumentTypes[0] instanceof Class<?> ) )
        {
            keyType = ( Class<?> ) argumentTypes[0];
        }

        // Initialize the txnManager thread
        createTransactionManager();
    }


    /**
     * Close the BTree, cleaning up all the data structure
     */
    public void close()
    {
        readTransactionsThread.interrupt();
        readTransactions.clear();
        rootPage = null;
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
        this.pageSize = pageSize;

        if ( pageSize <= 2 )
        {
            this.pageSize = DEFAULT_PAGE_SIZE;
        }

        this.pageSize = getPowerOf2( pageSize );
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
     * @return the pageSize
     */
    public int getPageSize()
    {
        return pageSize;
    }


    /**
     * Generates a new RecordId. It's only used by the Page instances.
     * 
     * @return a new incremental recordId
     */
    /** No qualifier */
    long generateRecordId()
    {
        return pageRecordIdGenerator.getAndIncrement();
    }


    /**
     * Generates a new revision number. It's only used by the Page instances.
     * 
     * @return a new incremental revision number
     */
    /** No qualifier */
    long generateRevision()
    {
        return revision.getAndIncrement();
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

        V existingValue = insert( key, value, revision );

        // Increase the number of element in the current tree if the insertion is successful
        // and does not replace an element
        if ( existingValue == null )
        {
            nbElems.getAndIncrement();
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

        // Decrease the number of element in the current tree if the delete is successful
        if ( deleted != null )
        {
            nbElems.getAndDecrement();
        }

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
        writeLock.lock();

        try
        {
            // If the key exists, the existing value will be replaced. We store it
            // to return it to the caller.
            Tuple<K, V> tuple = null;

            // Try to delete the entry starting from the root page. Here, the root
            // page may be either a Node or a Leaf
            DeleteResult<K, V> result = rootPage.delete( revision, key, null, -1 );

            if ( result instanceof NotPresentResult )
            {
                // Key not found.
                return null;
            }

            if ( result instanceof RemoveResult )
            {
                // The element was found, and removed
                RemoveResult<K, V> removeResult = ( RemoveResult<K, V> ) result;

                Page<K, V> newPage = removeResult.getModifiedPage();

                // This is a new root
                rootPage = newPage;
                tuple = removeResult.getRemovedElement();
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
     * Find a value in the tree, given its key. if the key is not found,
     * it will return null.
     * 
     * @param key The key we are looking at
     * @return The found value, or null if the key is not present in the tree
     * @throws IOException TODO
     */
    public V find( K key ) throws IOException
    {
        return rootPage.find( key );
    }


    /**
     * Creates a cursor starting on the given key
     * 
     * @param key The key which is the starting point. If the key is not found,
     * then the cursor will always return null.
     * @return A cursor on the btree
     * @throws IOException
     */
    public Cursor<K, V> browse( K key ) throws IOException
    {
        Transaction<K, V> transaction = beginReadTransaction();

        // Fetch the root page for this revision
        Page<K, V> root = rootPage;
        Cursor<K, V> cursor = root.browse( key, transaction, new LinkedList<ParentPos<K, V>>() );

        return cursor;
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
        Page<K, V> root = rootPage;
        LinkedList<ParentPos<K, V>> stack = new LinkedList<ParentPos<K, V>>();

        Cursor<K, V> cursor = root.browse( transaction, stack );

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
     * @return Existing value, if any.
     */
    private V insert( K key, V value, long revision ) throws IOException
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        if ( value == null )
        {
            throw new IllegalArgumentException( "Value must not be null" );
        }

        // Commented atm, we will have to play around the idea of transactions later
        writeLock.lock();

        try
        {
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

                // The root has just been modified, we haven't split it
                // Get it and make it the current root page
                rootPage = modifyResult.getModifiedPage();

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

                // Create the new rootPage
                rootPage = new Node<K, V>( this, revision, pivot, leftPage, rightPage );
            }

            // Return the value we have found if it was modified
            return modifiedValue;
        }
        finally
        {
            // See above
            writeLock.unlock();
        }
    }


    /**
     * Starts a Read Only transaction. If the transaction is not closed, it will be 
     * automatically closed after the timeout
     * @return The created transaction
     */
    private Transaction<K, V> beginReadTransaction()
    {
        Transaction<K, V> readTransaction = new Transaction<K, V>( rootPage, revision.get() - 1,
            System.currentTimeMillis() );

        readTransactions.add( readTransaction );

        return readTransaction;
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
     * @param serializer the serializer to set
     */
    public void setSerializer( Serializer<K, V> serializer )
    {
        this.serializer = serializer;
    }


    /**
     * @return the type for the keys
     */
    public Class<?> getKeyType()
    {
        return keyType;
    }


    /**
     * Write the data in the ByteBuffer, and eventually on disk if needed.
     * 
     * @param channel The channel we want to write to
     * @param bb The ByteBuffer we wat to feed
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
        File baseDirectory = new File( file.getParentFile().getAbsolutePath() );

        // Create a temporary file in the same directory to flush the current btree
        File tmpFileFD = File.createTempFile( "mavibot", null, baseDirectory );
        FileOutputStream stream = new FileOutputStream( tmpFileFD );
        FileChannel ch = stream.getChannel();
        ByteBuffer bb = ByteBuffer.allocateDirect( 65536 );

        Cursor<K, V> cursor = browse();

        if ( serializer == null )
        {
            throw new RuntimeException( "Cannot flush the btree without a serializer" );
        }

        // Write the number of elements first
        bb.putLong( nbElems.get() );

        while ( cursor.hasNext() )
        {
            Tuple<K, V> tuple = cursor.next();

            if ( bb.remaining() == 0 )
            {

            }

            byte[] keyBuffer = serializer.serializeKey( tuple.getKey() );

            writeBuffer( ch, bb, keyBuffer );

            byte[] valueBuffer = serializer.serializeValue( tuple.getValue() );

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
     * Read the data from the disk into this BTree. All the existing data in the 
     * BTree are kept, the read data will be associated with a new revision.
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

        // Prepare a list of keys and values read from the disk
        //List<K> keys = new ArrayList<K>();
        //List<V> values = new ArrayList<V>();

        // Loop on all the elements, store them in lists atm
        for ( long i = 0; i < nbElems; i++ )
        {
            // Read the key
            K key = serializer.deserializeKey( bufferHandler );

            //keys.add( key );

            // Read the value
            V value = serializer.deserializeValue( bufferHandler );

            //values.add( value );

            // Inject the data in the tree. (to be replaced by a bulk load)
            insert( key, value, revision );
        }

        // Now, process the lists to create the btree
        // TODO... BulkLoad
    }


    /**
     * Flush the latest revision to disk. We will replace the current file by the new one, as
     * we flush in a temporary file.
     */
    public void flush() throws IOException
    {
        flush( file );
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
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "BTree" );
        sb.append( "( pageSize:" ).append( pageSize );

        if ( rootPage != null )
        {
            sb.append( ", nbEntries:" ).append( rootPage.getNbElems() );
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

        sb.append( ") : \n" );
        sb.append( rootPage.dumpPage( "" ) );

        return sb.toString();
    }
}
