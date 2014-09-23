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


import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.directory.mavibot.btree.exception.InitializationException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.exception.MissingSerializerException;
import org.apache.directory.mavibot.btree.serializer.BufferHandler;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
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
/* No qualifier */class InMemoryBTree<K, V> extends AbstractBTree<K, V> implements Closeable
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( InMemoryBTree.class );

    /** The default journal name */
    public static final String DEFAULT_JOURNAL = "mavibot.log";

    /** The default data file suffix */
    public static final String DATA_SUFFIX = ".db";

    /** The default journal file suffix */
    public static final String JOURNAL_SUFFIX = ".log";

    /** The type to use to create the keys */
    /** The associated file. If null, this is an in-memory btree  */
    private File file;

    /** A flag used to tell the BTree that the journal is activated */
    private boolean withJournal;

    /** The associated journal. If null, this is an in-memory btree  */
    private File journal;

    /** The directory where the journal will be stored */
    private File envDir;

    /** The Journal channel */
    private FileChannel journalChannel = null;


    /**
     * Creates a new BTree, with no initialization.
     */
    /* no qualifier */InMemoryBTree()
    {
        super();
        setType( BTreeTypeEnum.IN_MEMORY );
    }


    /**
     * Creates a new in-memory BTree using the BTreeConfiguration to initialize the
     * BTree
     *
     * @param configuration The configuration to use
     */
    /* no qualifier */InMemoryBTree( InMemoryBTreeConfiguration<K, V> configuration )
    {
        super();
        String btreeName = configuration.getName();

        if ( btreeName == null )
        {
            throw new IllegalArgumentException( "BTree name cannot be null" );
        }

        String filePath = configuration.getFilePath();

        if ( filePath != null )
        {
            envDir = new File( filePath );
        }

        // Store the configuration in the B-tree
        setName( btreeName );
        setPageSize( configuration.getPageSize() );
        setKeySerializer( configuration.getKeySerializer() );
        setValueSerializer( configuration.getValueSerializer() );
        setAllowDuplicates( configuration.isAllowDuplicates() );
        setType( configuration.getType() );

        readTimeOut = configuration.getReadTimeOut();
        writeBufferSize = configuration.getWriteBufferSize();

        if ( keySerializer.getComparator() == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }

        // Create the B-tree header
        BTreeHeader<K, V> newBtreeHeader = new BTreeHeader<K, V>();

        // Create the first root page, with revision 0L. It will be empty
        // and increment the revision at the same time
        newBtreeHeader.setBTreeHeaderOffset( 0L );
        newBtreeHeader.setRevision( 0L );
        newBtreeHeader.setNbElems( 0L );
        newBtreeHeader.setRootPage( new InMemoryLeaf<K, V>( this ) );
        newBtreeHeader.setRootPageOffset( 0L );

        btreeRevisions.put( 0L, newBtreeHeader );
        currentBtreeHeader = newBtreeHeader;

        // Now, initialize the BTree
        try
        {
            init();
        }
        catch ( IOException ioe )
        {
            throw new InitializationException( ioe.getMessage() );
        }
    }


    /**
     * Initialize the BTree.
     *
     * @throws IOException If we get some exception while initializing the BTree
     */
    private void init() throws IOException
    {
        // if not in-memory then default to persist mode instead of managed
        if ( envDir != null )
        {
            if ( !envDir.exists() )
            {
                boolean created = envDir.mkdirs();

                if ( !created )
                {
                    throw new IllegalStateException( "Could not create the directory " + envDir + " for storing data" );
                }
            }

            this.file = new File( envDir, getName() + DATA_SUFFIX );

            this.journal = new File( envDir, file.getName() + JOURNAL_SUFFIX );
            setType( BTreeTypeEnum.BACKED_ON_DISK );
        }

        // Create the queue containing the pending read transactions
        readTransactions = new ConcurrentLinkedQueue<ReadTransaction<K, V>>();

        // Create the transaction manager
        transactionManager = new InMemoryTransactionManager();

        // Check the files and create them if missing
        // Create the queue containing the modifications, if it's not a in-memory btree
        if ( getType() == BTreeTypeEnum.BACKED_ON_DISK )
        {
            if ( file.length() > 0 )
            {
                // We have some existing file, load it
                load( file );
            }

            withJournal = true;

            FileOutputStream stream = new FileOutputStream( journal );
            journalChannel = stream.getChannel();

            // If the journal is not empty, we have to read it
            // and to apply all the modifications to the current file
            if ( journal.length() > 0 )
            {
                applyJournal();
            }
        }
        else
        {
            setType( BTreeTypeEnum.IN_MEMORY );

            // This is a new Btree, we have to store the BtreeHeader
            BTreeHeader<K, V> btreeHeader = new BTreeHeader<K, V>();
            btreeHeader.setRootPage( new InMemoryLeaf<K, V>( this ) );
            btreeHeader.setBtree( this );
            storeRevision( btreeHeader );
        }

        // Initialize the txnManager thread
        //FIXME we should NOT create a new transaction manager thread for each BTree
        //createTransactionManager();
    }


    /**
     * {@inheritDoc}
     */
    protected ReadTransaction<K, V> beginReadTransaction()
    {
        BTreeHeader<K, V> btreeHeader = getBtreeHeader();

        ReadTransaction<K, V> readTransaction = new ReadTransaction<K, V>( btreeHeader, readTransactions );

        readTransactions.add( readTransaction );

        return readTransaction;
    }


    /**
     * {@inheritDoc}
     */
    protected ReadTransaction<K, V> beginReadTransaction( long revision )
    {
        BTreeHeader<K, V> btreeHeader = getBtreeHeader( revision );

        if ( btreeHeader != null )
        {
            ReadTransaction<K, V> readTransaction = new ReadTransaction<K, V>( btreeHeader, readTransactions );

            readTransactions.add( readTransaction );

            return readTransaction;
        }
        else
        {
            return null;
        }
    }


    /**
     * Close the BTree, cleaning up all the data structure
     */
    public void close() throws IOException
    {
        // Stop the readTransaction thread
        // readTransactionsThread.interrupt();
        // readTransactions.clear();

        if ( getType() == BTreeTypeEnum.BACKED_ON_DISK )
        {
            // Flush the data
            flush();
            journalChannel.close();
        }
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
    protected Tuple<K, V> delete( K key, V value, long revision ) throws IOException
    {
        if ( revision == -1L )
        {
            revision = currentRevision.get() + 1;
        }

        BTreeHeader<K, V> oldBtreeHeader = getBtreeHeader();
        BTreeHeader<K, V> newBtreeHeader = createNewBtreeHeader( oldBtreeHeader, revision );
        newBtreeHeader.setBtree( this );

        // If the key exists, the existing value will be replaced. We store it
        // to return it to the caller.
        Tuple<K, V> tuple = null;

        // Try to delete the entry starting from the root page. Here, the root
        // page may be either a Node or a Leaf
        DeleteResult<K, V> result = getRootPage().delete( key, value, revision );

        if ( result instanceof NotPresentResult )
        {
            // Key not found.
            return null;
        }

        // Keep the oldRootPage so that we can later access it
        //Page<K, V> oldRootPage = rootPage;

        if ( result instanceof RemoveResult )
        {
            // The element was found, and removed
            RemoveResult<K, V> removeResult = ( RemoveResult<K, V> ) result;

            Page<K, V> modifiedPage = removeResult.getModifiedPage();

            // This is a new root
            newBtreeHeader.setRootPage( modifiedPage );
            tuple = removeResult.getRemovedElement();
        }

        if ( withJournal )
        {
            // Inject the modification into the modification queue
            writeToJournal( new Deletion<K, V>( key ) );
        }

        // Decrease the number of elements in the current tree if the deletion is successful
        if ( tuple != null )
        {
            newBtreeHeader.decrementNbElems();
        }

        storeRevision( newBtreeHeader );

        // Return the value we have found if it was modified
        if ( oldBtreeHeader.getNbUsers() == 0 )
        {
            btreeRevisions.remove( oldBtreeHeader.getRevision() );
        }

        return tuple;
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
    /* no qualifier */InsertResult<K, V> insert( K key, V value, long revision ) throws IOException
    {
        // We have to start a new transaction, which will be committed or rollbacked
        // locally. This will duplicate the current BtreeHeader during this phase.
        if ( revision == -1L )
        {
            revision = currentRevision.get() + 1;
        }

        BTreeHeader<K, V> oldBtreeHeader = getBtreeHeader();
        BTreeHeader<K, V> newBtreeHeader = createNewBtreeHeader( oldBtreeHeader, revision );
        newBtreeHeader.setBtree( this );

        // If the key exists, the existing value will be replaced. We store it
        // to return it to the caller.
        V modifiedValue = null;

        // Try to insert the new value in the tree at the right place,
        // starting from the root page. Here, the root page may be either
        // a Node or a Leaf
        InsertResult<K, V> result = newBtreeHeader.getRootPage().insert( key, value, revision );

        if ( result instanceof ExistsResult )
        {
            return result;
        }

        if ( result instanceof ModifyResult )
        {
            ModifyResult<K, V> modifyResult = ( ( ModifyResult<K, V> ) result );

            Page<K, V> modifiedPage = modifyResult.getModifiedPage();

            // The root has just been modified, we haven't split it
            // Get it and make it the current root page
            newBtreeHeader.setRootPage( modifiedPage );

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
            newBtreeHeader.setRootPage( new InMemoryNode<K, V>( this, revision, pivot, leftPage, rightPage ) );
        }

        // Inject the modification into the modification queue
        if ( withJournal )
        {
            writeToJournal( new Addition<K, V>( key, value ) );
        }

        // Increase the number of element in the current tree if the insertion is successful
        // and does not replace an element
        if ( modifiedValue == null )
        {
            newBtreeHeader.incrementNbElems();
        }

        storeRevision( newBtreeHeader );

        if ( oldBtreeHeader.getNbUsers() == 0 )
        {
            long oldRevision = oldBtreeHeader.getRevision();

            if ( oldRevision < newBtreeHeader.getRevision() )
            {
                btreeRevisions.remove( oldBtreeHeader.getRevision() );
            }
        }

        // Return the value we have found if it was modified
        return result;
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

        try
        {
            TupleCursor<K, V> cursor = browse();

            if ( keySerializer == null )
            {
                throw new MissingSerializerException( "Cannot flush the btree without a Key serializer" );
            }

            if ( valueSerializer == null )
            {
                throw new MissingSerializerException( "Cannot flush the btree without a Value serializer" );
            }

            // Write the number of elements first
            bb.putLong( getBtreeHeader().getNbElems() );

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
        }
        catch ( KeyNotFoundException knfe )
        {
            knfe.printStackTrace();
            throw new IOException( knfe.getMessage() );
        }

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
                    insert( key, value, getBtreeHeader().getRevision() );
                }
                else
                {
                    // Read the key
                    K key = keySerializer.deserialize( bufferHandler );

                    // Remove the key from the tree
                    delete( key, getBtreeHeader().getRevision() );
                }
            }
        }
        catch ( EOFException eofe )
        {
            eofe.printStackTrace();
            // Done reading the journal. truncate it
            journalChannel.truncate( 0 );
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
        if ( !file.exists() )
        {
            throw new IOException( "The file does not exist" );
        }

        FileChannel channel =
            new RandomAccessFile( file, "rw" ).getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 65536 );

        BufferHandler bufferHandler = new BufferHandler( channel, buffer );

        long nbElems = LongSerializer.deserialize( bufferHandler.read( 8 ) );

        // desactivate the journal while we load the file
        boolean isJournalActivated = withJournal;

        withJournal = false;

        // Loop on all the elements, store them in lists atm
        for ( long i = 0; i < nbElems; i++ )
        {
            // Read the key
            K key = keySerializer.deserialize( bufferHandler );

            // Read the value
            V value = valueSerializer.deserialize( bufferHandler );

            // Inject the data in the tree. (to be replaced by a bulk load)
            insert( key, value, getBtreeHeader().getRevision() );
        }

        // Restore the withJournal value
        withJournal = isJournalActivated;

        // Now, process the lists to create the btree
        // TODO... BulkLoad
    }


    /**
     * Get the rootPage associated to a give revision.
     *
     * @param revision The revision we are looking for
     * @return The rootPage associated to this revision
     * @throws IOException If we had an issue while accessing the underlying file
     * @throws KeyNotFoundException If the revision does not exist for this Btree
     */
    public Page<K, V> getRootPage( long revision ) throws IOException, KeyNotFoundException
    {
        // Atm, the in-memory BTree does not support searches in many revisions
        return getBtreeHeader().getRootPage();
    }


    /**
     * Get the current rootPage
     *
     * @return The rootPage
     */
    public Page<K, V> getRootPage()
    {
        return getBtreeHeader().getRootPage();
    }


    /* no qualifier */void setRootPage( Page<K, V> root )
    {
        getBtreeHeader().setRootPage( root );
    }


    /**
     * Flush the latest revision to disk. We will replace the current file by the new one, as
     * we flush in a temporary file.
     */
    public void flush() throws IOException
    {
        if ( getType() == BTreeTypeEnum.BACKED_ON_DISK )
        {
            // Then flush the file
            flush( file );
            journalChannel.truncate( 0 );
        }
    }


    /**
     * @return the file
     */
    public File getFile()
    {
        return file;
    }


    /**
     * Set the file path where the journal will be stored
     * 
     * @param filePath The file path
     */
    public void setFilePath( String filePath )
    {
        if ( filePath != null )
        {
            envDir = new File( filePath );
        }
    }


    /**
     * @return the journal
     */
    public File getJournal()
    {
        return journal;
    }


    /**
     * @return true if the BTree is fully in memory
     */
    public boolean isInMemory()
    {
        return getType() == BTreeTypeEnum.IN_MEMORY;
    }


    /**
     * @return true if the BTree is persisted on disk
     */
    public boolean isPersistent()
    {
        return getType() == BTreeTypeEnum.IN_MEMORY;
    }


    private void writeToJournal( Modification<K, V> modification )
        throws IOException
    {
        if ( modification instanceof Addition )
        {
            byte[] keyBuffer = keySerializer.serialize( modification.getKey() );
            ByteBuffer bb = ByteBuffer.allocateDirect( keyBuffer.length + 1 );
            bb.put( Modification.ADDITION );
            bb.put( keyBuffer );
            bb.flip();

            journalChannel.write( bb );

            byte[] valueBuffer = valueSerializer.serialize( modification.getValue() );
            bb = ByteBuffer.allocateDirect( valueBuffer.length );
            bb.put( valueBuffer );
            bb.flip();

            journalChannel.write( bb );
        }
        else if ( modification instanceof Deletion )
        {
            byte[] keyBuffer = keySerializer.serialize( modification.getKey() );
            ByteBuffer bb = ByteBuffer.allocateDirect( keyBuffer.length + 1 );
            bb.put( Modification.DELETION );
            bb.put( keyBuffer );
            bb.flip();

            journalChannel.write( bb );
        }

        // Flush to the disk for real
        journalChannel.force( true );
    }


    /**
     * Create a new B-tree header to be used for update operations
     * @param revision The reclaimed revision
     */
    private BTreeHeader<K, V> createNewBtreeHeader( BTreeHeader<K, V> btreeHeader, long revision )
    {
        BTreeHeader<K, V> newBtreeHeader = new BTreeHeader<K, V>();

        newBtreeHeader.setBTreeHeaderOffset( btreeHeader.getBTreeHeaderOffset() );
        newBtreeHeader.setRevision( revision );
        newBtreeHeader.setNbElems( btreeHeader.getNbElems() );
        newBtreeHeader.setRootPage( btreeHeader.getRootPage() );

        return newBtreeHeader;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        switch ( getType() )
        {
            case IN_MEMORY:
                sb.append( "In-memory " );
                break;

            case BACKED_ON_DISK:
                sb.append( "Persistent " );
                break;

            default:
                sb.append( "Wrong type... " );
                break;
        }

        sb.append( "BTree" );
        sb.append( "[" ).append( getName() ).append( "]" );
        sb.append( "( pageSize:" ).append( getPageSize() );

        if ( getBtreeHeader().getRootPage() != null )
        {
            sb.append( ", nbEntries:" ).append( getBtreeHeader().getNbElems() );
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

        sb.append( ", DuplicatesAllowed: " ).append( isAllowDuplicates() );

        if ( getType() == BTreeTypeEnum.BACKED_ON_DISK )
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
        sb.append( getRootPage().dumpPage( "" ) );

        return sb.toString();
    }
}
