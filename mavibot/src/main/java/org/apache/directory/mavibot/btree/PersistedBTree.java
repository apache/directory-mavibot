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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Status;
import net.sf.ehcache.config.CacheConfiguration;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
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
public class PersistedBTree<K, V> extends AbstractBTree<K, V> implements TransactionManager, Closeable
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( PersistedBTree.class );

    protected static final Logger LOG_PAGES = LoggerFactory.getLogger( "LOG_PAGES" );

    /** The RecordManager if the B-tree is managed */
    private RecordManager recordManager;

    /** The cache associated with this B-tree */
    protected Cache cache;

    /** The default number of pages to keep in memory */
    static final int DEFAULT_CACHE_SIZE = 1000;

    /** The cache size, default to 1000 elements */
    protected int cacheSize = DEFAULT_CACHE_SIZE;

    /** The number of stored Values before we switch to a B-tree */
    private static final int DEFAULT_VALUE_THRESHOLD_UP = 8;

    /** The number of stored Values before we switch back to an array */
    private static final int DEFAULT_VALUE_THRESHOLD_LOW = 1;

    /** The configuration for the array <-> B-tree switch */
    /*No qualifier*/static int valueThresholdUp = DEFAULT_VALUE_THRESHOLD_UP;
    /*No qualifier*/static int valueThresholdLow = DEFAULT_VALUE_THRESHOLD_LOW;

    /** The BtreeInfo offset */
    private long btreeInfoOffset;

    /**
     * Creates a new BTree, with no initialization.
     */
    /* no qualifier */PersistedBTree()
    {
        setType( BTreeTypeEnum.PERSISTED );
    }


    /**
     * Creates a new persisted B-tree using the BTreeConfiguration to initialize the
     * BTree
     *
     * @param configuration The configuration to use
     */
    /* no qualifier */PersistedBTree( PersistedBTreeConfiguration<K, V> configuration )
    {
        super();
        String name = configuration.getName();

        if ( name == null )
        {
            throw new IllegalArgumentException( "BTree name cannot be null" );
        }

        setName( name );
        setPageSize( configuration.getPageSize() );
        setKeySerializer( configuration.getKeySerializer() );
        setValueSerializer( configuration.getValueSerializer() );
        setAllowDuplicates( configuration.isAllowDuplicates() );
        setType( configuration.getBtreeType() );

        readTimeOut = configuration.getReadTimeOut();
        writeBufferSize = configuration.getWriteBufferSize();
        cacheSize = configuration.getCacheSize();

        if ( keySerializer.getComparator() == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }

        // Create the first root page, with revision 0L. It will be empty
        // and increment the revision at the same time
        Page<K, V> rootPage = new PersistedLeaf<K, V>( this );

        // Create a B-tree header, and initialize it
        BTreeHeader<K, V> btreeHeader = new BTreeHeader<K, V>();
        btreeHeader.setRootPage( rootPage );
        btreeHeader.setBtree( this );

        if ( btreeType == BTreeTypeEnum.PERSISTED_SUB )
        {
            // The subBTree inherit its cache from its parent BTree
            this.cache = ( ( PersistedBTree<K, V> ) configuration.getParentBTree() ).getCache();
            readTransactions = new ConcurrentLinkedQueue<ReadTransaction<K, V>>();
        }

        // Add the B-tree header into the revisions
        btreeRevisions.put( 0L, btreeHeader );

        // Now, initialize the BTree
        init();
    }


    /**
     * Initialize the BTree.
     *
     * @throws IOException If we get some exception while initializing the BTree
     */
    public void init()
    {
        if ( btreeType != BTreeTypeEnum.PERSISTED_SUB )
        {
            // This is not a subBtree, we have to initialize the cache

            // Create the queue containing the pending read transactions
            readTransactions = new ConcurrentLinkedQueue<ReadTransaction<K, V>>();

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
    /* No qualifier */ConcurrentLinkedQueue<ReadTransaction<K, V>> getReadTransactions()
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

        // Clean the cache
        if ( cache.getStatus() == Status.STATUS_ALIVE )
        {
            cache.removeAll();
        }

        cache.dispose();
    }


    /**
     * @return the btreeOffset
     */
    /* No qualifier*/long getBtreeOffset()
    {
        return getBtreeHeader().getBTreeInfoOffset();
    }


    /**
     * @param btreeOffset the B-tree header Offset to set
     */
    /* No qualifier*/void setBtreeHeaderOffset( long btreeHeaderOffset )
    {
        getBtreeHeader().setBTreeHeaderOffset( btreeHeaderOffset );
    }


    /**
     * @return the rootPageOffset
     */
    /* No qualifier*/long getRootPageOffset()
    {
        return getBtreeHeader().getRootPageOffset();
    }


    /**
     * Gets the RecordManager for a managed BTree
     *
     * @return The recordManager if the B-tree is managed
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
    /* no qualifier */ Tuple<K, V> delete( K key, V value, long revision ) throws IOException
    {
        recordManager.beginTransaction();

        // Try to delete the entry starting from the root page. Here, the root
        // page may be either a Node or a Leaf
        DeleteResult<K, V> result = processDelete( key, value, revision );

        // Check that we have found the element to delete
        if ( result instanceof NotPresentResult )
        {
            // We haven't found the element in the B-tree, just get out
            recordManager.commit();

            return null;
        }

        // The element was found, and removed
        AbstractDeleteResult<K, V> deleteResult = ( AbstractDeleteResult<K, V> ) result;

        Tuple<K, V> tuple = deleteResult.getRemovedElement();

        // Update the B-tree header, creating a new BtreeHeader page now
        long newBtreeHeaderOffset = recordManager.updateBtreeHeader( this, ( ( AbstractPage<K, V> ) getRootPage() ).getOffset() );

        // Update the B-tree of B-trees with this new offset, if we are not already doing so
        switch ( btreeType )
        {
            case PERSISTED :
                // We have a new B-tree header to inject into the B-tree of btrees
                recordManager.addInBtreeOfBtrees( getName(), revision, newBtreeHeaderOffset );
                break;

            case BTREE_OF_BTREES :
            case COPIED_PAGES_BTREE :
                // The B-tree of B-trees or the copiedPages B-tree has been updated, update the RMheader parameters
                getBtreeHeader().setBTreeHeaderOffset( newBtreeHeaderOffset );

                break;

            default:
                // Nothing to do for sub-btrees
                break;
        }

        // We can safely free the copied pages
        recordManager.freePages( this, revision, result.getCopiedPages() );

        // If the B-tree is managed, we have to update the rootPage on disk
        // Update the RecordManager header
        recordManager.commit();

        // Return the value we have found if it was modified
        return tuple;
    }


    /**
     * Insert the tuple into the B-tree rootPage, get back the new rootPage
     */
    private DeleteResult<K, V> processDelete( K key, V value, long revision ) throws IOException
    {
        // Try to delete the entry starting from the root page. Here, the root
        // page may be either a Node or a Leaf
        DeleteResult<K, V> result = getRootPage().delete( key, value, revision);

        if ( result instanceof NotPresentResult )
        {
            // Key not found.
            return result;
        }

        // The element was found, and removed
        AbstractDeleteResult<K, V> removeResult = ( AbstractDeleteResult<K, V> ) result;

        Page<K, V> modifiedPage = removeResult.getModifiedPage();

        // Write the modified page on disk
        // Note that we don't use the holder, the new root page will
        // remain in memory.
        PageHolder<K, V> holder = writePage( modifiedPage, revision );

        // Store the offset on disk in the page in memory
        ( ( AbstractPage<K, V> ) modifiedPage ).setOffset( ( ( PersistedPageHolder<K, V> ) holder )
            .getOffset() );

        // Store the last offset on disk in the page in memory
        ( ( AbstractPage<K, V> ) modifiedPage )
            .setLastOffset( ( ( PersistedPageHolder<K, V> ) holder )
                .getLastOffset() );

        // This is a new root
        //rootPage = modifiedPage;

        // Decrease the number of elements in the current tree
        getBtreeHeader().decrementNbElems();

        // We have to update the rootPage on disk
        // Update the B-tree header now
        recordManager.updateBtreeHeader( this, ( ( AbstractPage<K, V> ) getBtreeHeader().getRootPage() ).getOffset() );

        // Return the value we have found if it was modified
        return result;
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
        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        // We have to start a new transaction, which will be committed or rollbacked
        // locally. This will duplicate the current BtreeHeader during this phase.
        if ( revision == -1L )
        {
            revision = currentRevision.get() + 1;
        }

        recordManager.beginTransaction();

        try
        {
            // Try to insert the new value in the tree at the right place,
            // starting from the root page. Here, the root page may be either
            // a Node or a Leaf
            InsertResult<K, V> result = processInsert( key, value, revision );

            // Done ! we can commit now
            commit();

            // Return the value we have found if it was modified
            return result;
        }
        catch ( IOException ioe )
        {
            // if we've got an error, we have to rollback
            rollback();

            throw ioe;
        }
    }


    /**
     * Insert the tuple into the B-tree rootPage, get back the new rootPage
     */
    private InsertResult<K, V> processInsert( K key, V value, long revision ) throws IOException
    {
        // Get the current B-tree header, and insert the value into it
        BTreeHeader<K, V> btreeHeader = getBtreeHeader();
        InsertResult<K, V> result = btreeHeader.getRootPage().insert( key, value, revision );

        // Create a new BTreeHeader
        BTreeHeader<K, V> newBtreeHeader = btreeHeader.copy();

        // Inject the old B-tree header into the pages to be freed
        // if we are inserting an element in a management BTree
        if ( ( btreeType == BTreeTypeEnum.BTREE_OF_BTREES ) || ( btreeType == BTreeTypeEnum.COPIED_PAGES_BTREE ) )
        {
            PageIO[] pageIos = recordManager.readPageIOs( btreeHeader.getBTreeHeaderOffset(), -1L );

            for ( PageIO pageIo : pageIos )
            {
                recordManager.freedPages.add( pageIo );
            }
        }

        Page<K, V> newRootPage;

        if ( result instanceof ModifyResult )
        {
            ModifyResult<K, V> modifyResult = ( ( ModifyResult<K, V> ) result );

            newRootPage = modifyResult.getModifiedPage();

            // Increment the counter if we have inserted a new value
            if ( modifyResult.getModifiedValue() == null )
            {
                newBtreeHeader.incrementNbElems();
            }
        }
        else
        {
            // We have split the old root, create a new one containing
            // only the pivotal we got back
            SplitResult<K, V> splitResult = ( ( SplitResult<K, V> ) result );

            K pivot = splitResult.getPivot();
            Page<K, V> leftPage = splitResult.getLeftPage();
            Page<K, V> rightPage = splitResult.getRightPage();

            // If the B-tree is managed, we have to write the two pages that were created
            // and to keep a track of the two offsets for the upper node
            PageHolder<K, V> holderLeft = writePage( leftPage, revision );

            PageHolder<K, V> holderRight = writePage( rightPage, revision );

            // Create the new rootPage
            newRootPage = new PersistedNode<K, V>( this, revision, pivot, holderLeft, holderRight );

            // Always increment the counter : we have added a new value
            newBtreeHeader.incrementNbElems();
        }

        // Write the new root page on disk
        LOG_PAGES.debug( "Writing the new rootPage revision {} for {}", revision, name );
        writePage( newRootPage, revision );

        // Update the new B-tree header
        newBtreeHeader.setRootPage( newRootPage );
        newBtreeHeader.setRevision( revision );

        // Write down the data on disk
        long newBtreeHeaderOffset = recordManager.writeBtreeHeader( this, newBtreeHeader );

        // Update the B-tree of B-trees with this new offset, if we are not already doing so
        switch ( btreeType )
        {
            case PERSISTED :
                // We have a new B-tree header to inject into the B-tree of btrees
                recordManager.addInBtreeOfBtrees( getName(), revision, newBtreeHeaderOffset );

                recordManager.addInCopiedPagesBtree( getName(), revision, result.getCopiedPages() );

                // Store the new revision
                storeRevision( newBtreeHeader );

                break;

            case BTREE_OF_BTREES :
                // The B-tree of B-trees or the copiedPages B-tree has been updated, update the RMheader parameters
                recordManager.updateRecordManagerHeader( newBtreeHeaderOffset, -1L );

                // We can free the copied pages
                recordManager.freePages( this, revision, result.getCopiedPages() );

                // Store the new revision
                storeRevision( newBtreeHeader );

                break;

            case COPIED_PAGES_BTREE :
                // The B-tree of B-trees or the copiedPages B-tree has been updated, update the RMheader parameters
                recordManager.updateRecordManagerHeader( -1L, newBtreeHeaderOffset );

                // We can free the copied pages
                recordManager.freePages( this, revision, result.getCopiedPages() );

                // Store the new revision
                storeRevision( newBtreeHeader );

                break;

            default:
                // Nothing to do for sub-btrees
                break;
        }

        // Get the new root page and make it the current root page
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
     * Write a page either in the pending pages if the transaction is started,
     * or directly on disk.
     */
    private PageHolder<K, V> writePage( Page<K, V> modifiedPage, long revision ) throws IOException
    {
        PageHolder<K, V> pageHolder = recordManager.writePage( this, modifiedPage, revision );

        return pageHolder;
    }

    /**
     * Get the rootPzge associated to a give revision.
     *
     * @param revision The revision we are looking for
     * @return The rootPage associated to this revision
     * @throws IOException If we had an issue while accessing the underlying file
     * @throws KeyNotFoundException If the revision does not exist for this Btree
     */
    public Page<K, V> getRootPage( long revision ) throws IOException, KeyNotFoundException
    {
        return recordManager.getRootPage( this, revision );
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
     * @return the btreeInfoOffset
     */
    public long getBtreeInfoOffset()
    {
        return btreeInfoOffset;
    }


    /**
     * @param btreeInfoOffset the btreeInfoOffset to set
     */
    public void setBtreeInfoOffset( long btreeInfoOffset )
    {
        this.btreeInfoOffset = btreeInfoOffset;
    }


    /**
     * Starts a transaction
     */
    public void beginTransaction()
    {
        beginTransaction( getRevision() + 1 );
    }


    /**
     * Starts a transaction
     */
    private void beginTransaction( long revision )
    {
    }


    /**
     * Commits a transaction
     */
    public void commit()
    {
        recordManager.commit();
    }


    /**
     * Rollback a transaction
     */
    public void rollback()
    {
        recordManager.rollback();
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Managed BTree" );
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

        sb.append( ") : \n" );
        sb.append( getBtreeHeader().getRootPage().dumpPage( "" ) );

        return sb.toString();
    }
}
