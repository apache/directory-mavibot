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
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.collections.map.LRUMap;
import org.apache.directory.mavibot.btree.exception.BTreeCreationException;
import org.apache.directory.mavibot.btree.exception.CursorException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The b-tree MVCC data structure.
 *
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BTreeImpl<K, V> implements BTree<K, V>, Closeable
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( BTreeImpl.class );

    protected static final Logger LOG_PAGES = LoggerFactory.getLogger( "org.apache.directory.mavibot.LOG_PAGES" );

    /** The default number of pages to keep in memory */
    public static final int DEFAULT_CACHE_SIZE = 1000;

    /** The size of the stack to use to manage tree searches */
    private static final int MAX_STACK_DEPTH = 32;

    /** The BtreeInfo */
    private BTreeInfo<K, V> btreeInfo = null;

    /** The cache size, default to 1000 elements */
    private int cacheSize = DEFAULT_CACHE_SIZE;
    
    /** The current Header for a managed BTree */
    private BTreeHeader<K, V> currentBtreeHeader;

    /** The BTree type : either in-memory, disk backed or persisted */
    private BTreeTypeEnum btreeType;

    /** The cache associated with this B-tree */
    private LRUMap cache;

    /** The Key serializer used for this tree.*/
    private ElementSerializer<K> keySerializer;

    /** The FQCN of the Key serializer */
    private String keySerializerFQCN;

    /** The Value serializer used for this tree. */
    private ElementSerializer<V> valueSerializer;

    /** The FQCN of the Value serializer */
    private String valueSerializerFQCN;

    /** The BTree name */
    private String name;

    /** The number of elements in a page for this B-tree */
    private int pageNbElem;
    
    /** The RecordManager */
    private RecordManager recordManager;
    
    /** The RecordManagerHeader */
    private RecordManagerHeader recordManagerHeader;


    /**
     * Creates a new BTree, with no initialization.
     */
    /* No qualifier */BTreeImpl()
    {
        setType( BTreeTypeEnum.PERSISTED );
    }


    /**
     * Creates a new persisted B-tree using the BTreeConfiguration to initialize the
     * BTree
     *
     * @param configuration The configuration to use
     */
    BTreeImpl( Transaction transaction, BTreeConfiguration<K, V> configuration )
    {
        if ( configuration.getName() == null )
        {
            throw new IllegalArgumentException( "BTree name cannot be null" );
        }

        setName( configuration.getName() );
        setPageNbElem( configuration.getPageNbElem() );
        setKeySerializer( configuration.getKeySerializer() );
        setValueSerializer( configuration.getValueSerializer() );
        setType( configuration.getBtreeType() );
        recordManager = transaction.getRecordManager();
        recordManagerHeader = transaction.getRecordManagerHeader();

        cacheSize = configuration.getCacheSize();

        if ( keySerializer.getComparator() == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }
        
        initCache();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public TupleCursor<K, V> browse( Transaction transaction ) throws IOException, KeyNotFoundException, CursorException
    {
        if ( transaction == null )
        {
            return new EmptyTupleCursor<>();
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
    @Override
    public TupleCursor<K, V> browseFrom( Transaction transaction, K key ) throws IOException
    {
        // Check that we have a TransactionManager
        if ( transaction == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        ParentPos<K, V>[] stack = ( ParentPos<K, V>[] ) Array.newInstance( ParentPos.class, MAX_STACK_DEPTH );

        return getRootPage().browse( transaction, key, stack, 0 );
    }


    /**
     * Close the BTree, cleaning up all the data structure
     */
    @Override
    public void close() throws IOException
    {
        // Clean the cache
        //cache.clear();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains( Transaction transaction, K key, V value ) throws IOException
    {
        if ( transaction == null )
        {
            return false;
        }
        else
        {
            return getRootPage().contains( key, value );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple<K, V> delete( WriteTransaction transaction, K key ) throws IOException
    {
        // Check that we have a TransactionManager
        if ( transaction == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        try
        {
            return processDelete( transaction, key );
        }
        catch ( IOException ioe )
        {
            // We have had an exception, we must rollback the transaction
            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V get( Transaction transaction, K key ) throws IOException, KeyNotFoundException
    {
        if ( transaction == null )
        {
            return null;
        }
        else
        {
            return getRootPage().get( key );
        }
    }


    /**
     * Initialize the BTree.
     *
     * @throws IOException If we get some exception while initializing the BTree
     */
    public void initCache()
    {
        // This is not a subBtree, we have to initialize the cache
        if ( cacheSize < 1 )
        {
            cacheSize = DEFAULT_CACHE_SIZE;
        }

        cache = new LRUMap( cacheSize );
    }


    /**
     * Return the cache we use in this BTree
     */
    /* No qualifier */LRUMap getCache()
    {
        return cache;
    }


    /**
     * @return the btreeOffset
     */
    /* No qualifier*/long getBtreeOffset()
    {
        return currentBtreeHeader.getOffset();
    }


    /**
     * @param btreeOffset the B-tree header Offset to set
     */
    /* No qualifier*/void setBtreeHeaderOffset( long btreeHeaderOffset )
    {
        currentBtreeHeader.setOffset( btreeHeaderOffset );
    }


    /**
     * @return the rootPageOffset
     */
    /* No qualifier*/long getRootPageOffset()
    {
        return currentBtreeHeader.getRootPageOffset();
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
    @Override
    public Tuple<K, V> delete( WriteTransaction transaction, K key, V value ) throws IOException
    {
        try
        {
            // Try to delete the entry starting from the root page. Here, the root
            // page may be either a Node or a Leaf
            DeleteResult<K, V> result = processDelete( transaction, key, value );

            // Check that we have found the element to delete
            if ( result instanceof NotPresentResult )
            {
                // We haven't found the element in the B-tree, just get out
                // without updating the recordManager

                return null;
            }

            // The element was found, and removed
            AbstractDeleteResult<K, V> deleteResult = ( AbstractDeleteResult<K, V> ) result;

            Tuple<K, V> tuple = deleteResult.getRemovedElement();

            // If the B-tree is managed, we have to update the rootPage on disk
            // Update the RecordManager header

            // Return the value we have found if it was modified
            return tuple;
        }
        catch ( IOException ioe )
        {
            // if we've got an error, we have to rollback
            throw ioe;
        }
    }


    /**
     * Insert the tuple into the B-tree rootPage, get back the new rootPage
     */
    private DeleteResult<K, V> processDelete( WriteTransaction transaction, K key ) throws IOException
    {
        RecordManager recordManager = transaction.getRecordManager();
        RecordManagerHeader recordManagerheader = transaction.getRecordManagerHeader();
        // Get the current B-tree header, and delete the value from it
        BTreeHeader<K, V> btreeHeader = getBTreeHeader( getName() );

        // Try to delete the entry starting from the root page. Here, the root
        // page may be either a Node or a Leaf
        DeleteResult<K, V> result = btreeHeader.getRootPage().delete( transaction, key );

        if ( result instanceof NotPresentResult )
        {
            // Key not found.
            return result;
        }

        // Create a new BTreeHeader
        BTreeHeader<K, V> newBtreeHeader = btreeHeader.copy();

        // Inject the old B-tree header into the pages to be freed
        // if we are deleting an element from a management BTree
        if ( ( btreeType == BTreeTypeEnum.BTREE_OF_BTREES ) || ( btreeType == BTreeTypeEnum.COPIED_PAGES_BTREE ) )
        {
            PageIO[] pageIos = recordManager.readPageIOs( recordManagerheader, btreeHeader.getOffset(), -1L );

            for ( PageIO pageIo : pageIos )
            {
                recordManager.freedPages.add( pageIo );
            }
        }

        // The element was found, and removed
        AbstractDeleteResult<K, V> removeResult = ( AbstractDeleteResult<K, V> ) result;

        // This is a new root
        Page<K, V> newRootPage = removeResult.getModifiedPage();

        // Write the modified page on disk
        // Note that we don't use the holder, the new root page will
        // remain in memory.
        writePage( transaction, newRootPage );

        // Decrease the number of elements in the current tree
        newBtreeHeader.decrementNbElems();
        newBtreeHeader.setRootPage( newRootPage );
        newBtreeHeader.setRevision( revision );

        // Write down the data on disk
        PageIO[] newBtreeHeaderPageIos = recordManager.writeBtreeHeader( this, newBtreeHeader, false );
        long newBtreeHeaderOffset = newBtreeHeaderPageIos[0].getOffset();

        // Update the B-tree of B-trees with this new offset, if we are not already doing so
        switch ( btreeType )
        {
            case PERSISTED:
                // We have a new B-tree header to inject into the B-tree of btrees
                recordManager.addInBtreeOfBtrees( transaction, getName(), revision, newBtreeHeaderOffset );

                recordManager.addInCopiedPagesBtree( getName(), revision, result.getCopiedPages() );

                // Store the new revision
                storeRevision( newBtreeHeader, recordManager.isKeepRevisions() );

                break;

            case BTREE_OF_BTREES:
                // The B-tree of B-trees or the copiedPages B-tree has been updated, update the RMheader parameters
                recordManager.updateRecordManagerHeader( newBtreeHeaderOffset, -1L );

                // We can free the copied pages
                recordManager.freePages( this, revision, result.getCopiedPages() );

                // Store the new revision
                storeRevision( newBtreeHeader, recordManager.isKeepRevisions() );

                break;

            case COPIED_PAGES_BTREE:
                // The B-tree of B-trees or the copiedPages B-tree has been updated, update the RMheader parameters
                recordManager.updateRecordManagerHeader( -1L, newBtreeHeaderOffset );

                // We can free the copied pages
                recordManager.freePages( this, revision, result.getCopiedPages() );

                // Store the new revision
                storeRevision( newBtreeHeader, recordManager.isKeepRevisions() );

                break;

            default:
                // Nothing to do for sub-btrees
                break;
        }

        // Return the value we have found if it was modified
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasKey( Transaction transaction, K key ) throws IOException, KeyNotFoundException
    {
        if ( key == null )
        {
            return false;
        }

        if ( transaction == null )
        {
            return false;
        }
        else
        {
            return getRootPage().hasKey( key );
        }
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
    @Override
    public InsertResult<K, V> insert( WriteTransaction transaction, K key, V value ) throws IOException
    {
        try
        {
            // Try to insert the new value in the tree at the right place,
            // starting from the root page. Here, the root page may be either
            // a Node or a Leaf.
            
            // Get the current B-tree header, check if it's in the transaction's pages
            BTreeHeader<K, V> btreeHeader = currentBtreeHeader;
            BTreeHeader<K, V> newBtreeHeader;
            long revision = transaction.getRevision();
            
            // Get the rootPage
            Page<K, V> newRootPage;
            Page<K, V> rootPage = btreeHeader.getRootPage();
            
            // And insert the <K,V> in it. We will have three possible results :
            // o The <K,V> exists in the B-tree : we get back ExistsResult, and there is nothing to do
            // o We get back a ModifyResult, we have to get the page that will become the new rootPage
            // o The rootPage has been split, we need to create a new Node that will become the new
            // rootPage
            InsertResult<K, V> result = rootPage.insert( transaction, key, value );

            // First case : the B-tree is unmodified
            if ( result instanceof ExistsResult )
            {
                return result;
            }

            // If the current b-tree header has been modified, it will be in the WalObject map.
            // Fetch it, or create a new version of it.
            long btreeHeaderId = btreeHeader.getId();
            
            WALObject<?, ?> walObject = transaction.getWALObject( btreeHeaderId );
            
            if ( walObject != null )
            {
                newBtreeHeader = (BTreeHeader<K, V>)transaction.removeWALObject( btreeHeaderId );
                newBtreeHeader.initId( transaction.getRecordManagerHeader() );
            }
            else
            {
                // Create a new BTreeHeader
                newBtreeHeader = btreeHeader.copy();
            }
            
            // Second case : the B-tree has been modified, and the old rootPage has not
            // been split. We have to get the new root page an store it in the new B-tree 
            // header
            if ( result instanceof ModifyResult )
            {
                ModifyResult<K, V> modifyResult = ( ModifyResult<K, V> ) result;

                newRootPage = modifyResult.getModifiedPage();

                // Increment the counter if we have inserted a new value
                if ( modifyResult.getModifiedValue() == null )
                {
                    newBtreeHeader.incrementNbElems();
                }
                
                // Add the the old rootPage in the transaction copied pages,
                // if we are not processing the CopiedPages B-tree
                // In this case, we have to move the copied page into
                // the free-pages list
                if ( getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
                {
                    transaction.addCopiedWALObject( rootPage );
                }
                else
                {
                    // transaction.addFreePage( rootPage );
                }
            }
            else
            {
                // The old root page has been split. We have to create a new Node
                // that will contain the new pivot, and the reference to the two new
                // leaves.
                SplitResult<K, V> splitResult = ( SplitResult<K, V> ) result;

                K pivot = splitResult.getPivot();

                PageHolder<K, V> holderLeft = new PageHolder<>( this, splitResult.getLeftPage() );
                PageHolder<K, V> holderRight = new PageHolder<>( this, splitResult.getRightPage() );

                // Create the new rootPage if needed
                newRootPage = new Node<>( this, revision, pivot, holderLeft, holderRight );
                ( ( Node<K, V> ) newRootPage ).initId( transaction.getRecordManagerHeader() );

                if ( rootPage.getRevision() != revision )
                {
                    // Move the old rootPage to the FP list
                    // transaction.addFreePage( rootPage );
                }
                
                // Add the new root page in the transaction pages map
                transaction.addWALObject( newRootPage );
                
                // We now have to remove the old root page from the list of new pages
                // and also remove the old btree-header, as we have a new rootPage
                transaction.removeWALObject( btreeHeaderId );
                
                // Add the the old rootPage in the transaction copied pages,
                // if we are not processing the CopiedPages B-tree
                // In this case, we have to move the copied page into
                // the free-pages list
                if ( getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
                {
                    transaction.addCopiedWALObject( rootPage );
                }
                else
                {
                    // transaction.addFreePage( rootPage );
                }

                // Always increment the counter : we have added a new value
                newBtreeHeader.incrementNbElems();
            }
            
            // Update the new B-tree header
            newBtreeHeader.setRootPage( newRootPage );
            newBtreeHeader.setRevision( revision );
            
            // Inject the new btreeHeader into the created pages
            transaction.addWALObject( newBtreeHeader );
            
            // And move the old B-tree header into the CopiedPages B-tree,
            // if we are not processing the CopiedPages B-tree itself
            if ( getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
            {
                transaction.addCopiedWALObject( btreeHeader );
            }
            else
            {
                // transaction.addFreePage( btreeHeader );
            }

            // Get the new root page and make it the current root page
            currentBtreeHeader = newBtreeHeader;
            
            // Return the value we have found if it was modified
            return result;
        }
        catch ( IOException ioe )
        {
            throw ioe;
        }
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
    private PageHolder<K, V> writePage( WriteTransaction transaction, Page<K, V> modifiedPage ) throws IOException
    {
        transaction.addWALObject( modifiedPage );

        return new PageHolder<>( this, modifiedPage );
    }


    /**
     * @return The current BtreeHeader
     */
    /* no qualifier */ BTreeHeader<K, V> getBtreeHeader()
    {
        return currentBtreeHeader;
    }


    /**
     * @return The current BtreeHeader
     */
    /* no qualifier */ void setBtreeHeader( BTreeHeader<K, V> btreeHeader )
    {
        currentBtreeHeader = btreeHeader;
    }


    /**
     * @return the btreeInfoOffset
     */
    public long getBtreeInfoOffset()
    {
        return btreeInfo.getOffset();
    }


    /**
     * @param btreeInfoOffset the btreeInfoOffset to set
     */
    public void setBtreeInfo( BTreeInfo<K, V> btreeInfo )
    {
        this.btreeInfo = btreeInfo;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<K> getKeyComparator()
    {
        return keySerializer.getComparator();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public ElementSerializer<K> getKeySerializer()
    {
        return keySerializer;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String getKeySerializerFQCN()
    {
        return keySerializerFQCN;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setKeySerializer( ElementSerializer<K> keySerializer )
    {
        this.keySerializer = keySerializer;
        keySerializerFQCN = keySerializer.getClass().getName();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String getName()
    {
        return name;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setName( String name )
    {
        this.name = name;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getNbElems( RecordManagerHeader recordManagerHeader )
    {
        return 0L;
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setNbElems( long nbElems )
    {
        currentBtreeHeader.setNbElems( nbElems );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getPageNbElem()
    {
        return pageNbElem;
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setPageNbElem( int pageNbElem )
    {
        if ( pageNbElem <= 2 )
        {
            this.pageNbElem = DEFAULT_PAGE_NBELEM;
        }
        else
        {
            this.pageNbElem = getPowerOf2( pageNbElem );
        }
    }


    /**
     * Gets the number which is a power of 2 immediately above the given positive number.
     */
    private int getPowerOf2( int size )
    {
        int newSize = size - 1;
        newSize |= newSize >> 1;
        newSize |= newSize >> 2;
        newSize |= newSize >> 4;
        newSize |= newSize >> 8;
        newSize |= newSize >> 16;
        newSize++;

        return newSize;
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setRevision( long revision )
    {
        currentBtreeHeader.setRevision( revision );
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public RecordManager getRecordManager()
    {
        return recordManager;
    }
    
    
    /* No qualifier */ void setRecordManager( RecordManager recordManager )
    {
        this.recordManager = recordManager;
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public RecordManagerHeader getRecordManagerHeader()
    {
        return recordManagerHeader;
    }
    
    
    /* No qualifier */ void setRecordManagerHeader( RecordManagerHeader recordManagerHeader )
    {
        this.recordManagerHeader = recordManagerHeader;
    }


    /**
     * Get the current rootPage
     *
     * @return The rootPage
     */
    @Override
    public Page<K, V> getRootPage()
    {
        return currentBtreeHeader.getRootPage();
    }


    /**
     * Update the B-tree rootPage
     * 
     * @param rootPage the rootPage to set
     */
    /* no qualifier */void setRootPage( Page<K, V> rootPage )
    {
        currentBtreeHeader.setRootPage( rootPage );
    }


    /**
     * {@inheritDoc}
     */
    @Override
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
     * {@inheritDoc}
     */
    @Override
    public Comparator<V> getValueComparator()
    {
        return valueSerializer.getComparator();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public ElementSerializer<V> getValueSerializer()
    {
        return valueSerializer;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String getValueSerializerFQCN()
    {
        return valueSerializerFQCN;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setValueSerializer( ElementSerializer<V> valueSerializer )
    {
        this.valueSerializer = valueSerializer;
        valueSerializerFQCN = valueSerializer.getClass().getName();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Managed BTree" );
        sb.append( "[" ).append( getName() ).append( "]" );
        sb.append( "( pageNbElem:" ).append( getPageNbElem() );
        
        long nbElems = getBtreeHeader().getNbElems();

        sb.append( ", nbElements:" ).append( nbElems );
        sb.append( ", comparator:" );

        if ( keySerializer.getComparator() == null )
        {
            sb.append( "null" );
        }
        else
        {
            sb.append( keySerializer.getComparator().getClass().getSimpleName() );
        }

        sb.append( ") : \n" );

        if ( nbElems > 0L )
        {
            sb.append( getBtreeHeader().getRootPage().dumpPage( "" ) );
        }

        return sb.toString();
    }
}
