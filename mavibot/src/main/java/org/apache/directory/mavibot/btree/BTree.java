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
public class BTree<K, V> implements Closeable
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( BTree.class );

    protected static final Logger LOG_PAGES = LoggerFactory.getLogger( "org.apache.directory.mavibot.LOG_PAGES" );

    /** Default page size (number of entries per node) */
    public static final int DEFAULT_PAGE_NBELEM = 16;

    /** Default size of the buffer used to write data on disk. Around 1Mb */
    public static final int DEFAULT_WRITE_BUFFER_SIZE = 4096 * 250;

    /** Define a default delay for a read transaction. This is 10 seconds */
    public static final long DEFAULT_READ_TIMEOUT = 10 * 1000L;

    /** The size of the stack to use to manage tree searches */
    private static final int MAX_STACK_DEPTH = 32;

    /** The BtreeInfo */
    private BTreeInfo<K, V> btreeInfo = null;
    
    /** The B-tree Header */
    private BTreeHeader<K, V> currentBtreeHeader;
    
    /** The RecordManager */
    private RecordManager recordManager;


    /**
     * Creates a new BTree, with no initialization.
     */
    BTree()
    {
    }


    /**
     * Creates a new persisted B-tree using the BTreeConfiguration to initialize the
     * BTree
     *
     * @param configuration The configuration to use
     */
    BTree( Transaction transaction, BTreeConfiguration<K, V> configuration )
    {
        if ( configuration.getName() == null )
        {
            throw new IllegalArgumentException( "BTree name cannot be null" );
        }

        btreeInfo = new BTreeInfo<>();
        
        setName( configuration.getName() );
        setPageNbElem( configuration.getPageNbElem() );
        setKeySerializer( configuration.getKeySerializer() );
        setValueSerializer( configuration.getValueSerializer() );
        setType( configuration.getBtreeType() );
        recordManager = transaction.getRecordManager();
        //recordManagerHeader = transaction.getRecordManagerHeader();
        
        if ( btreeInfo.getKeySerializer().getComparator() == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }
    }
    
    
    public BTree<K, V> copy()
    {
        BTree<K, V> copy = new BTree<>();
        
        copy.btreeInfo = btreeInfo;
        copy.currentBtreeHeader = currentBtreeHeader;
        copy.recordManager = recordManager;
        
        return copy;
    }


    /**
     * Creates a cursor starting at the beginning of the tree
     *
     * @param transaction The Transaction we are running in 
     * @return A cursor on the B-tree
     * @throws IOException
     */
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
     * Creates a cursor starting on the given key
     *
     * @param key The key which is the starting point. If the key is not found,
     * then the cursor will always return null.
     * @return A cursor on the B-tree
     * @throws IOException
     */
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
        // Nothing to do
    }


    /**
     * Checks if the B-tree contains the given key with the given value.
     *
     * @param key The key we are looking for
     * @param value The value associated with the given key
     * @return true if the key and value are associated with each other, false otherwise
     */
    public boolean contains( Transaction transaction, K key, V value ) throws IOException
    {
        if ( transaction == null )
        {
            return false;
        }
        else
        {
            return getRootPage().contains( transaction, key, value );
        }
    }


    /**
     * Delete the entry which key is given as a parameter. If the entry exists, it will
     * be removed from the tree, the old tuple will be returned. Otherwise, null is returned.
     *
     * @param key The key for the entry we try to remove
     * @return A Tuple<K, V> containing the removed entry, or null if it's not found.
     */
    public Tuple<K, V> delete( WriteTransaction transaction, K key ) throws IOException
    {
        // Check that we have a Transaction
        if ( transaction == null )
        {
            throw new BTreeCreationException( "We don't have a transactionLManager" );
        }

        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }

        DeleteResult<K, V> result = processDelete( transaction, key );
        
        // Check that we have found the element to delete
        if ( result instanceof NotPresentResult )
        {
            // We haven't found the element in the B-tree, just get out
            // without updating the recordManager

            return null;
        }

        // The element was found, and removed
        AbstractDeleteResult<K, V> deleteResult = ( AbstractDeleteResult<K, V> ) result;

        return deleteResult.getRemovedElement();
    }


    /**
     * Find a value in the tree, given its key. If the key is not found,
     * it will throw a KeyNotFoundException. <br/>
     * Note that we can get a null value stored, or many values.
     *
     * @param key The key we are looking at
     * @return The found value, or null if the key is not present in the tree
     * @throws KeyNotFoundException If the key is not found in the B-tree
     * @throws IOException TODO
     */
    public V get( Transaction transaction, K key ) throws IOException, KeyNotFoundException
    {
        if ( transaction == null )
        {
            return null;
        }
        else
        {
            return currentBtreeHeader.getRootPage().get( transaction, key );
        }
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
     * Insert the tuple into the B-tree rootPage, get back the new rootPage
     */
    private DeleteResult<K, V> processDelete( WriteTransaction transaction, K key ) throws IOException
    {
        // Get the current B-tree header, check if it's in the transaction's pages
        BTreeHeader<K, V> btreeHeader = currentBtreeHeader;

        // Get the rootPage
        Page<K, V> rootPage = btreeHeader.getRootPage();

        
        // Get the current B-tree header, and delete the value from it

        // Try to delete the entry starting from the root page. Here, the root
        // page may be either a Node or a Leaf
        DeleteResult<K, V> result = rootPage.delete( transaction, key, null, -1 );

        if ( result instanceof NotPresentResult )
        {
            // Key not found.
            return result;
        }

        AbstractDeleteResult<K, V> removeResult = ( AbstractDeleteResult<K, V> ) result;

        // Create a new BTreeHeader
        BTreeHeader<K, V> newBtreeHeader;
        long revision = transaction.getRevision();

        // Create a new BTreeHeader
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
            newBtreeHeader = btreeHeader.copy( transaction.getRecordManagerHeader() );
        }
        
        // Update the new B-tree header
        newBtreeHeader.setRevision( revision );
        newBtreeHeader.setRootPage( removeResult.getModifiedPage() );
        newBtreeHeader.decrementNbElems();

        // Inject the new btreeHeader into the created pages
        transaction.addWALObject( newBtreeHeader );

        // Store the new B-tree header into the btreeMap
        currentBtreeHeader = newBtreeHeader;
        
        // And move the old B-tree header into the CopiedPages B-tree,
        // if we are not processing the CopiedPages B-tree itself
        if ( btreeHeader.isBTreeUser() )
        {
            transaction.addCopiedWALObject( btreeHeader );
        }
        else
        {
            // transaction.addFreePage( btreeHeader );
        }

        return removeResult;
    }


    /**
     * Checks if the given key exists.
     *
     * @param key The key we are looking at
     * @return true if the key is present, false otherwise
     * @throws IOException If we have an error while trying to access the page
     * @throws KeyNotFoundException If the key is not found in the B-tree
     */
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
            return getRootPage().hasKey( transaction, key );
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
    public InsertResult<K, V> insert( WriteTransaction transaction, K key, V value ) throws IOException
    {
        // Check that we have a Transaction
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
            
            if ( walObject == null )
            {
                // Create a new BTreeHeader
                newBtreeHeader = btreeHeader.copy( transaction.getRecordManagerHeader() );
            }
            else
            {
                newBtreeHeader = ( BTreeHeader<K, V> ) walObject;
            }
            
            // Second case : the B-tree has been modified, and the old rootPage has not
            // been split. We have to get the new root page an store it in the new B-tree 
            // header
            if ( result instanceof ModifyResult )
            {
                ModifyResult<K, V> modifyResult = ( ModifyResult<K, V> ) result;

                newRootPage = modifyResult.getModifiedPage();

                // Increment the counter if we have inserted a new value, instead of replacing a value
                if ( modifyResult.getModifiedValue() == null )
                {
                    newBtreeHeader.incrementNbElems();
                }
            }
            else
            {
                // The old root page has been split. We have to create a new Node
                // that will contain the new pivot, and the reference to the two new
                // leaves.
                SplitResult<K, V> splitResult = ( SplitResult<K, V> ) result;

                K pivot = splitResult.getPivot();

                long leftPage = splitResult.getLeftPage().getOffset();
                
                if ( leftPage == BTreeConstants.NO_PAGE )
                {
                    leftPage = -splitResult.getLeftPage().getId();
                }
                
                long rightPage = splitResult.getRightPage().getOffset();

                if ( rightPage == BTreeConstants.NO_PAGE )
                {
                    rightPage = -splitResult.getRightPage().getId();
                }

                // Create the new rootPage with a new ID
                newRootPage = transaction.newNode( btreeInfo, pivot, leftPage, rightPage );

                if ( rootPage.getRevision() != revision )
                {
                    // Move the old rootPage to the FP list
                    // transaction.addFreePage( rootPage );
                }
                
                // Add the new root page in the transaction pages map
                transaction.addWALObject( newRootPage );
                
                // Always increment the counter : we have added a new value
                newBtreeHeader.incrementNbElems();
            }
            
            // Update the new B-tree header
            newBtreeHeader.setRootPage( newRootPage );
            newBtreeHeader.setRevision( revision );
            
            // Inject the new btreeHeader into the created pages, and remove the previous one
            transaction.removeWALObject( btreeHeaderId );
            transaction.updateWAL( btreeHeaderId, btreeHeader, newBtreeHeader );

            // Store the new B-tree header into the btreeMap
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
    private long writePage( WriteTransaction transaction, Page<K, V> modifiedPage ) throws IOException
    {
        transaction.addWALObject( modifiedPage );

        return modifiedPage.getOffset();
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
    public BTreeInfo<K, V> getBtreeInfo()
    {
        return btreeInfo;
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
     * @return the key comparator
     */
    public Comparator<K> getKeyComparator()
    {
        return btreeInfo.getKeySerializer().getComparator();
    }


    /**
     * @return the keySerializer
     */
    public ElementSerializer<K> getKeySerializer()
    {
        return btreeInfo.getKeySerializer();
    }


    /**
     * @return the keySerializer FQCN
     */
    public String getKeySerializerFQCN()
    {
        return btreeInfo.getKeySerializerFQCN();
    }


    /**
     * @param keySerializer the Key serializer to set
     */
    public void setKeySerializer( ElementSerializer<K> keySerializer )
    {
        btreeInfo.setKeySerializer( keySerializer );
        btreeInfo.setKeySerializerFQCN( keySerializer.getClass().getName() );
    }


    /**
     * @return the name
     */
    public String getName()
    {
        return btreeInfo.getName();
    }


    /**
     * @param name the name to set
     */
    public void setName( String name )
    {
        btreeInfo.setName( name );
    }


    /**
     * @return The current number of elements in the B-tree
     */
    public long getNbElems()
    {
        return currentBtreeHeader.getNbElems();
    }


    /**
     * {@inheritDoc}
     */
    /* no qualifier */void setNbElems( long nbElems )
    {
        currentBtreeHeader.setNbElems( nbElems );
    }


    /**
     * @return the number of elements per page
     */
    public int getPageNbElem()
    {
        return btreeInfo.getPageNbElem();
    }
    
    
    /**
     * Set the maximum number of elements we can store in a page. This must be a
     * number greater than 1, and a power of 2. The default page size is 16.
     * <br/>
     * If the provided size is below 2, we will default to DEFAULT_PAGE_NB_ELEM.<br/>
     * If the provided size is not a power of 2, we will select the closest power of 2
     * higher than the given number<br/>
     *
     * @param pageNbElem The number of element per page
     */
    public void setPageNbElem( int pageNbElem )
    {
        if ( pageNbElem <= 2 )
        {
            btreeInfo.setPageNbElem( DEFAULT_PAGE_NBELEM );
        }
        else
        {
            btreeInfo.setPageNbElem( getPowerOf2( pageNbElem ) );
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
     * @return The RecordManager instance
     */
    public RecordManager getRecordManager()
    {
        return recordManager;
    }
    
    
    /**
     * @param recordManager The RecordManager instance to set
     */
    /* No qualifier */ void setRecordManager( RecordManager recordManager )
    {
        this.recordManager = recordManager;
    }

    
    /**
     * @return The RecordManagerHeader instance
     *
    public RecordManagerHeader getRecordManagerHeader()
    {
        return recordManagerHeader;
    }
    
    
    /* No qualifier * void setRecordManagerHeader( RecordManagerHeader recordManagerHeader )
    {
        this.recordManagerHeader = recordManagerHeader;
    }


    /**
     * Get the current rootPage
     *
     * @return The current rootPage
     */
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
     * @return the type
     */
    public BTreeTypeEnum getType()
    {
        return btreeInfo.getType();
    }


    /**
     * @param type the type to set
     */
    public void setType( BTreeTypeEnum type )
    {
        btreeInfo.setType( type );
    }


    /**
     * @return the value comparator
     */
    public Comparator<V> getValueComparator()
    {
        return btreeInfo.getValueSerializer().getComparator();
    }


    /**
     * @return the valueSerializer
     */
    public ElementSerializer<V> getValueSerializer()
    {
        return btreeInfo.getValueSerializer();
    }


    /**
     * @return the valueSerializer FQCN
     */
    public String getValueSerializerFQCN()
    {
        return btreeInfo.getValueSerializerFQCN();
    }


    /**
     * @param valueSerializer the Value serializer to set
     */
    public void setValueSerializer( ElementSerializer<V> valueSerializer )
    {
        btreeInfo.setValueSerializer( valueSerializer );
        btreeInfo.setValueSerializerFQCN( valueSerializer.getClass().getName() );
    }
    
    
    /**
     * {@inheritDoc}
     * @see java.lang.Object#hashCode()
     *
    @Override
    public int hashCode()
    {
        return getName().hashCode();
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
        
        long nbElems = 0L;
        
        if ( currentBtreeHeader != null )
        {
            nbElems = currentBtreeHeader.getNbElems();
        }

        sb.append( ", nbElements:" ).append( nbElems );
        sb.append( ", comparator:" );

        if ( btreeInfo.getKeySerializer().getComparator() == null )
        {
            sb.append( "null" );
        }
        else
        {
            sb.append( btreeInfo.getKeySerializer().getComparator().getClass().getSimpleName() );
        }

        sb.append( ") : \n" );

        if ( nbElems > 0L )
        {
            sb.append( getBtreeHeader().getRootPage().dumpPage( "" ) );
        }

        return sb.toString();
    }
}
