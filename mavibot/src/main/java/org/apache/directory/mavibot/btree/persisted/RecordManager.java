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
package org.apache.directory.mavibot.btree.persisted;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.directory.mavibot.btree.AbstractPage;
import org.apache.directory.mavibot.btree.BTree;
import org.apache.directory.mavibot.btree.KeyHolder;
import org.apache.directory.mavibot.btree.Page;
import org.apache.directory.mavibot.btree.ValueHolder;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongArraySerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The RecordManager is used to manage the file in which we will store the BTrees. 
 * A RecordManager will manage more than one BTree.<br/>
 * 
 * It stores data in fixed size pages (default size is 512 bytes), which may be linked one to 
 * the other if the data we want to store is too big for a page.
 *  
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RecordManager
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( RecordManager.class );

    /** A dedicated logger for the check */
    protected static final Logger LOG_CHECK = LoggerFactory.getLogger( "RM_CHECK" );

    /** The associated file */
    private File file;

    /** The channel used to read and write data */
    private FileChannel fileChannel;

    /** The number of stored BTrees */
    private int nbBtree;

    /** The first and last free page */
    private long firstFreePage;
    private long lastFreePage;

    /** The list of available free pages */
    List<PageIO> freePages = new ArrayList<PageIO>();

    /** A counter to track the number of free pages */
    public AtomicLong nbFreedPages = new AtomicLong( 0 );
    public AtomicLong nbCreatedPages = new AtomicLong( 0 );
    public AtomicLong nbReusedPages = new AtomicLong( 0 );
    public AtomicLong nbUpdateRMHeader = new AtomicLong( 0 );
    public AtomicLong nbUpdateBTreeHeader = new AtomicLong( 0 );
    public AtomicLong nbUpdatePageIOs = new AtomicLong( 0 );

    /** The offset of the end of the file */
    private long endOfFileOffset;

    /** 
     * A Btree used to manage the page that has been copied in a new version.
     * Those pages can be reclaimed when the associated version is dead. 
     **/
    private BTree<RevisionName, long[]> copiedPageBTree;

    /** A BTree used to store all the valid revisions for all the stored BTrees */
    private BTree<RevisionName, Long> revisionBTree;

    /** A constant for an offset on a non existing page */
    private static final long NO_PAGE = -1L;

    /** The number of stored BTrees */
    private static final int NB_TREE_SIZE = 4;

    /** The header page size */
    private static final int PAGE_SIZE = 4;

    /** The size of the data size in a page */
    private static final int DATA_SIZE = 4;

    /** The size of the link to next page */
    private static final int LINK_SIZE = 8;

    /** Some constants */
    private static final int BYTE_SIZE = 1;
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;

    /** The size of the link to the first and last free page */
    private static final int FIRST_FREE_PAGE_SIZE = 8;
    private static final int LAST_FREE_PAGE_SIZE = 8;

    /** The default page size */
    private static final int DEFAULT_PAGE_SIZE = 512;

    /** The header size */
    private static int HEADER_SIZE = DEFAULT_PAGE_SIZE;

    /** A global buffer used to store the header */
    private static ByteBuffer HEADER_BUFFER;

    /** A static buffer used to store the header */
    private static byte[] HEADER_BYTES;

    /** The length of an Offset, as a nagative value */
    private static byte[] LONG_LENGTH = new byte[]
        { ( byte ) 0xFF, ( byte ) 0xFF, ( byte ) 0xFF, ( byte ) 0xF8 };

    /** The RecordManager underlying page size. */
    private int pageSize = DEFAULT_PAGE_SIZE;

    /** The set of managed BTrees */
    private Map<String, BTree<Object, Object>> managedBTrees;

    /** The offset on the last added BTree */
    private long lastAddedBTreeOffset = NO_PAGE;

    /** The default file name */
    private static final String DEFAULT_FILE_NAME = "mavibot.db";

    /** A deserializer for Offsets */
    private static final LongSerializer OFFSET_SERIALIZER = new LongSerializer();

    private static final String REVISION_BTREE_NAME = "_revisionBTree_";

    private static final String COPIED_PAGE_BTREE_NAME = "_copiedPageBTree_";

    /** A flag set to true if we want to keep old revisions */
    private boolean keepRevisions;
    
    /** A flag used by internal btrees */
    public static final boolean INTERNAL_BTREE = true;
    
    /** A flag used by internal btrees */
    public static final boolean NORMAL_BTREE = false;


    /**
     * Create a Record manager which will either create the underlying file
     * or load an existing one. If a folder is provided, then we will create
     * a file with a default name : mavibot.db
     * 
     * @param name The file name, or a folder name
     */
    public RecordManager( String fileName )
    {
        this( fileName, DEFAULT_PAGE_SIZE );
    }


    /**
     * Create a Record manager which will either create the underlying file
     * or load an existing one. If a folder is provider, then we will create
     * a file with a default name : mavibot.db
     * 
     * @param name The file name, or a folder name
     * @param pageSize the size of a page on disk
     */
    public RecordManager( String fileName, int pageSize )
    {
        managedBTrees = new LinkedHashMap<String, BTree<Object, Object>>();

        HEADER_BUFFER = ByteBuffer.allocate( pageSize );
        HEADER_BYTES = new byte[pageSize];
        HEADER_SIZE = pageSize;

        // Open the file or create it
        File tmpFile = new File( fileName );
        boolean isNewFile = false;

        if ( tmpFile.isDirectory() )
        {
            // It's a directory. Check that we don't have an existing mavibot file
            File mavibotFile = new File( tmpFile, DEFAULT_FILE_NAME );

            if ( !mavibotFile.exists() )
            {
                // We have to create a new file
                try
                {
                    mavibotFile.createNewFile();
                    isNewFile = true;
                }
                catch ( IOException ioe )
                {
                    LOG.error( "Cannot create the file {}", mavibotFile.getName() );
                    return;
                }
            }

            file = mavibotFile;
        }
        else
        {
            // It's a file. Let's see if it exists, otherwise create it
            if ( !tmpFile.exists() || ( tmpFile.length() == 0 ) )
            {
                isNewFile = true;

                try
                {
                    tmpFile.createNewFile();
                }
                catch ( IOException ioe )
                {
                    LOG.error( "Cannot create the file {}", tmpFile.getName() );
                    return;
                }
            }

            file = tmpFile;
        }

        try
        {
            RandomAccessFile randomFile = new RandomAccessFile( file, "rw" );
            fileChannel = randomFile.getChannel();

            // get the current end of file offset
            endOfFileOffset = fileChannel.size();

            if ( isNewFile )
            {
                this.pageSize = pageSize;
                initRecordManager();
            }
            else
            {
                loadRecordManager();
            }
        }
        catch ( Exception e )
        {
            LOG.error( "Error while initializing the RecordManager : {}", e.getMessage() );
            LOG.error( "", e );
            throw new RuntimeException( e );
        }
    }


    /**
     * We will create a brand new RecordManager file, containing nothing, but the header, 
     * a BTree to manage the old revisions we want to keep and
     * a BTree used to manage pages associated with old versions.
     * <br/>
     * The Header contains the following details :
     * <pre>
     * +-----------+
     * | PageSize  | 4 bytes : The size of a physical page (default to 4096)
     * +-----------+
     * |  NbTree   | 4 bytes : The number of managed BTrees (at least 1)
     * +-----------+ 
     * | FirstFree | 8 bytes : The offset of the first free page
     * +-----------+
     * | LastFree  | 8 bytes : The offset of the last free page
     * +-----------+
     * </pre>
     * 
     * We then store the BTree managing the pages that have been copied when we have added
     * or deleted an element in the BTree. They are associated with a version.
     * 
     * Last, we add the bTree that keep a track on each revision we can have access to.
     */
    private void initRecordManager() throws IOException
    {
        // Create a new Header
        nbBtree = 0;
        firstFreePage = NO_PAGE;
        lastFreePage = NO_PAGE;
        updateRecordManagerHeader();

        // Set the offset of the end of the file
        endOfFileOffset = fileChannel.size();

        // Now, initialize the Copied Page BTree
        copiedPageBTree = new PersistedBTree<RevisionName, long[]>( COPIED_PAGE_BTREE_NAME, new RevisionNameSerializer(),
            new LongArraySerializer() );

        // and initialize the Revision BTree
        revisionBTree = new PersistedBTree<RevisionName, Long>( REVISION_BTREE_NAME, new RevisionNameSerializer(),
            new LongSerializer() );

        // Inject these BTrees into the RecordManager
        try
        {
            manage( copiedPageBTree );
            manage( revisionBTree );
        }
        catch ( BTreeAlreadyManagedException btame )
        {
            // Can't happen here.
        }

        // We are all set !
    }


    /**
     * Load the BTrees from the disk. 
     * 
     * @throws InstantiationException 
     * @throws IllegalAccessException 
     * @throws ClassNotFoundException 
     */
    private void loadRecordManager() throws IOException, ClassNotFoundException, IllegalAccessException,
        InstantiationException
    {
        if ( fileChannel.size() != 0 )
        {
            ByteBuffer header = ByteBuffer.allocate( HEADER_SIZE );

            // The file exists, we have to load the data now 
            fileChannel.read( header );

            header.rewind();

            // The page size
            pageSize = header.getInt();

            // The number of managed BTrees
            nbBtree = header.getInt();

            // The first and last free page
            firstFreePage = header.getLong();
            lastFreePage = header.getLong();

            // Now read each BTree. The first one is the one which
            // manage the modified pages. Once read, we can discard all
            // the pages that are stored in it, as we have restarted 
            // the RecordManager.
            long btreeOffset = HEADER_SIZE;

            PageIO[] pageIos = readPageIOs( HEADER_SIZE, Long.MAX_VALUE );

            // Create the BTree
            copiedPageBTree = BTreeFactory.<RevisionName, long[]> createBTree();
            ((PersistedBTree<RevisionName, long[]>)copiedPageBTree).setBtreeOffset( btreeOffset );

            loadBTree( pageIos, copiedPageBTree );
            long nextBtreeOffset = ((PersistedBTree<RevisionName, long[]>)copiedPageBTree).getNextBTreeOffset();

            // And the Revision BTree
            pageIos = readPageIOs( nextBtreeOffset, Long.MAX_VALUE );

            revisionBTree = BTreeFactory.<RevisionName, Long> createBTree();
            ((PersistedBTree<RevisionName, Long>)revisionBTree).setBtreeOffset( nextBtreeOffset );

            loadBTree( pageIos, revisionBTree );
            nextBtreeOffset = ((PersistedBTree<RevisionName, Long>)revisionBTree).getNextBTreeOffset();

            // Then process the next ones
            for ( int i = 2; i < nbBtree; i++ )
            {
                // Create the BTree
                BTree<Object, Object> btree = BTreeFactory.createBTree();
                ((PersistedBTree<Object, Object>)btree).setRecordManager( this );
                ((PersistedBTree<Object, Object>)btree).setBtreeOffset( nextBtreeOffset );
                lastAddedBTreeOffset = nextBtreeOffset;

                // Read the associated pages
                pageIos = readPageIOs( nextBtreeOffset, Long.MAX_VALUE );

                // Load the BTree
                loadBTree( pageIos, btree );
                nextBtreeOffset = ((PersistedBTree<Object, Object>)btree).getNextBTreeOffset();

                // Store it into the managedBtrees map
                managedBTrees.put( btree.getName(), btree );
            }

            // We are done ! Let's finish with the last initilization parts
            endOfFileOffset = fileChannel.size();
        }
    }


    /**
     * Reads all the PageIOs that are linked to the page at the given position, including
     * the first page.
     * 
     * @param position The position of the first page
     * @return An array of pages
     */
    private PageIO[] readPageIOs( long position, long limit ) throws IOException, EndOfFileExceededException
    {
        LOG.debug( "Read PageIOs at position {}", position );

        if ( limit <= 0 )
        {
            limit = Long.MAX_VALUE;
        }

        PageIO firstPage = fetchPage( position );
        firstPage.setSize();
        List<PageIO> listPages = new ArrayList<PageIO>();
        listPages.add( firstPage );
        long dataRead = pageSize - LONG_SIZE - INT_SIZE;

        // Iterate on the pages, if needed
        long nextPage = firstPage.getNextPage();

        if ( ( dataRead < limit ) && ( nextPage != NO_PAGE ) )
        {
            while ( dataRead < limit )
            {
                PageIO page = fetchPage( nextPage );
                listPages.add( page );
                nextPage = page.getNextPage();
                dataRead += pageSize - LONG_SIZE;

                if ( nextPage == NO_PAGE )
                {
                    page.setNextPage( NO_PAGE );
                    break;
                }
            }
        }

        LOG.debug( "Nb of PageIOs read : {}", listPages.size() );

        // Return 
        return listPages.toArray( new PageIO[]
            {} );
    }


    /**
     * Read a BTree from the disk. The meta-data are at the given position in the list of pages.
     * 
     * @param pageIos The list of pages containing the meta-data
     * @param btree The BTree we have to initialize
     * @throws InstantiationException 
     * @throws IllegalAccessException 
     * @throws ClassNotFoundException 
     */
    private <K, V> void loadBTree( PageIO[] pageIos, BTree<K, V> btree ) throws EndOfFileExceededException,
        IOException, ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        long dataPos = 0L;

        // The BTree current revision
        long revision = readLong( pageIos, dataPos );
        BTreeFactory.setRevision( btree, revision );
        dataPos += LONG_SIZE;

        // The nb elems in the tree
        long nbElems = readLong( pageIos, dataPos );
        BTreeFactory.setNbElems( btree, nbElems );
        dataPos += LONG_SIZE;

        // The BTree rootPage offset
        long rootPageOffset = readLong( pageIos, dataPos );
        BTreeFactory.setRootPageOffset( btree, rootPageOffset );
        dataPos += LONG_SIZE;

        // The next BTree offset
        long nextBTreeOffset = readLong( pageIos, dataPos );
        BTreeFactory.setNextBTreeOffset( btree, nextBTreeOffset );
        dataPos += LONG_SIZE;

        // The BTree page size
        int btreePageSize = readInt( pageIos, dataPos );
        BTreeFactory.setPageSize( btree, btreePageSize );
        dataPos += INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = readBytes( pageIos, dataPos );
        dataPos += INT_SIZE + btreeNameBytes.limit();
        String btreeName = Strings.utf8ToString( btreeNameBytes );
        BTreeFactory.setName( btree, btreeName );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = readBytes( pageIos, dataPos );
        dataPos += INT_SIZE + keySerializerBytes.limit();

        String keySerializerFqcn = "";

        if ( keySerializerBytes != null )
        {
            keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );
        }

        BTreeFactory.setKeySerializer( btree, keySerializerFqcn );

        // The valueSerialier FQCN
        ByteBuffer valueSerializerBytes = readBytes( pageIos, dataPos );

        String valueSerializerFqcn = "";
        dataPos += INT_SIZE + valueSerializerBytes.limit();

        if ( valueSerializerBytes != null )
        {
            valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );
        }

        BTreeFactory.setValueSerializer( btree, valueSerializerFqcn );

        // The BTree allowDuplicates flag
        int allowDuplicates = readInt( pageIos, dataPos );
        ((PersistedBTree<K, V>)btree).setAllowDuplicates( allowDuplicates != 0 );
        dataPos += INT_SIZE;

        // Now, init the BTree
        btree.init();
        
        ((PersistedBTree<K, V>)btree).setRecordManager( this );

        // Now, load the rootPage, which can be a Leaf or a Node, depending 
        // on the number of elements in the tree : if it's above the pageSize,
        // it's a Node, otherwise it's a Leaf

        // Read the rootPage pages on disk
        PageIO[] rootPageIos = readPageIOs( rootPageOffset, Long.MAX_VALUE );

        Page<K, V> btreeRoot = readPage( btree, rootPageIos );
        BTreeFactory.setRecordManager( btree, this );

        BTreeFactory.setRootPage( btree, btreeRoot );
    }


    private <K, V> Page<K, V> readNode( BTree<K, V> btree, long offset, long revision, int nbElems ) throws IOException
    {
        Page<K, V> node = BTreeFactory.createNode( btree, revision, nbElems );

        // Read the rootPage pages on disk
        PageIO[] pageIos = readPageIOs( offset, Long.MAX_VALUE );

        return node;
    }


    public <K, V> Page<K, V> deserialize( BTree<K, V> btree, long offset ) throws EndOfFileExceededException,
        IOException
    {
        PageIO[] rootPageIos = readPageIOs( offset, Long.MAX_VALUE );

        Page<K, V> page = readPage( btree, rootPageIos );

        ( ( AbstractPage<K, V> ) page ).setOffset( rootPageIos[0].getOffset() );
        ( ( AbstractPage<K, V> ) page ).setLastOffset( rootPageIos[rootPageIos.length - 1].getOffset() );

        return page;
    }


    private <K, V> Page<K, V> readPage( BTree<K, V> btree, PageIO[] pageIos ) throws IOException
    {
        // Deserialize the rootPage now
        long position = 0L;

        // The revision
        long revision = readLong( pageIos, position );
        position += LONG_SIZE;

        // The number of elements in the page
        int nbElems = readInt( pageIos, position );
        position += INT_SIZE;

        // The size of the data containing the keys and values
        Page<K, V> page = null;

        // Reads the bytes containing all the keys and values, if we have some
        // We read  big blog of data into  ByteBuffer, then we will process
        // this ByteBuffer
        ByteBuffer byteBuffer = readBytes( pageIos, position );

        // Now, deserialize the data block. If the number of elements
        // is positive, it's a Leaf, otherwise it's a Node
        // Note that only a leaf can have 0 elements, and it's the root page then.
        if ( nbElems >= 0 )
        {
            // It's a leaf
            page = readLeafKeysAndValues( btree, nbElems, revision, byteBuffer, pageIos );
        }
        else
        {
            // It's a node
            page = readNodeKeysAndValues( btree, -nbElems, revision, byteBuffer, pageIos );
        }

        return page;
    }


    /**
     * Deserialize a Leaf from some PageIOs
     */
    private <K, V> PersistedLeaf<K, V> readLeafKeysAndValues( BTree<K, V> btree, int nbElems, long revision, ByteBuffer byteBuffer,
        PageIO[] pageIos )
    {
        // Its a leaf, create it
        PersistedLeaf<K, V> leaf = BTreeFactory.createLeaf( btree, revision, nbElems );

        // Store the page offset on disk
        leaf.setOffset( pageIos[0].getOffset() );
        leaf.setLastOffset( pageIos[pageIos.length - 1].getOffset() );

        int[] keyLengths = new int[nbElems];
        int[] valueLengths = new int[nbElems];

        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            // Read the number of values
            int nbValues = byteBuffer.getInt();
            PersistedValueHolder<V> valueHolder = null;

            if ( nbValues < 0 )
            {
                // This is a sub-btree
                byte[] btreeOffsetBytes = new byte[LONG_SIZE];
                byteBuffer.get( btreeOffsetBytes );
                
                // Create the valueHolder. As the number of values is negative, we have to switch
                // to a positive value but as we start at -1 for 0 value, add 1.
                valueHolder = new PersistedValueHolder<V>( btree, 1 - nbValues, btreeOffsetBytes );
            }
            else
            {
                // This is an array
                // Read the value's array length
                valueLengths[i] = byteBuffer.getInt();

                // This is an Array of values, read the byte[] associated with it
                byte[] arrayBytes = new byte[valueLengths[i]];
                byteBuffer.get( arrayBytes );
                valueHolder = new PersistedValueHolder<V>( btree, nbValues, arrayBytes );
            }

            BTreeFactory.setValue( leaf, i, valueHolder );

            keyLengths[i] = byteBuffer.getInt();
            byte[] data = new byte[keyLengths[i]];
            byteBuffer.get( data );
            BTreeFactory.setKey( btree, leaf, i, data );
        }

        return leaf;
    }


    /**
     * Deserialize a Node from some PageIos
     */
    private <K, V> Node<K, V> readNodeKeysAndValues( BTree<K, V> btree, int nbElems, long revision, ByteBuffer byteBuffer,
        PageIO[] pageIos ) throws IOException
    {
        Node<K, V> node = BTreeFactory.createNode( btree, revision, nbElems );

        // Read each value and key
        for ( int i = 0; i < nbElems; i++ )
        {
            // This is an Offset
            long offset = OFFSET_SERIALIZER.deserialize( byteBuffer );
            long lastOffset = OFFSET_SERIALIZER.deserialize( byteBuffer );

            PersistedPageHolder<K, V> valueHolder = new PersistedPageHolder<K, V>( btree, null, offset, lastOffset );
            node.setValue( i, valueHolder );

            // Read the key length
            int keyLength = byteBuffer.getInt();
            
            int currentPosition = byteBuffer.position();
            
            // and the key value
            K key = btree.getKeySerializer().deserialize( byteBuffer );
            
            // Set the new position now
            byteBuffer.position( currentPosition + keyLength );
            
            BTreeFactory.setKey( btree, node, i, key );
        }

        // and read the last value, as it's a node
        long offset = OFFSET_SERIALIZER.deserialize( byteBuffer );
        long lastOffset = OFFSET_SERIALIZER.deserialize( byteBuffer );

        PersistedPageHolder<K, V> valueHolder = new PersistedPageHolder<K, V>( btree, null, offset, lastOffset );
        node.setValue( nbElems, valueHolder );

        return node;
    }


    /**
     * Read a byte[] from pages.
     * @param pageIos The pages we want to read the byte[] from
     * @param position The position in the data stored in those pages
     * @return The byte[] we have read
     */
    private ByteBuffer readBytes( PageIO[] pageIos, long position )
    {
        // Read the byte[] length first
        int length = readInt( pageIos, position );
        position += INT_SIZE;

        // Compute the page in which we will store the data given the 
        // current position
        int pageNb = computePageNb( position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * LONG_SIZE + INT_SIZE ) - pageNb * pageSize;

        ByteBuffer pageData = pageIos[pageNb].getData();
        int remaining = pageData.capacity() - pagePos;

        if ( length == 0 )
        {
            // No bytes to read : return null;
            return null;
        }
        else
        {
            ByteBuffer bytes = ByteBuffer.allocate( length );
            int bytesPos = 0;

            while ( length > 0 )
            {
                if ( length <= remaining )
                {
                    pageData.mark();
                    pageData.position( pagePos );
                    int oldLimit = pageData.limit();
                    pageData.limit( pagePos + length );
                    bytes.put( pageData );
                    pageData.limit( oldLimit );
                    pageData.reset();
                    bytes.rewind();

                    return bytes;
                }

                pageData.mark();
                pageData.position( pagePos );
                int oldLimit = pageData.limit();
                pageData.limit( pagePos + remaining );
                bytes.put( pageData );
                pageData.limit( oldLimit );
                pageData.reset();
                pageNb++;
                pagePos = LINK_SIZE;
                bytesPos += remaining;
                pageData = pageIos[pageNb].getData();
                length -= remaining;
                remaining = pageData.capacity() - pagePos;
            }

            bytes.rewind();

            return bytes;
        }
    }


    /**
     * Read an int from pages 
     * @param pageIos The pages we want to read the int from
     * @param position The position in the data stored in those pages
     * @return The int we have read
     */
    private int readInt( PageIO[] pageIos, long position )
    {
        // Compute the page in which we will store the data given the 
        // current position
        int pageNb = computePageNb( position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * LONG_SIZE + INT_SIZE ) - pageNb * pageSize;

        ByteBuffer pageData = pageIos[pageNb].getData();
        int remaining = pageData.capacity() - pagePos;
        int value = 0;

        if ( remaining >= INT_SIZE )
        {
            value = pageData.getInt( pagePos );
        }
        else
        {
            value = 0;

            switch ( remaining )
            {
                case 3:
                    value += ( ( pageData.get( pagePos + 2 ) & 0x00FF ) << 8 );
                    // Fallthrough !!!

                case 2:
                    value += ( ( pageData.get( pagePos + 1 ) & 0x00FF ) << 16 );
                    // Fallthrough !!!

                case 1:
                    value += ( pageData.get( pagePos ) << 24 );
                    break;
            }

            // Now deal with the next page
            pageData = pageIos[pageNb + 1].getData();
            pagePos = LINK_SIZE;

            switch ( remaining )
            {
                case 1:
                    value += ( pageData.get( pagePos ) & 0x00FF ) << 16;
                    // fallthrough !!!

                case 2:
                    value += ( pageData.get( pagePos + 2 - remaining ) & 0x00FF ) << 8;
                    // fallthrough !!!

                case 3:
                    value += ( pageData.get( pagePos + 3 - remaining ) & 0x00FF );
                    break;
            }
        }

        return value;
    }


    /**
     * Read a byte from pages 
     * @param pageIos The pages we want to read the byte from
     * @param position The position in the data stored in those pages
     * @return The byte we have read
     */
    private byte readByte( PageIO[] pageIos, long position )
    {
        // Compute the page in which we will store the data given the 
        // current position
        int pageNb = computePageNb( position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * LONG_SIZE + INT_SIZE ) - pageNb * pageSize;

        ByteBuffer pageData = pageIos[pageNb].getData();
        byte value = 0;

        value = pageData.get( pagePos );

        return value;
    }


    /**
     * Read a long from pages 
     * @param pageIos The pages we want to read the long from
     * @param position The position in the data stored in those pages
     * @return The long we have read
     */
    private long readLong( PageIO[] pageIos, long position )
    {
        // Compute the page in which we will store the data given the 
        // current position
        int pageNb = computePageNb( position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * LONG_SIZE + INT_SIZE ) - pageNb * pageSize;

        ByteBuffer pageData = pageIos[pageNb].getData();
        int remaining = pageData.capacity() - pagePos;
        long value = 0L;

        if ( remaining >= LONG_SIZE )
        {
            value = pageData.getLong( pagePos );
        }
        else
        {
            switch ( remaining )
            {
                case 7:
                    value += ( ( ( long ) pageData.get( pagePos + 6 ) & 0x00FF ) << 8 );
                    // Fallthrough !!!

                case 6:
                    value += ( ( ( long ) pageData.get( pagePos + 5 ) & 0x00FF ) << 16 );
                    // Fallthrough !!!

                case 5:
                    value += ( ( ( long ) pageData.get( pagePos + 4 ) & 0x00FF ) << 24 );
                    // Fallthrough !!!

                case 4:
                    value += ( ( ( long ) pageData.get( pagePos + 3 ) & 0x00FF ) << 32 );
                    // Fallthrough !!!

                case 3:
                    value += ( ( ( long ) pageData.get( pagePos + 2 ) & 0x00FF ) << 40 );
                    // Fallthrough !!!

                case 2:
                    value += ( ( ( long ) pageData.get( pagePos + 1 ) & 0x00FF ) << 48 );
                    // Fallthrough !!!

                case 1:
                    value += ( ( long ) pageData.get( pagePos ) << 56 );
                    break;
            }

            // Now deal with the next page
            pageData = pageIos[pageNb + 1].getData();
            pagePos = LINK_SIZE;

            switch ( remaining )
            {
                case 1:
                    value += ( ( long ) pageData.get( pagePos ) & 0x00FF ) << 48;
                    // fallthrough !!!

                case 2:
                    value += ( ( long ) pageData.get( pagePos + 2 - remaining ) & 0x00FF ) << 40;
                    // fallthrough !!!

                case 3:
                    value += ( ( long ) pageData.get( pagePos + 3 - remaining ) & 0x00FF ) << 32;
                    // fallthrough !!!

                case 4:
                    value += ( ( long ) pageData.get( pagePos + 4 - remaining ) & 0x00FF ) << 24;
                    // fallthrough !!!

                case 5:
                    value += ( ( long ) pageData.get( pagePos + 5 - remaining ) & 0x00FF ) << 16;
                    // fallthrough !!!

                case 6:
                    value += ( ( long ) pageData.get( pagePos + 6 - remaining ) & 0x00FF ) << 8;
                    // fallthrough !!!

                case 7:
                    value += ( ( long ) pageData.get( pagePos + 7 - remaining ) & 0x00FF );
                    break;
            }
        }

        return value;
    }


    /**
     * Manage a BTree. The btree will be added and managed by this RecordManager. We will create a 
     * new RootPage for this added BTree, which will contain no data. 
     *  
     * @param btree The new BTree to manage.
     */
    public synchronized <K, V> void manage( BTree<K, V> btree ) throws BTreeAlreadyManagedException, IOException
    {
        manage( ( BTree<Object, Object> ) btree, NORMAL_BTREE );
    }


    /**
     * works the same as @see #manage(BTree) except the given tree will not be linked to top level trees that will be
     * loaded initially if the internalTree flag is set to true
     * 
     * @param btree The new BTree to manage.
     * @param internalTree flag indicating if this is an internal tree
     * 
     * @throws BTreeAlreadyManagedException
     * @throws IOException
     */
    public synchronized <K, V> void manage( BTree<K, V> btree, boolean internalTree )
        throws BTreeAlreadyManagedException,
        IOException
    {
        LOG.debug( "Managing the btree {} which is an internam tree : {}", btree.getName(), internalTree );
        BTreeFactory.setRecordManager( btree, this );

        String name = btree.getName();

        if ( managedBTrees.containsKey( name ) )
        {
            // There is already a BTree with this name in the recordManager...
            LOG.error( "There is already a BTree named '{}' managed by this recordManager", name );
            throw new BTreeAlreadyManagedException( name );
        }

        // Do not add the BTree if it's internal into the Map of managed btrees, otherwise we will 
        // not discard it when reloading a page wth internal btrees
        if ( !internalTree )
        {
            managedBTrees.put( name, ( BTree<Object, Object> ) btree );
        } 

        // We will add the newly managed BTree at the end of the header.
        byte[] btreeNameBytes = Strings.getBytesUtf8( name );
        byte[] keySerializerBytes = Strings.getBytesUtf8( btree.getKeySerializerFQCN() );
        byte[] valueSerializerBytes = Strings.getBytesUtf8( btree.getValueSerializerFQCN() );

        int bufferSize =
            INT_SIZE + // The name size
                btreeNameBytes.length + // The name
                INT_SIZE + // The keySerializerBytes size 
                keySerializerBytes.length + // The keySerializerBytes
                INT_SIZE + // The valueSerializerBytes size
                valueSerializerBytes.length + // The valueSerializerBytes
                INT_SIZE + // The page size
                LONG_SIZE + // The revision
                LONG_SIZE + // the number of element
                LONG_SIZE + // the nextBtree offset
                LONG_SIZE + // The root offset
                INT_SIZE; // The allowDuplicates flag

        // Get the pageIOs we need to store the data. We may need more than one.
        PageIO[] pageIos = getFreePageIOs( bufferSize );

        // Store the BTree Offset into the BTree
        long btreeOffset = pageIos[0].getOffset();
        ((PersistedBTree<K, V>)btree).setBtreeOffset( btreeOffset );

        // Now store the BTree data in the pages :
        // - the BTree revision
        // - the BTree number of elements
        // - The RootPage offset
        // - The next Btree offset 
        // - the BTree page size
        // - the BTree name
        // - the keySerializer FQCN
        // - the valueSerializer FQCN
        // - the flags that tell if the dups are allowed
        // Starts at 0
        long position = 0L;

        // The BTree current revision
        position = store( position, btree.getRevision(), pageIos );

        // The nb elems in the tree
        position = store( position, btree.getNbElems(), pageIos );

        // Serialize the BTree root page
        Page<K, V> rootPage = BTreeFactory.getRoot( btree );

        PageIO[] rootPageIos = serializePage( btree, btree.getRevision(), rootPage );

        // Get the reference on the first page
        PageIO rootPageIo = rootPageIos[0];

        // Now, we can inject the BTree rootPage offset into the BTree header
        position = store( position, rootPageIo.getOffset(), pageIos );
        ((PersistedBTree<K, V>)btree).setRootPageOffset( rootPageIo.getOffset() );
        ( ( PersistedLeaf<K, V> ) rootPage ).setOffset( rootPageIo.getOffset() );

        // The next BTree Header offset (-1L, as it's a new BTree)
        position = store( position, NO_PAGE, pageIos );

        // The BTree page size
        position = store( position, btree.getPageSize(), pageIos );

        // The tree name
        position = store( position, btreeNameBytes, pageIos );

        // The keySerializer FQCN
        position = store( position, keySerializerBytes, pageIos );

        // The valueSerialier FQCN
        position = store( position, valueSerializerBytes, pageIos );

        // The allowDuplicates flag
        position = store( position, ( btree.isAllowDuplicates() ? 1 : 0 ), pageIos );

        // And flush the pages to disk now
        LOG.debug( "Flushing the newly managed '{}' btree header", btree.getName() );
        flushPages( pageIos );
        LOG.debug( "Flushing the newly managed '{}' btree rootpage", btree.getName() );
        flushPages( rootPageIos );

        // Now, if this added BTree is not the first BTree, we have to link it with the 
        // latest added BTree
        if ( !internalTree )
        {
            nbBtree++;

            if ( lastAddedBTreeOffset != NO_PAGE )
            {
                // We have to update the nextBtreeOffset from the previous BTreeHeader
                pageIos = readPageIOs( lastAddedBTreeOffset, LONG_SIZE + LONG_SIZE + LONG_SIZE + LONG_SIZE );
                store( LONG_SIZE + LONG_SIZE + LONG_SIZE, btreeOffset, pageIos );

                // Write the pages on disk
                LOG.debug( "Updated the previous btree pointer on the added BTree {}", btree.getName() );
                flushPages( pageIos );
            }

            lastAddedBTreeOffset = btreeOffset;

            // Last, not least, update the number of managed BTrees in the header
            updateRecordManagerHeader();
        }

        if ( LOG_CHECK.isDebugEnabled() )
        {
            check();
        }
    }


    /**
     * Serialize a new Page. It will contain the following data :<br/>
     * <ul>
     * <li>the revision : a long</li>
     * <li>the number of elements : an int (if <= 0, it's a Node, otherwise it's a Leaf)</li>
     * <li>the size of the values/keys when serialized
     * <li>the keys : an array of serialized keys</li>
     * <li>the values : an array of references to the children pageIO offset (stored as long)
     * if it's a Node, or a list of values if it's a Leaf</li>
     * <li></li>
     * </ul>
     * 
     * @param revision The node revision
     * @param keys The keys to serialize
     * @param children The references to the children
     * @return An array of pages containing the serialized node
     * @throws IOException
     */
    private <K, V> PageIO[] serializePage( BTree<K, V> btree, long revision, Page<K, V> page ) throws IOException
    {
        int nbElems = page.getNbElems();

        if ( nbElems == 0 )
        {
            return serializeRootPage( revision );
        }
        else
        {
            // Prepare a list of byte[] that will contain the serialized page
            int nbBuffers = 1 + 1 + 1 + nbElems * 3;
            int dataSize = 0;
            int serializedSize = 0;

            if ( page instanceof Node )
            {
                // A Node has one more value to store
                nbBuffers++;
            }

            // Now, we can create the list with the right size
            List<byte[]> serializedData = new ArrayList<byte[]>( nbBuffers );

            // The revision
            byte[] buffer = LongSerializer.serialize( revision );
            serializedData.add( buffer );
            serializedSize += buffer.length;

            // The number of elements
            // Make it a negative value if it's a Node
            int pageNbElems = nbElems;

            if ( page instanceof Node )
            {
                pageNbElems = -nbElems;
            }

            buffer = IntSerializer.serialize( pageNbElems );
            serializedData.add( buffer );
            serializedSize += buffer.length;

            // Iterate on the keys and values. We first serialize the value, then the key
            // until we are done with all of them. If we are serializing a page, we have
            // to serialize one more value
            for ( int pos = 0; pos < nbElems; pos++ )
            {
                // Start with the value
                if ( page instanceof Node )
                {
                    dataSize += serializeNodeValue( ( Node<K, V> ) page, pos, serializedData );
                    dataSize += serializeNodeKey( ( Node<K, V> ) page, pos, serializedData );
                }
                else
                {
                    dataSize += serializeLeafValue( ( PersistedLeaf<K, V> ) page, pos, serializedData );
                    dataSize += serializeLeafKey( ( PersistedLeaf<K, V> ) page, pos, serializedData );
                }
            }

            // Nodes have one more value to serialize
            if ( page instanceof Node )
            {
                dataSize += serializeNodeValue( ( Node<K, V> ) page, nbElems, serializedData );
            }

            // Store the data size
            buffer = IntSerializer.serialize( dataSize );
            serializedData.add( 2, buffer );
            serializedSize += buffer.length;

            serializedSize += dataSize;

            // We are done. Allocate the pages we need to store the data
            PageIO[] pageIos = getFreePageIOs( serializedSize );

            // And store the data into those pages
            long position = 0L;

            for ( byte[] bytes : serializedData )
            {
                position = storeRaw( position, bytes, pageIos );
            }

            return pageIos;
        }
    }


    /**
     * Serialize a Node's key
     */
    private <K, V> int serializeNodeKey( Node<K, V> node, int pos, List<byte[]> serializedData )
    {
        KeyHolder<K> holder = node.getKeyHolder( pos );
        byte[] buffer = ((PersistedKeyHolder<K>)holder).getRaw();
        
        // We have to store the serialized key length
        byte[] length = IntSerializer.serialize( buffer.length );
        serializedData.add( length );

        // And store the serialized key now
        serializedData.add( buffer );

        return buffer.length + INT_SIZE;
    }


    /**
     * Serialize a Node's Value. We store the two offsets of the child page.
     */
    private <K, V> int serializeNodeValue( Node<K, V> node, int pos, List<byte[]> serializedData )
        throws IOException
    {
        // For a node, we just store the children's offsets
        Page<K, V> child = node.getReference( pos );

        // The first offset
        byte[] buffer = LongSerializer.serialize( child.getOffset() );
        serializedData.add( buffer );
        int dataSize = buffer.length;

        // The last offset
        buffer = LongSerializer.serialize( child.getLastOffset() );
        serializedData.add( buffer );
        dataSize += buffer.length;

        return dataSize;
    }


    /**
     * Serialize a Leaf's key
     */
    private <K, V> int serializeLeafKey( PersistedLeaf<K, V> leaf, int pos, List<byte[]> serializedData )
    {
        int dataSize = 0;
        KeyHolder<K> keyHolder = leaf.getKeyHolder( pos );
        byte[] keyData = ((PersistedKeyHolder<K>)keyHolder).getRaw();

        if ( keyData != null )
        {
            // We have to store the serialized key length
            byte[] length = IntSerializer.serialize( keyData.length );
            serializedData.add( length );

            // And the key data
            serializedData.add( keyData );
            dataSize += keyData.length + INT_SIZE;
        }
        else
        {
            serializedData.add( IntSerializer.serialize( 0 ) );
            dataSize += INT_SIZE;
        }

        return dataSize;
    }


    /**
     * Serialize a Leaf's Value. We store 
     */
    private <K, V> int serializeLeafValue( PersistedLeaf<K, V> leaf, int pos, List<byte[]> serializedData )
        throws IOException
    {
        // The value can be an Array or a sub-btree, but we don't care
        // we just iterate on all the values
        ValueHolder<V> valueHolder = leaf.getValue( pos );
        int dataSize = 0;

        if ( !valueHolder.isSubBtree() )
        {
            int nbValues = valueHolder.size();
            
            // Write the nb elements first
            byte[] buffer = IntSerializer.serialize( nbValues );
            serializedData.add( buffer );
            dataSize = INT_SIZE;

            // We have a serialized value. Just flush it
            byte[] data = ((PersistedValueHolder<V>)valueHolder).getRaw();
            dataSize += data.length;
            
            // Store the data size
            buffer = IntSerializer.serialize( data.length );
            serializedData.add( buffer );
            dataSize += INT_SIZE;

            // and add the data
            serializedData.add( data );
        }
        else
        {
            // First take the number of values
            int nbValues = valueHolder.size();
    
            if ( nbValues == 0 )
            {
                // No value. 
                byte[] buffer = IntSerializer.serialize( nbValues );
                serializedData.add( buffer );
    
                return buffer.length;
            }

            if ( valueHolder.isSubBtree() )
            {
                // Store the nbVlues as a negative number. We add 1 so that 0 is not confused with an Array value 
                byte[] buffer = IntSerializer.serialize( -( nbValues + 1 ) );
                serializedData.add( buffer );
                dataSize += buffer.length;
    
                // the BTree offset
                buffer = LongSerializer.serialize( ((PersistedValueHolder<V>)valueHolder).getOffset() );
                serializedData.add( buffer );
                dataSize += buffer.length;
            }
            else
            {
                // This is an array, store the nb of values as a positive number
                byte[] buffer = IntSerializer.serialize( nbValues );
                serializedData.add( buffer );
                dataSize += buffer.length;
    
                // Now store each value
                byte[] data = ((PersistedValueHolder<V>)valueHolder).getRaw();
                buffer = IntSerializer.serialize( data.length );
                serializedData.add( buffer );
                dataSize += buffer.length;
    
                if ( data.length > 0 )
                {
                    serializedData.add( data );
                }
    
                dataSize += data.length;
            }
        }

        return dataSize;
    }


    /**
     * Write a root page with no elements in it
     */
    private PageIO[] serializeRootPage( long revision ) throws IOException
    {
        // We will have 1 single page if we have no elements
        PageIO[] pageIos = new PageIO[1];

        // This is either a new root page or a new page that will be filled later
        PageIO newPage = fetchNewPage();

        // We need first to create a byte[] that will contain all the data
        // For the root page, this is easy, as we only have to store the revision, 
        // and the number of elements, which is 0.
        long position = 0L;

        position = store( position, revision, newPage );
        position = store( position, 0, newPage );

        // Update the page size now
        newPage.setSize( ( int ) position );

        // Insert the result into the array of PageIO
        pageIos[0] = newPage;

        return pageIos;
    }


    /**
     * Update the header, injecting the nbBtree, firstFreePage and lastFreePage
     */
    public void updateRecordManagerHeader() throws IOException
    {
        // The page size
        HEADER_BYTES[0] = ( byte ) ( pageSize >>> 24 );
        HEADER_BYTES[1] = ( byte ) ( pageSize >>> 16 );
        HEADER_BYTES[2] = ( byte ) ( pageSize >>> 8 );
        HEADER_BYTES[3] = ( byte ) ( pageSize );

        // The number of managed BTree (currently we have only one : the discardedPage BTree
        HEADER_BYTES[4] = ( byte ) ( nbBtree >>> 24 );
        HEADER_BYTES[5] = ( byte ) ( nbBtree >>> 16 );
        HEADER_BYTES[6] = ( byte ) ( nbBtree >>> 8 );
        HEADER_BYTES[7] = ( byte ) ( nbBtree );

        // The first free page
        HEADER_BYTES[8] = ( byte ) ( firstFreePage >>> 56 );
        HEADER_BYTES[9] = ( byte ) ( firstFreePage >>> 48 );
        HEADER_BYTES[10] = ( byte ) ( firstFreePage >>> 40 );
        HEADER_BYTES[11] = ( byte ) ( firstFreePage >>> 32 );
        HEADER_BYTES[12] = ( byte ) ( firstFreePage >>> 24 );
        HEADER_BYTES[13] = ( byte ) ( firstFreePage >>> 16 );
        HEADER_BYTES[14] = ( byte ) ( firstFreePage >>> 8 );
        HEADER_BYTES[15] = ( byte ) ( firstFreePage );

        // The last free page
        HEADER_BYTES[16] = ( byte ) ( lastFreePage >>> 56 );
        HEADER_BYTES[17] = ( byte ) ( lastFreePage >>> 48 );
        HEADER_BYTES[18] = ( byte ) ( lastFreePage >>> 40 );
        HEADER_BYTES[19] = ( byte ) ( lastFreePage >>> 32 );
        HEADER_BYTES[20] = ( byte ) ( lastFreePage >>> 24 );
        HEADER_BYTES[21] = ( byte ) ( lastFreePage >>> 16 );
        HEADER_BYTES[22] = ( byte ) ( lastFreePage >>> 8 );
        HEADER_BYTES[23] = ( byte ) ( lastFreePage );

        // Write the header on disk
        HEADER_BUFFER.put( HEADER_BYTES );
        HEADER_BUFFER.flip();

        LOG.debug( "Update RM header, FF : {}, LF : {}", firstFreePage, lastFreePage );
        fileChannel.write( HEADER_BUFFER, 0 );
        HEADER_BUFFER.clear();

        nbUpdateRMHeader.incrementAndGet();
    }


    /**
     * Update the BTree header after a BTree modification. We update the following fields :
     * <ul>
     * <li>the revision</li>
     * <li>the number of elements</li>
     * <li>the rootPage offset</li>
     * </ul>
     * @param btree
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    /* No qualifier*/<K, V> void updateBtreeHeader( BTree<K, V> btree, long rootPageOffset )
        throws EndOfFileExceededException,
        IOException
    {
        // Read the pageIOs associated with this BTree
        long offset = ((PersistedBTree<K, V>)btree).getBtreeOffset();
        long headerSize = LONG_SIZE + LONG_SIZE + LONG_SIZE;

        PageIO[] pageIos = readPageIOs( offset, headerSize );

        // Now, update the revision
        long position = 0;

        position = store( position, btree.getRevision(), pageIos );
        position = store( position, btree.getNbElems(), pageIos );
        position = store( position, rootPageOffset, pageIos );

        // Write the pages on disk
        if ( LOG.isDebugEnabled() )
        {
            LOG.debug( "-----> Flushing the '{}' BTreeHeader", btree.getName() );
            LOG.debug( "  revision : " + btree.getRevision() + ", NbElems : " + btree.getNbElems() + ", root offset : "
                + rootPageOffset );
        }

        flushPages( pageIos );

        nbUpdateBTreeHeader.incrementAndGet();

        if ( LOG_CHECK.isDebugEnabled() )
        {
            check();
        }
    }


    /**
     * Write the pages in the disk, either at the end of the file, or at
     * the position they were taken from.
     * 
     * @param pageIos The list of pages to write
     * @throws IOException If the write failed
     */
    private void flushPages( PageIO... pageIos ) throws IOException
    {
        if ( LOG.isDebugEnabled() )
        {
            for ( PageIO pageIo : pageIos )
            {
                dump( pageIo );
            }
        }

        for ( PageIO pageIo : pageIos )
        {
            pageIo.getData().rewind();

            if ( fileChannel.size() < ( pageIo.getOffset() + pageSize ) )
            {
                LOG.debug( "Adding a page at the end of the file" );
                // This is a page we have to add to the file
                fileChannel.write( pageIo.getData(), fileChannel.size() );
                //fileChannel.force( false );
            }
            else
            {
                LOG.debug( "Writing a page at position {}", pageIo.getOffset() );
                fileChannel.write( pageIo.getData(), pageIo.getOffset() );
                //fileChannel.force( false );
            }

            nbUpdatePageIOs.incrementAndGet();

            pageIo.getData().rewind();
        }
    }


    /**
     * Compute the page in which we will store data given an offset, when 
     * we have a list of pages.
     * 
     * @param offset The position in the data
     * @return The page number in which the offset will start
     */
    private int computePageNb( long offset )
    {
        long pageNb = 0;

        offset -= pageSize - LINK_SIZE - PAGE_SIZE;

        if ( offset < 0 )
        {
            return ( int ) pageNb;
        }

        pageNb = 1 + offset / ( pageSize - LINK_SIZE );

        return ( int ) pageNb;
    }


    /**
     * Stores a byte[] into one ore more pageIO (depending if the long is stored
     * across a boundary or not)
     * 
     * @param position The position in a virtual byte[] if all the pages were contiguous
     * @param bytes The byte[] to serialize
     * @param pageIos The pageIOs we have to store the data in
     * @return The new position
     */
    private long store( long position, byte[] bytes, PageIO... pageIos )
    {
        if ( bytes != null )
        {
            // Write the bytes length
            position = store( position, bytes.length, pageIos );

            // Compute the page in which we will store the data given the 
            // current position
            int pageNb = computePageNb( position );

            // Get back the buffer in this page
            ByteBuffer pageData = pageIos[pageNb].getData();

            // Compute the position in the current page
            int pagePos = ( int ) ( position + ( pageNb + 1 ) * LONG_SIZE + INT_SIZE ) - pageNb * pageSize;

            // Compute the remaining size in the page
            int remaining = pageData.capacity() - pagePos;
            int nbStored = bytes.length;

            // And now, write the bytes until we have none
            while ( nbStored > 0 )
            {
                if ( remaining > nbStored )
                {
                    pageData.mark();
                    pageData.position( pagePos );
                    pageData.put( bytes, bytes.length - nbStored, nbStored );
                    pageData.reset();
                    nbStored = 0;
                }
                else
                {
                    pageData.mark();
                    pageData.position( pagePos );
                    pageData.put( bytes, bytes.length - nbStored, remaining );
                    pageData.reset();
                    pageNb++;
                    pageData = pageIos[pageNb].getData();
                    pagePos = LINK_SIZE;
                    nbStored -= remaining;
                    remaining = pageData.capacity() - pagePos;
                }
            }

            // We are done
            position += bytes.length;
        }
        else
        {
            // No bytes : write 0 and return
            position = store( position, 0, pageIos );
        }

        return position;
    }


    /**
     * Stores a byte[] into one ore more pageIO (depending if the long is stored
     * across a boundary or not). We don't add the byte[] size, it's already present
     * in the received byte[].
     * 
     * @param position The position in a virtual byte[] if all the pages were contiguous
     * @param bytes The byte[] to serialize
     * @param pageIos The pageIOs we have to store the data in
     * @return The new position
     */
    private long storeRaw( long position, byte[] bytes, PageIO... pageIos )
    {
        if ( bytes != null )
        {
            // Compute the page in which we will store the data given the 
            // current position
            int pageNb = computePageNb( position );

            // Get back the buffer in this page
            ByteBuffer pageData = pageIos[pageNb].getData();

            // Compute the position in the current page
            int pagePos = ( int ) ( position + ( pageNb + 1 ) * LONG_SIZE + INT_SIZE ) - pageNb * pageSize;

            // Compute the remaining size in the page
            int remaining = pageData.capacity() - pagePos;
            int nbStored = bytes.length;

            // And now, write the bytes until we have none
            while ( nbStored > 0 )
            {
                if ( remaining > nbStored )
                {
                    pageData.mark();
                    pageData.position( pagePos );
                    pageData.put( bytes, bytes.length - nbStored, nbStored );
                    pageData.reset();
                    nbStored = 0;
                }
                else
                {
                    pageData.mark();
                    pageData.position( pagePos );
                    pageData.put( bytes, bytes.length - nbStored, remaining );
                    pageData.reset();
                    pageNb++;

                    if ( pageNb == pageIos.length )
                    {
                        // We can stop here : we have reach the end of the page
                        break;
                    }

                    pageData = pageIos[pageNb].getData();
                    pagePos = LINK_SIZE;
                    nbStored -= remaining;
                    remaining = pageData.capacity() - pagePos;
                }
            }

            // We are done
            position += bytes.length;
        }
        else
        {
            // No bytes : write 0 and return
            position = store( position, 0, pageIos );
        }

        return position;
    }


    /**
     * Stores an Integer into one ore more pageIO (depending if the int is stored
     * across a boundary or not)
     * 
     * @param position The position in a virtual byte[] if all the pages were contiguous
     * @param value The int to serialize
     * @param pageIos The pageIOs we have to store the data in
     * @return The new position
     */
    private long store( long position, int value, PageIO... pageIos )
    {
        // Compute the page in which we will store the data given the 
        // current position
        int pageNb = computePageNb( position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * LONG_SIZE + INT_SIZE ) - pageNb * pageSize;

        // Get back the buffer in this page
        ByteBuffer pageData = pageIos[pageNb].getData();

        // Compute the remaining size in the page
        int remaining = pageData.capacity() - pagePos;

        if ( remaining < INT_SIZE )
        {
            // We have to copy the serialized length on two pages

            switch ( remaining )
            {
                case 3:
                    pageData.put( pagePos + 2, ( byte ) ( value >>> 8 ) );
                    // Fallthrough !!!

                case 2:
                    pageData.put( pagePos + 1, ( byte ) ( value >>> 16 ) );
                    // Fallthrough !!!

                case 1:
                    pageData.put( pagePos, ( byte ) ( value >>> 24 ) );
                    break;
            }

            // Now deal with the next page
            pageData = pageIos[pageNb + 1].getData();
            pagePos = LINK_SIZE;

            switch ( remaining )
            {
                case 1:
                    pageData.put( pagePos, ( byte ) ( value >>> 16 ) );
                    // fallthrough !!!

                case 2:
                    pageData.put( pagePos + 2 - remaining, ( byte ) ( value >>> 8 ) );
                    // fallthrough !!!

                case 3:
                    pageData.put( pagePos + 3 - remaining, ( byte ) ( value ) );
                    break;
            }
        }
        else
        {
            // Store the value in the page at the selected position
            pageData.putInt( pagePos, value );
        }

        // Increment the position to reflect the addition of an Int (4 bytes)
        position += INT_SIZE;

        return position;
    }


    /**
     * Stores a Long into one ore more pageIO (depending if the long is stored
     * across a boundary or not)
     * 
     * @param position The position in a virtual byte[] if all the pages were contiguous
     * @param value The long to serialize
     * @param pageIos The pageIOs we have to store the data in
     * @return The new position
     */
    private long store( long position, long value, PageIO... pageIos )
    {
        // Compute the page in which we will store the data given the 
        // current position
        int pageNb = computePageNb( position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * LONG_SIZE + INT_SIZE ) - pageNb * pageSize;

        // Get back the buffer in this page
        ByteBuffer pageData = pageIos[pageNb].getData();

        // Compute the remaining size in the page
        int remaining = pageData.capacity() - pagePos;

        if ( remaining < LONG_SIZE )
        {
            // We have to copy the serialized length on two pages

            switch ( remaining )
            {
                case 7:
                    pageData.put( pagePos + 6, ( byte ) ( value >>> 8 ) );
                    // Fallthrough !!!

                case 6:
                    pageData.put( pagePos + 5, ( byte ) ( value >>> 16 ) );
                    // Fallthrough !!!

                case 5:
                    pageData.put( pagePos + 4, ( byte ) ( value >>> 24 ) );
                    // Fallthrough !!!

                case 4:
                    pageData.put( pagePos + 3, ( byte ) ( value >>> 32 ) );
                    // Fallthrough !!!

                case 3:
                    pageData.put( pagePos + 2, ( byte ) ( value >>> 40 ) );
                    // Fallthrough !!!

                case 2:
                    pageData.put( pagePos + 1, ( byte ) ( value >>> 48 ) );
                    // Fallthrough !!!

                case 1:
                    pageData.put( pagePos, ( byte ) ( value >>> 56 ) );
                    break;
            }

            // Now deal with the next page
            pageData = pageIos[pageNb + 1].getData();
            pagePos = LINK_SIZE;

            switch ( remaining )
            {
                case 1:
                    pageData.put( pagePos, ( byte ) ( value >>> 48 ) );
                    // fallthrough !!!

                case 2:
                    pageData.put( pagePos + 2 - remaining, ( byte ) ( value >>> 40 ) );
                    // fallthrough !!!

                case 3:
                    pageData.put( pagePos + 3 - remaining, ( byte ) ( value >>> 32 ) );
                    // fallthrough !!!

                case 4:
                    pageData.put( pagePos + 4 - remaining, ( byte ) ( value >>> 24 ) );
                    // fallthrough !!!

                case 5:
                    pageData.put( pagePos + 5 - remaining, ( byte ) ( value >>> 16 ) );
                    // fallthrough !!!

                case 6:
                    pageData.put( pagePos + 6 - remaining, ( byte ) ( value >>> 8 ) );
                    // fallthrough !!!

                case 7:
                    pageData.put( pagePos + 7 - remaining, ( byte ) ( value ) );
                    break;
            }
        }
        else
        {
            // Store the value in the page at the selected position
            pageData.putLong( pagePos, value );
        }

        // Increment the position to reflect the addition of an Long (8 bytes)
        position += LONG_SIZE;

        return position;
    }


    /**
     * Stores a new page on disk. We will add the modified page into the tree of copied pages.
     * The new page is serialized and saved on disk.
     * 
     * @param oldPage
     * @param oldRevision
     * @param newPage
     * @param newRevision
     * @return The offset of the new page
     * @throws IOException 
     */
    /* No qualifier*/<K, V> PersistedPageHolder<K, V> writePage( BTree<K, V> btree, Page<K, V> newPage,
        long newRevision )
        throws IOException
    {
        // We first need to save the new page on disk
        PageIO[] pageIos = serializePage( btree, newRevision, newPage );
        
        LOG.debug( "Write data for '{}' btree ", btree.getName() );
        
        // Write the page on disk
        flushPages( pageIos );

        // Build the resulting reference
        long offset = pageIos[0].getOffset();
        long lastOffset = pageIos[pageIos.length - 1].getOffset();
        PersistedPageHolder<K, V> valueHolder = new PersistedPageHolder<K, V>( btree, newPage, offset,
            lastOffset );

        if ( LOG_CHECK.isDebugEnabled() )
        {
            check();
        }

        return valueHolder;
    }


    /**
     * Compute the number of pages needed to store some specific size of data.
     * 
     * @param dataSize The size of the data we want to store in pages
     * @return The number of pages needed
     */
    private int computeNbPages( int dataSize )
    {
        if ( dataSize <= 0 )
        {
            return 0;
        }

        // Compute the number of pages needed.
        // Considering that each page can contain PageSize bytes,
        // but that the first 8 bytes are used for links and we 
        // use 4 bytes to store the data size, the number of needed
        // pages is :
        // NbPages = ( (dataSize - (PageSize - 8 - 4 )) / (PageSize - 8) ) + 1 
        // NbPages += ( if (dataSize - (PageSize - 8 - 4 )) % (PageSize - 8) > 0 : 1 : 0 )
        int availableSize = ( pageSize - LONG_SIZE );
        int nbNeededPages = 1;

        // Compute the number of pages that will be full but the first page
        if ( dataSize > availableSize - INT_SIZE )
        {
            int remainingSize = dataSize - ( availableSize - INT_SIZE );
            nbNeededPages += remainingSize / availableSize;
            int remain = remainingSize % availableSize;

            if ( remain > 0 )
            {
                nbNeededPages++;
            }
        }

        return nbNeededPages;
    }


    /**
     * Get as many pages as needed to store the data of the given size
     *  
     * @param dataSize The data size
     * @return An array of pages, enough to store the full data
     */
    private PageIO[] getFreePageIOs( int dataSize ) throws IOException
    {
        if ( dataSize == 0 )
        {
            return new PageIO[]
                {};
        }

        int nbNeededPages = computeNbPages( dataSize );

        PageIO[] pageIOs = new PageIO[nbNeededPages];

        // The first page : set the size
        pageIOs[0] = fetchNewPage();
        pageIOs[0].setSize( dataSize );

        for ( int i = 1; i < nbNeededPages; i++ )
        {
            pageIOs[i] = fetchNewPage();

            // Create the link
            pageIOs[i - 1].setNextPage( pageIOs[i].getOffset() );
        }

        return pageIOs;
    }


    /**
     * Return a new Page. We take one of the existing free pages, or we create
     * a new page at the end of the file.
     * 
     * @return The fetched PageIO
     */
    private PageIO fetchNewPage() throws IOException
    {
        if ( firstFreePage == NO_PAGE )
        {
            nbCreatedPages.incrementAndGet();

            // We don't have any free page. Reclaim some new page at the end
            // of the file
            PageIO newPage = new PageIO( endOfFileOffset );

            endOfFileOffset += pageSize;

            ByteBuffer data = ByteBuffer.allocateDirect( pageSize );

            newPage.setData( data );
            newPage.setNextPage( NO_PAGE );
            newPage.setSize( 0 );

            LOG.debug( "Requiring a new page at offset {}", newPage.getOffset() );

            return newPage;
        }
        else
        {
            nbReusedPages.incrementAndGet();

            // We have some existing free page. Fetch it from disk
            PageIO pageIo = fetchPage( firstFreePage );

            // Update the firstFreePage pointer
            firstFreePage = pageIo.getNextPage();

            // overwrite the data of old page
            ByteBuffer data = ByteBuffer.allocateDirect( pageSize );
            pageIo.setData( data );

            pageIo.setNextPage( NO_PAGE );
            pageIo.setSize( 0 );

            LOG.debug( "Reused page at offset {}", pageIo.getOffset() );

            // If we don't have any more free page, update the last free page pointer too
            if ( firstFreePage == NO_PAGE )
            {
                lastFreePage = NO_PAGE;
            }

            return pageIo;
        }
    }


    /**
     * fetch a page from disk, knowing its position in the file.
     * 
     * @param offset The position in the file
     * @return The found page
     */
    private PageIO fetchPage( long offset ) throws IOException, EndOfFileExceededException
    {
        if ( fileChannel.size() < offset + pageSize )
        {
            // Error : we are past the end of the file
            throw new EndOfFileExceededException( "We are fetching a page on " + offset +
                " when the file's size is " + fileChannel.size() );
        }
        else
        {
            // Read the page
            fileChannel.position( offset );

            ByteBuffer data = ByteBuffer.allocate( pageSize );
            fileChannel.read( data );
            data.rewind();

            PageIO readPage = new PageIO( offset );
            readPage.setData( data );

            return readPage;
        }
    }


    /**
     * @return the pageSize
     */
    public int getPageSize()
    {
        return pageSize;
    }


    public void setPageSize( int pageSize )
    {
        if ( this.pageSize != -1 )
        {
        }
        else
        {
            this.pageSize = pageSize;
        }
    }


    /**
     * Close the RecordManager and flush everything on disk
     */
    public void close() throws IOException
    {
        // TODO : we must wait for the last write to finish

        for ( BTree<Object, Object> tree : managedBTrees.values() )
        {
            tree.close();
        }

        managedBTrees.clear();

        // Write the data
        fileChannel.force( true );

        // And close the channel
        fileChannel.close();
    }
    
    
    /** Hex chars */
    private static final byte[] HEX_CHAR = new byte[]
        { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    
    public static String dump( byte octet )
    {
        return new String( new byte[]
            { HEX_CHAR[( octet & 0x00F0 ) >> 4], HEX_CHAR[octet & 0x000F] } );
    }

    /**
     * Dump a pageIO
     */
    private void dump( PageIO pageIo )
    {
        ByteBuffer buffer = pageIo.getData();
        buffer.mark();
        byte[] longBuffer = new byte[LONG_SIZE];
        byte[] intBuffer = new byte[INT_SIZE]; 
        
        // get the next page offset
        buffer.get( longBuffer );
        long nextOffset = LongSerializer.deserialize( longBuffer );
        
        // Get the data size 
        buffer.get( intBuffer );
        int size = IntSerializer.deserialize( intBuffer );
        
        buffer.reset();
        
        System.out.println( "PageIO[" + Long.toHexString( pageIo.getOffset() ) + "], size = " + size + ", NEXT PageIO:" + Long.toHexString( nextOffset ) );
        System.out.println( " 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F " );
        System.out.println( "+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+" );
        
        int position = buffer.position();
        
        for ( int i = 0; i < buffer.limit(); i+= 16 )
        {
            System.out.print( "|" );
            
            for ( int j = 0; j < 16; j++ )
            {
                System.out.print( dump( buffer.get() ) );
                
                if ( j == 15 )
                {
                    System.out.println( "|" );
                }
                else
                {
                    System.out.print( " " );
                }
            }
        }
        
        System.out.println( "+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+" );

        buffer.reset();
    }


    /**
     * Dump the RecordManager file
     * @throws IOException 
     */
    public void dump() throws IOException
    {
        RandomAccessFile randomFile = new RandomAccessFile( file, "r" );
        FileChannel fileChannel = randomFile.getChannel();

        ByteBuffer header = ByteBuffer.allocate( HEADER_SIZE );

        // load the header 
        fileChannel.read( header );

        header.rewind();

        // The page size
        int pageSize = header.getInt();

        // The number of managed BTrees
        int nbBTree = header.getInt();

        // The first and last free page
        long firstFreePage = header.getLong();
        long lastFreePage = header.getLong();

        if ( LOG.isDebugEnabled() )
        {
            LOG.debug( "RecordManager" );
            LOG.debug( "-------------" );
            LOG.debug( "  Header " );
            LOG.debug( "    '{}'", Strings.dumpBytes( header.array() ) );
            LOG.debug( "    page size : {}", pageSize );
            LOG.debug( "    nbTree : {}", nbBTree );
            LOG.debug( "    firstFreePage : {}", firstFreePage );
            LOG.debug( "    lastFreePage : {}", lastFreePage );
        }

        long position = HEADER_SIZE;

        // Dump the BTrees
        for ( int i = 0; i < nbBTree; i++ )
        {
            LOG.debug( "  Btree[{}]", i );
            PageIO[] pageIos = readPageIOs( position, Long.MAX_VALUE );

            for ( PageIO pageIo : pageIos )
            {
                LOG.debug( "    {}", pageIo );
            }
        }

        randomFile.close();
    }


    /**
     * Get the number of managed trees. We don't count the CopiedPage BTree. and the Revsion BTree
     * 
     * @return The number of managed BTrees
     */
    public int getNbManagedTrees()
    {
        return nbBtree - 2;
    }


    /**
     * Get the managed trees. We don't return the CopiedPage BTree nor the Revision BTree.
     * 
     * @return The managed BTrees
     */
    public Set<String> getManagedTrees()
    {
        Set<String> btrees = new HashSet<String>( managedBTrees.keySet() );

        btrees.remove( COPIED_PAGE_BTREE_NAME );
        btrees.remove( REVISION_BTREE_NAME );

        return btrees;
    }


    /**
     * Store a reference to an old rootPage into the Revision BTree
     * 
     * @param btree The BTree we want to keep an old RootPage for
     * @param rootPage The old rootPage
     * @throws IOException If we have an issue while writing on disk
     */
    /* No qualifier */<K, V> void storeRootPage( BTree<K, V> btree, Page<K, V> rootPage ) throws IOException
    {
        if ( !isKeepRevisions() )
        {
            return;
        }

        if ( ( btree == copiedPageBTree ) || ( btree == revisionBTree ) )
        {
            return;
        }

        RevisionName revisionName = new RevisionName( rootPage.getRevision(), btree.getName() );

        revisionBTree.insert( revisionName, rootPage.getOffset(), 0 );

        if ( LOG_CHECK.isDebugEnabled() )
        {
            check();
        }
    }


    /**
     * Fetch the rootPage of a given BTree for a given revision.
     * 
     * @param btree The BTree we are interested in
     * @param revision The revision we want to get back
     * @return The rootPage for this BTree and this revision, if any
     * @throws KeyNotFoundException If we can't find the rootPage for this revision and this BTree
     * @throws IOException If we had an ise while accessing the data on disk
     */
    /* No qualifier */<K, V> Page<K, V> getRootPage( BTree<K, V> btree, long revision ) throws KeyNotFoundException,
        IOException
    {
        if ( btree.getRevision() == revision )
        {
            // We are asking for the current revision
            return btree.getRootPage();
        }

        RevisionName revisionName = new RevisionName( revision, btree.getName() );
        long rootPageOffset = revisionBTree.get( revisionName );

        // Read the rootPage pages on disk
        PageIO[] rootPageIos = readPageIOs( rootPageOffset, Long.MAX_VALUE );

        Page<K, V> btreeRoot = readPage( btree, rootPageIos );

        return btreeRoot;
    }


    /**
     * Get one managed trees, knowing its name. 
     * 
     * @return The managed BTrees
     */
    public <K, V> BTree<K, V> getManagedTree( String name )
    {
        return ( BTree<K, V> ) managedBTrees.get( name );
    }


    /**
     * Move a list of pages to the free page list. A logical page is associated with on 
     * or physical PageIO, which are on the disk. We have to move all those PagIO instance
     * to the free list, and do the same in memory (we try to keep a reference to a set of 
     * free pages.
     *  
     * @param btree The BTree which were owning the pages
     * @param pages The pages to free
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    /* Package protected */<K, V> void addFreePages( BTree<K, V> btree, List<Page<K, V>> pages )
        throws EndOfFileExceededException,
        IOException
    {
        if ( ( btree == copiedPageBTree ) || ( btree == revisionBTree ) )
        {
            return;
        }

        if ( ( pages == null ) || pages.isEmpty() )
        {
            return;
        }

        if ( !keepRevisions )
        {
            // if the btree doesn't keep revisions, we can safely move
            // the pages to the free page list.
            // NOTE : potential improvement : we can update the header only when
            // we have processed all the logical pages.

            for ( Page<K, V> page : pages )
            {
                // Retrieve all the PageIO associated with this logical page
                long firstOffset = page.getOffset();

                // skip the page with offset 0, this is the first in-memory root page that
                // was copied during first insert in a BTree.
                // a Node or Leaf will *never* have 0 or -1 as its offset 
                if ( firstOffset == NO_PAGE )
                {
                    continue;
                }

                long lastOffset = page.getLastOffset();

                // Update the pointers
                if ( firstFreePage == NO_PAGE )
                {
                    // We don't have yet any free pageIos. The
                    // PageIOs for this Page will be used
                    firstFreePage = firstOffset;
                }
                else
                {
                    // We add the Page's PageIOs before the 
                    // existing free pages.
                    long offset = page.getLastOffset();

                    if ( offset == NO_PAGE )
                    {
                        offset = page.getOffset();
                    }

                    // Fetch the pageIO
                    PageIO pageIo = fetchPage( offset );

                    // Link it to the first free page
                    pageIo.setNextPage( firstFreePage );

                    LOG.debug( "Flushing the first free page" );

                    // And flush it to disk
                    flushPages( pageIo );

                    // We can update the lastFreePage offset 
                    firstFreePage = firstOffset;
                }
            }
        }
        else
        {
            LOG.debug( "We should not get there" );

            for ( Page<K, V> p : pages )
            {
                addFreePage( btree, p );
            }
        }
    }


    /**
     * 
     * TODO addFreePage.
     *
     * @param btree
     * @param freePage
     */
    private <K, V> void addFreePage( BTree<K, V> btree, Page<K, V> freePage )
    {
        try
        {
            RevisionName revision = new RevisionName( freePage.getRevision(), btree.getName() );
            long[] offsetArray = null;

            if ( copiedPageBTree.hasKey( revision ) )
            {
                offsetArray = copiedPageBTree.get( revision );
                long[] tmp = new long[offsetArray.length + 1];
                System.arraycopy( offsetArray, 0, tmp, 0, offsetArray.length );
                offsetArray = tmp;
            }
            else
            {
                offsetArray = new long[1];
            }

            offsetArray[offsetArray.length - 1] = freePage.getOffset();

            copiedPageBTree.insert( revision, offsetArray, 0 );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
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
     * Creates a BTree and automatically adds it to the list of managed btrees
     * 
     * @param name the name of the BTree
     * @param keySerializer key serializer
     * @param valueSerializer value serializer
     * @param allowDuplicates flag for allowing duplicate keys 
     * @return a managed BTree
     * @throws IOException
     * @throws BTreeAlreadyManagedException
     */
    @SuppressWarnings("all")
    public <K, V> BTree<K, V> addBTree( String name, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer,
        boolean allowDuplicates ) throws IOException, BTreeAlreadyManagedException
    {
        BTreeConfiguration config = new BTreeConfiguration();

        config.setName( name );
        config.setKeySerializer( keySerializer );
        config.setValueSerializer( valueSerializer );
        config.setAllowDuplicates( allowDuplicates );

        BTree btree = new PersistedBTree( config );
        manage( btree );

        if ( LOG_CHECK.isDebugEnabled() )
        {
            check();
        }

        return btree;
    }


    private void setCheckedPage( long[] checkedPages, long offset, int pageSize )
    {
        long pageOffset = ( offset - HEADER_SIZE ) / pageSize;
        int index = ( int ) ( pageOffset / 64L );
        long mask = ( 1L << ( pageOffset % 64L ) );
        long bits = checkedPages[index];

        if ( ( bits & mask ) == 1 )
        {
            throw new RuntimeException( "The page at : " + offset + " has already been checked" );
        }

        checkedPages[index] |= mask;

    }


    /**
     * Check the free pages
     * 
     * @param checkedPages
     * @throws IOException 
     */
    private void checkFreePages( long[] checkedPages, int pageSize, long firstFreePage, long lastFreePage )
        throws IOException
    {
        if ( firstFreePage == NO_PAGE )
        {
            if ( lastFreePage == NO_PAGE )
            {
                return;
            }
            else
            {
                throw new RuntimeException( "Wrong last free page : " + lastFreePage );
            }
        }

        if ( lastFreePage != NO_PAGE )
        {
            throw new RuntimeException( "Wrong last free page : " + lastFreePage );
        }

        // Now, read all the free pages
        long currentOffset = firstFreePage;
        long fileSize = fileChannel.size();

        while ( currentOffset != NO_PAGE )
        {
            if ( currentOffset > fileSize )
            {
                throw new RuntimeException( "Wrong free page offset, above file size : " + currentOffset );
            }

            try
            {
                PageIO pageIo = fetchPage( currentOffset );

                if ( currentOffset != pageIo.getOffset() )
                {
                    throw new RuntimeException( "PageIO offset is incorrect : " + currentOffset + "-"
                        + pageIo.getOffset() );
                }

                setCheckedPage( checkedPages, currentOffset, pageSize );

                long newOffset = pageIo.getNextPage();
                currentOffset = newOffset;
            }
            catch ( IOException ioe )
            {
                throw new RuntimeException( "Cannot fetch page at : " + currentOffset );
            }
        }
    }


    /**
     * Check the root page for a given BTree
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    private void checkRoot( long[] checkedPages, long offset, int pageSize, long nbBTreeElems,
        ElementSerializer keySerializer, ElementSerializer valueSerializer, boolean allowDuplicates )
        throws EndOfFileExceededException, IOException
    {
        // Read the rootPage pages on disk
        PageIO[] rootPageIos = readPageIOs( offset, Long.MAX_VALUE );

        // Deserialize the rootPage now
        long position = 0L;

        // The revision
        long revision = readLong( rootPageIos, position );
        position += LONG_SIZE;

        // The number of elements in the page
        int nbElems = readInt( rootPageIos, position );
        position += INT_SIZE;

        // The size of the data containing the keys and values
        ByteBuffer byteBuffer = null;

        // Reads the bytes containing all the keys and values, if we have some
        ByteBuffer data = readBytes( rootPageIos, position );

        if ( nbElems >= 0 )
        {
            // Its a leaf

            // Check the page offset
            long pageOffset = rootPageIos[0].getOffset();

            if ( ( pageOffset < 0 ) || ( pageOffset > fileChannel.size() ) )
            {
                throw new RuntimeException( "The page offset is incorrect : " + pageOffset );
            }

            // Check the page last offset
            long pageLastOffset = rootPageIos[rootPageIos.length - 1].getOffset();

            if ( ( pageLastOffset <= 0 ) || ( pageLastOffset > fileChannel.size() ) )
            {
                throw new RuntimeException( "The page last offset is incorrect : " + pageLastOffset );
            }

            // Read each value and key
            for ( int i = 0; i < nbElems; i++ )
            {
                // Just deserialize all the keys and values
                if ( allowDuplicates )
                {
                    /*
                    long value = OFFSET_SERIALIZER.deserialize( byteBuffer );

                    rootPageIos = readPageIOs( value, Long.MAX_VALUE );

                    BTree dupValueContainer = BTreeFactory.createBTree();
                    dupValueContainer.setBtreeOffset( value );

                    try
                    {
                        loadBTree( pageIos, dupValueContainer );
                    }
                    catch ( Exception e )
                    {
                        // should not happen
                        throw new RuntimeException( e );
                    }
                    */
                }
                else
                {
                    valueSerializer.deserialize( byteBuffer );
                }

                keySerializer.deserialize( byteBuffer );
            }
        }
        else
        {
            /*
            // It's a node
            int nodeNbElems = -nbElems;

            // Read each value and key
            for ( int i = 0; i < nodeNbElems; i++ )
            {
                // This is an Offset
                long offset = OFFSET_SERIALIZER.deserialize( byteBuffer );
                long lastOffset = OFFSET_SERIALIZER.deserialize( byteBuffer );

                ElementHolder valueHolder = new ReferenceHolder( btree, null, offset, lastOffset );
                ( ( Node ) page ).setValue( i, valueHolder );

                Object key = btree.getKeySerializer().deserialize( byteBuffer );
                BTreeFactory.setKey( page, i, key );
            }

            // and read the last value, as it's a node
            long offset = OFFSET_SERIALIZER.deserialize( byteBuffer );
            long lastOffset = OFFSET_SERIALIZER.deserialize( byteBuffer );

            ElementHolder valueHolder = new ReferenceHolder( btree, null, offset, lastOffset );
            ( ( Node ) page ).setValue( nodeNbElems, valueHolder );*/
        }
    }


    /**
     * Check a BTree
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws ClassNotFoundException 
     */
    private long checkBTree( long[] checkedPages, PageIO[] pageIos, int pageSize, boolean isLast )
        throws EndOfFileExceededException, IOException, InstantiationException, IllegalAccessException,
        ClassNotFoundException
    {
        long dataPos = 0L;

        // The BTree current revision
        long revision = readLong( pageIos, dataPos );
        dataPos += LONG_SIZE;

        // The nb elems in the tree
        long nbElems = readLong( pageIos, dataPos );
        dataPos += LONG_SIZE;

        // The BTree rootPage offset
        long rootPageOffset = readLong( pageIos, dataPos );

        if ( ( rootPageOffset < 0 ) || ( rootPageOffset > fileChannel.size() ) )
        {
            throw new RuntimeException( "The rootpage is incorrect : " + rootPageOffset );
        }

        dataPos += LONG_SIZE;

        // The next BTree offset
        long nextBTreeOffset = readLong( pageIos, dataPos );

        if ( ( ( rootPageOffset < 0 ) && ( !isLast ) ) || ( nextBTreeOffset > fileChannel.size() ) )
        {
            throw new RuntimeException( "The rootpage is incorrect : " + rootPageOffset );
        }

        dataPos += LONG_SIZE;

        // The BTree page size
        int btreePageSize = readInt( pageIos, dataPos );

        if ( ( btreePageSize < 2 ) || ( ( btreePageSize & ( ~btreePageSize + 1 ) ) != btreePageSize ) )
        {
            throw new RuntimeException( "The BTree page size is not a power of 2 : " + btreePageSize );
        }

        dataPos += INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = readBytes( pageIos, dataPos );
        dataPos += INT_SIZE;

        dataPos += btreeNameBytes.limit();
        String btreeName = Strings.utf8ToString( btreeNameBytes );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = readBytes( pageIos, dataPos );

        String keySerializerFqcn = null;
        dataPos += INT_SIZE;

        if ( keySerializerBytes != null )
        {
            dataPos += keySerializerBytes.limit();
            keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );
        }
        else
        {
            keySerializerFqcn = "";
        }

        // The valueSerialier FQCN
        ByteBuffer valueSerializerBytes = readBytes( pageIos, dataPos );

        String valueSerializerFqcn = null;
        dataPos += INT_SIZE;

        if ( valueSerializerBytes != null )
        {
            dataPos += valueSerializerBytes.limit();
            valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );
        }
        else
        {
            valueSerializerFqcn = "";
        }

        // The BTree allowDuplicates flag
        int allowDuplicates = readInt( pageIos, dataPos );
        dataPos += INT_SIZE;

        // Now, check the rootPage, which can be a Leaf or a Node, depending 
        // on the number of elements in the tree : if it's above the pageSize,
        // it's a Node, otherwise it's a Leaf
        Class<?> valueSerializer = Class.forName( valueSerializerFqcn );
        Class<?> keySerializer = Class.forName( keySerializerFqcn );

        /*
        checkRoot( checkedPages, rootPageOffset, pageSize, nbElems,
            ( ElementSerializer<?> ) keySerializer.newInstance(),
            ( ElementSerializer<?> ) valueSerializer.newInstance(), allowDuplicates != 0 );
        */

        return nextBTreeOffset;
    }


    /**
     * Check each BTree we manage
     * @throws IOException 
     * @throws EndOfFileExceededException 
     * @throws ClassNotFoundException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    private void checkBTrees( long[] checkedPages, int pageSize, int nbBTrees ) throws EndOfFileExceededException,
        IOException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        // Iterate on each BTree until we have exhausted all of them. The number
        // of btrees is just used to check that we have the correct number
        // of stored BTrees, as they are all linked.
        long position = HEADER_SIZE;

        for ( int i = 0; i < nbBTrees; i++ )
        {
            // Load the pageIOs containing the BTree
            PageIO[] pageIos = readPageIOs( position, Long.MAX_VALUE );

            // Check that they are correctly linked and not already used
            int pageNb = 0;

            for ( PageIO currentPageIo : pageIos )
            {
                // 
                long nextPageOffset = currentPageIo.getNextPage();

                if ( pageNb == pageIos.length - 1 )
                {
                    if ( nextPageOffset != NO_PAGE )
                    {
                        throw new RuntimeException( "The pointer to the next page is not valid, expected NO_PAGE" );
                    }
                }
                else
                {
                    if ( nextPageOffset == NO_PAGE )
                    {
                        throw new RuntimeException( "The pointer to the next page is not valid, NO_PAGE" );
                    }
                }

                if ( ( nextPageOffset != NO_PAGE ) && ( ( nextPageOffset - HEADER_SIZE ) % pageSize != 0 ) )
                {
                    throw new RuntimeException( "The pointer to the next page is not valid" );
                }

                // Update the array of processed pages
                setCheckedPage( checkedPages, currentPageIo.getOffset(), pageSize );
            }

            // Now check the BTree
            long nextBTree = checkBTree( checkedPages, pageIos, pageSize, i == nbBTrees - 1 );

            if ( ( nextBTree == NO_PAGE ) && ( i < nbBTrees - 1 ) )
            {
                throw new RuntimeException( "The pointer to the next BTree is incorrect" );
            }

            position = nextBTree;
        }
    }


    /**
     * Check the whole file
     */
    private void check()
    {
        try
        {
            // First check the header
            ByteBuffer header = ByteBuffer.allocate( HEADER_SIZE );
            long fileSize = fileChannel.size();

            if ( fileSize < HEADER_SIZE )
            {
                throw new RuntimeException( "File size too small : " + fileSize );
            }

            // Read the header
            fileChannel.read( header, 0L );
            header.flip();

            // The page size. It must be a power of 2, and above 16.
            int pageSize = header.getInt();

            if ( ( pageSize < 0 ) || ( pageSize < 32 ) || ( ( pageSize & ( ~pageSize + 1 ) ) != pageSize ) )
            {
                throw new RuntimeException( "Wrong page size : " + pageSize );
            }

            // Compute the number of pages in this file
            long nbPages = ( fileSize - HEADER_SIZE ) / pageSize;

            // The number of trees. It must be at least 2 and > 0
            int nbBTrees = header.getInt();

            if ( nbBTrees < 0 )
            {
                throw new RuntimeException( "Wrong nb trees : " + nbBTrees );
            }

            // The first free page offset. It must be either -1 or below file size
            // and its value must be a modulo of pageSize
            long firstFreePage = header.getLong();

            if ( firstFreePage > fileSize )
            {
                throw new RuntimeException( "First free page pointing after the end of the file : " + firstFreePage );
            }

            if ( ( firstFreePage != NO_PAGE ) && ( ( ( firstFreePage - HEADER_SIZE ) % pageSize ) != 0 ) )
            {
                throw new RuntimeException( "First free page not pointing to a correct offset : " + firstFreePage );
            }

            // The last free page offset. It must be -1
            long lastFreePage = header.getLong();

            if ( ( ( lastFreePage != NO_PAGE ) && ( ( ( lastFreePage - HEADER_SIZE ) % pageSize ) != 0 ) ) )
            //|| ( lastFreePage != 0 ) )
            {
                throw new RuntimeException( "Invalid last free page : " + lastFreePage );
            }

            int nbPageBits = ( int ) ( nbPages / 64 );

            // Create an array of pages to be checked
            // We use one bit per page. It's 0 when the page
            // hasn't been checked, 1 otherwise.
            long[] checkedPages = new long[nbPageBits + 1];

            // Then the free files
            checkFreePages( checkedPages, pageSize, firstFreePage, lastFreePage );

            // The BTrees
            checkBTrees( checkedPages, pageSize, nbBTrees );
        }
        catch ( Exception e )
        {
            // We catch the exception and rethrow it immediately to be able to
            // put a breakpoint here
            e.printStackTrace();
            throw new RuntimeException( "Error : " + e.getMessage() );
        }
    }


    /**
     * Loads a BTree holding the values of a duplicate key
     * This tree is also called as dups tree or sub tree
     * 
     * @param offset the offset of the BTree header
     * @return the deserialized BTree
     */
    /* No qualifier */<K, V> BTree<K, V> loadDupsBTree( long offset )
    {
        try
        {
            PageIO[] pageIos = readPageIOs( offset, Long.MAX_VALUE );

            BTree<K, V> subBtree = BTreeFactory.createBTree();
            ((PersistedBTree<K, V>)subBtree).setBtreeOffset( offset );

            loadBTree( pageIos, subBtree );

            return subBtree;
        }
        catch ( Exception e )
        {
            // should not happen
            throw new RuntimeException( e );
        }
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "RM free pages : [" );

        if ( firstFreePage != NO_PAGE )
        {
            long current = firstFreePage;
            boolean isFirst = true;

            while ( current != NO_PAGE )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }

                PageIO pageIo;

                try
                {
                    pageIo = fetchPage( current );
                    sb.append( pageIo.getOffset() );
                    current = pageIo.getNextPage();
                }
                catch ( EndOfFileExceededException e )
                {
                    e.printStackTrace();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }

            }
        }

        sb.append( "]" );

        return sb.toString();
    }
}
