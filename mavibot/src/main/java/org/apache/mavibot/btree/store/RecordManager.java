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
package org.apache.mavibot.btree.store;


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

import org.apache.mavibot.btree.AbstractPage;
import org.apache.mavibot.btree.BTree;
import org.apache.mavibot.btree.BTreeFactory;
import org.apache.mavibot.btree.ElementHolder;
import org.apache.mavibot.btree.Leaf;
import org.apache.mavibot.btree.MemoryHolder;
import org.apache.mavibot.btree.Node;
import org.apache.mavibot.btree.Page;
import org.apache.mavibot.btree.ReferenceHolder;
import org.apache.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.mavibot.btree.serializer.IntSerializer;
import org.apache.mavibot.btree.serializer.LongArraySerializer;
import org.apache.mavibot.btree.serializer.LongSerializer;
import org.apache.mavibot.btree.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The RecordManager is used to manage the file in which we will store the BTrees. 
 * A RecordManager will manage more than one BTree.<br/>
 * 
 * It stores data in fixed size pages (default size is 4Kb), which may be linked one to 
 * the other if the data we want to store is too bug for a page.
 * if 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class RecordManager
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( RecordManager.class );

    /** The file name */
    private String fileName;

    /** The associated file */
    private File file;

    /** The channel used to read and write data */
    private FileChannel fileChannel;

    /** The number of stored BTrees */
    private int nbBtree;

    /** The first and last free page */
    private long firstFreePage;
    private long lastFreePage;

    /** The offset of the end of the file */
    private long endOfFileOffset;

    /** 
     * A Btree used to manage the page that has been copied in a new version.
     * Those page can be reclaimed when the associated version is dead. 
     **/
    private BTree<Integer, long[]> copiedPageBTree;

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
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;

    /** The size of the link to the first and last free page */
    private static final int FIRST_FREE_PAGE_SIZE = 8;
    private static final int LAST_FREE_PAGE_SIZE = 8;

    private static final int HEADER_SIZE = NB_TREE_SIZE + PAGE_SIZE + FIRST_FREE_PAGE_SIZE + LAST_FREE_PAGE_SIZE;

    /** The default page size */
    private static final int DEFAULT_PAGE_SIZE = 4 * 1024;

    /** The RecordManager underlying page size. */
    private int pageSize = -1;

    /** A buffer used to read a page */
    private ByteBuffer blockBuffer;

    /** The set of managed BTrees */
    private Map<String, BTree<?, ?>> managedBTrees = new LinkedHashMap<String, BTree<?, ?>>();

    /** The offset on the last added BTree */
    private long lastAddedBTreeOffset = NO_PAGE;

    /** The default file name */
    private static final String DEFAULT_FILE_NAME = "mavibot.db";

    /** A deserializer for Offsets */
    private static final LongSerializer OFFSET_SERIALIZER = new LongSerializer();


    /**
     * Create a Record manager which will either create the underlying file
     * or load an existing one. If a folder is provider, then we will create
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
        this.fileName = fileName;

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
            e.printStackTrace();
            LOG.error( "Error while initializing the RecordManager : {}", e.getMessage() );
        }
    }


    /**
     * We will create a brand new RecordManager file, containing nothing, but the header and
     * a BTree used to manage pages associated with old versions.
     * <br/>
     * The Header contains the following informations :
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

        // Now, initialize the Discarded Page BTree, which is a in-memory BTree
        copiedPageBTree = new BTree<Integer, long[]>( "copiedPageBTree", new IntSerializer(), new LongArraySerializer() );

        // Inject this BTree into the RecordManager
        try
        {
            manage( copiedPageBTree );
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

            PageIO[] pageIos = readPages( HEADER_SIZE, Long.MAX_VALUE );
            long position = pageIos.length * pageSize + HEADER_SIZE;

            // Create the BTree
            copiedPageBTree = BTreeFactory.createBTree();
            copiedPageBTree.setBtreeOffset( btreeOffset );

            loadBTree( pageIos, copiedPageBTree );
            long nextBtreeOffset = copiedPageBTree.getNextBTreeOffset();

            // Then process the next ones
            for ( int i = 1; i < nbBtree; i++ )
            {
                // Create the BTree
                BTree<?, ?> btree = BTreeFactory.createBTree();
                btree.setBtreeOffset( nextBtreeOffset );

                // Read the associated pages
                pageIos = readPages( nextBtreeOffset, Long.MAX_VALUE );

                // Load the BTree
                loadBTree( pageIos, btree );
                nextBtreeOffset = btree.getNextBTreeOffset();

                // Store it into the managedBtrees map
                managedBTrees.put( btree.getName(), btree );
            }

            // We are done ! Let's finish with the last initilization parts
            endOfFileOffset = fileChannel.size();
        }
    }


    /**
     * Reads all the pages that are linked to the page at the given position, including
     * the first page.
     * 
     * @param position The position of the first page
     * @return An array of pages
     */
    private PageIO[] readPages( long position, long limit ) throws IOException, EndOfFileExceededException
    {
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
        if ( dataRead < limit )
        {
            long nextPage = firstPage.getNextPage();

            while ( ( nextPage != NO_PAGE ) && ( dataRead < limit ) )
            {
                PageIO page = fetchPage( nextPage );
                listPages.add( page );
                nextPage = page.getNextPage();
                dataRead += pageSize - LONG_SIZE;
            }
        }

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
    private void loadBTree( PageIO[] pageIos, BTree<?, ?> btree ) throws EndOfFileExceededException,
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
        byte[] btreeNameBytes = readBytes( pageIos, dataPos );
        dataPos += INT_SIZE;

        if ( btreeNameBytes != null )
        {
            dataPos += btreeNameBytes.length;
            String btreeName = Strings.utf8ToString( btreeNameBytes );
            BTreeFactory.setName( btree, btreeName );
        }
        else
        {
            BTreeFactory.setName( btree, "" );
        }

        // The keySerializer FQCN
        byte[] keySerializerBytes = readBytes( pageIos, dataPos );

        String keySerializerFqcn = null;
        dataPos += INT_SIZE;

        if ( keySerializerBytes != null )
        {
            dataPos += keySerializerBytes.length;
            keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );
        }
        else
        {
            keySerializerFqcn = "";
        }

        BTreeFactory.setKeySerializer( btree, keySerializerFqcn );

        // The valueSerialier FQCN
        byte[] valueSerializerBytes = readBytes( pageIos, dataPos );

        String valueSerializerFqcn = null;
        dataPos += INT_SIZE;

        if ( valueSerializerBytes != null )
        {
            dataPos += valueSerializerBytes.length;
            valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );
        }
        else
        {
            valueSerializerFqcn = "";
        }

        BTreeFactory.setValueSerializer( btree, valueSerializerFqcn );

        // Now, init the BTree
        btree.init();

        // Now, load the rootPage, which can be a Leaf or a Node, depending 
        // on the number of elements in the tree : if it's above the pageSize,
        // it's a Node, otherwise it's a Leaf

        // Read the rootPage pages on disk
        PageIO[] rootPageIos = readPages( rootPageOffset, Long.MAX_VALUE );

        Page btreeRoot = readPage( btree, rootPageIos );
        BTreeFactory.setRecordManager( btree, this );

        BTreeFactory.setRoot( btree, btreeRoot );
    }


    private Page readNode( BTree btree, long offset, long revision, int nbElems ) throws IOException
    {
        Page node = BTreeFactory.createNode( btree, revision, nbElems );

        // Read the rootPage pages on disk
        PageIO[] pageIos = readPages( offset, Long.MAX_VALUE );

        return node;
    }


    public Page deserialize( BTree btree, long offset ) throws EndOfFileExceededException, IOException
    {
        PageIO[] rootPageIos = readPages( offset, Long.MAX_VALUE );

        Page page = readPage( btree, rootPageIos );

        return page;
    }


    private Page readPage( BTree btree, PageIO[] pageIos ) throws IOException
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
        Page page = null;
        ByteBuffer byteBuffer = null;

        // Reads the bytes containing all the keys and values, if we have some
        byte[] data = readBytes( pageIos, position );

        if ( data != null )
        {
            byteBuffer = ByteBuffer.allocate( data.length );
            byteBuffer.put( data );
            byteBuffer.rewind();
        }

        if ( nbElems >= 0 )
        {
            // Its a leaf
            page = BTreeFactory.createLeaf( btree, revision, nbElems );

            // Read each value and key
            for ( int i = 0; i < nbElems; i++ )
            {
                Object value = btree.getValueSerializer().deserialize( byteBuffer );

                ElementHolder valueHolder = new MemoryHolder( btree, value );
                BTreeFactory.setValue( ( ( Leaf ) page ), i, valueHolder );

                Object key = btree.getKeySerializer().deserialize( byteBuffer );

                BTreeFactory.setKey( page, i, key );
            }
        }
        else
        {
            // It's a node
            int nodeNbElems = -nbElems;

            page = BTreeFactory.createNode( btree, revision, nodeNbElems );

            // Read each value and key
            for ( int i = 0; i < nodeNbElems; i++ )
            {
                // This is an Offset
                long offset = OFFSET_SERIALIZER.deserialize( byteBuffer );

                ElementHolder valueHolder = new ReferenceHolder( btree, null, offset );
                ( ( Node ) page ).setValue( i, valueHolder );

                Object key = btree.getKeySerializer().deserialize( byteBuffer );
                BTreeFactory.setKey( page, i, key );
            }

            // and read the last value, as it's a node
            long offset = OFFSET_SERIALIZER.deserialize( byteBuffer );

            ElementHolder valueHolder = new ReferenceHolder( btree, null, offset );
            ( ( Node ) page ).setValue( nodeNbElems, valueHolder );
        }

        return page;
    }


    /**
     * Read a byte[] from pages.
     * @param pageIos The pages we want to read the byte[] from
     * @param position The position in the data stored in those pages
     * @return The byte[] we have read
     */
    private byte[] readBytes( PageIO[] pageIos, long position )
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
            byte[] bytes = new byte[length];
            int bytesPos = 0;

            while ( length > 0 )
            {
                if ( length <= remaining )
                {
                    pageData.mark();
                    pageData.position( pagePos );
                    pageData.get( bytes, bytesPos, length );
                    pageData.reset();

                    return bytes;
                }

                pageData.mark();
                pageData.position( pagePos );
                pageData.get( bytes, bytesPos, remaining );
                pageData.reset();
                pageNb++;
                pagePos = LINK_SIZE;
                bytesPos += remaining;
                pageData = pageIos[pageNb].getData();
                length -= remaining;
                remaining = pageData.capacity() - pagePos;
            }

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
    public synchronized void manage( BTree<?, ?> btree ) throws BTreeAlreadyManagedException, IOException
    {
        BTreeFactory.setRecordManager( btree, this );

        String name = btree.getName();

        if ( managedBTrees.containsKey( name ) )
        {
            // There is already a BTree with this name in the recordManager...
            LOG.error( "There is already a BTree named '{}' managed by this recordManager", name );
            throw new BTreeAlreadyManagedException( name );
        }

        managedBTrees.put( name, btree );

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
                LONG_SIZE; // The root offset

        // Get the pageIOs we need to store the data. We may need more than one.
        PageIO[] pageIos = getFreePageIOs( bufferSize );

        // Store the BTree Offset into the BTree
        long btreeOffset = pageIos[0].getOffset();
        btree.setBtreeOffset( btreeOffset );

        // Now store the BTree data in the pages :
        // - the BTree name
        // - the keySerializer FQCN
        // - the valueSerializer FQCN
        // - the BTree page size
        // - the BTree revision
        // - the BTree number of elements
        // - The RootPage offset
        // Starts at 0
        long position = 0L;

        // The BTree current revision
        position = store( position, btree.getRevision(), pageIos );

        // The nb elems in the tree
        position = store( position, btree.getNbElems(), pageIos );

        // Serialize the BTree root page
        Page rootPage = BTreeFactory.getRoot( btree );

        PageIO[] rootPageIos = serializePage( btree, btree.getRevision(), rootPage );

        // Get the reference on the first page
        PageIO rootPageIo = rootPageIos[0];

        // Now, we can inject the BTree rootPage offset into the BTree header
        position = store( position, rootPageIo.getOffset(), pageIos );
        btree.setRootPageOffset( rootPageIo.getOffset() );

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

        // And flush the pages to disk now
        flushPages( pageIos );
        flushPages( rootPageIos );

        nbBtree++;

        // Now, if this added BTree is not the first BTree, we have to link it with the 
        // latest added BTree
        if ( lastAddedBTreeOffset != NO_PAGE )
        {
            // We have to update the nextBtreeOffset from the previous BTreeHeader
            pageIos = readPages( lastAddedBTreeOffset, LONG_SIZE + LONG_SIZE + LONG_SIZE + LONG_SIZE );
            store( LONG_SIZE + LONG_SIZE + LONG_SIZE, btreeOffset, pageIos );

            // Write the pages on disk
            flushPages( pageIos );
        }

        lastAddedBTreeOffset = btreeOffset;

        // Last, not last, update the number of managed BTrees in the header
        updateRecordManagerHeader();
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
    private PageIO[] serializePage( BTree btree, long revision, Page page ) throws IOException
    {
        int nbElems = page.getNbElems();

        if ( nbElems == 0 )
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
            position = store( position, nbElems, newPage );

            // Update the page size now
            newPage.setSize( ( int ) position );

            // Insert the result into the array of PageIO
            pageIos[0] = newPage;

            return pageIos;
        }
        else
        {
            // Prepare a list of byte[] that will contain the serialized page
            int nbBuffers = 1 + 1 + 1 + nbElems * 2;
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

            // Iterate on the keys
            for ( int pos = 0; pos < nbElems; pos++ )
            {
                // Start with the value
                if ( page instanceof Node )
                {
                    Page child = ( ( Node ) page ).getReference( pos );
                    buffer = LongSerializer.serialize( ( ( AbstractPage ) child ).getOffset() );
                    serializedData.add( buffer );
                    dataSize += buffer.length;
                }
                else
                {
                    ElementHolder value = ( ( Leaf ) page ).getValue( pos );
                    buffer = btree.getValueSerializer().serialize( value.getValue( btree ) );
                    serializedData.add( buffer );
                    dataSize += buffer.length;
                }

                // and the key
                buffer = btree.getKeySerializer().serialize( page.getKey( pos ) );
                serializedData.add( buffer );
                dataSize += buffer.length;
            }

            // Nodes have one more value to serialize
            if ( page instanceof Node )
            {
                Page child = ( ( Node ) page ).getReference( nbElems );
                buffer = LongSerializer.serialize( ( ( AbstractPage ) child ).getOffset() );
                serializedData.add( buffer );
                dataSize += buffer.length;
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
     * Update the header, injecting the nbBtree, firstFreePage and lastFreePage
     */
    private void updateRecordManagerHeader() throws IOException
    {
        // The page size
        ByteBuffer header = ByteBuffer.allocate( HEADER_SIZE );

        // The page size
        header.putInt( pageSize );

        // The number of managed BTree (currently we have only one : the discardedPage BTree
        header.putInt( nbBtree );

        // The first free page
        header.putLong( firstFreePage );

        // The last free page
        header.putLong( lastFreePage );

        // Write the header on disk
        header.rewind();
        fileChannel.write( header, 0L );
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
    public void updateBtreeHeader( BTree btree, long rootPageOffset ) throws EndOfFileExceededException, IOException
    {
        // Read the pageIOs associated with this BTree
        long offset = btree.getBtreeOffset();
        long headerSize = LONG_SIZE + LONG_SIZE + LONG_SIZE;

        PageIO[] pageIos = readPages( offset, headerSize );

        // Now, update the revision
        long position = 0;

        position = store( position, btree.getRevision(), pageIos );
        position = store( position, btree.getNbElems(), pageIos );
        position = store( position, rootPageOffset, pageIos );

        // Flush only the needed pages : we have stored the revision, the number of elements 
        // and the rootPage offset, three longs.
        int nbPages = computeNbPages( ( int ) headerSize );

        for ( int i = 0; i < nbPages; i++ )
        {
            flushPages( pageIos[i] );
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
        for ( PageIO pageIo : pageIos )
        {
            pageIo.getData().rewind();

            if ( fileChannel.size() <= ( pageIo.getOffset() + pageSize ) )
            {
                // This is a page we have to add to the file
                fileChannel.write( pageIo.getData(), fileChannel.size() );
            }
            else
            {
                fileChannel.write( pageIo.getData(), pageIo.getOffset() );
            }

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
    public ElementHolder writePage( BTree btree, Page oldPage, Page newPage, long newRevision )
        throws IOException
    {
        // We first need to save the new page on disk
        PageIO[] pageIos = serializePage( btree, newRevision, newPage );

        // Write the page on disk
        flushPages( pageIos );

        // Build the resulting reference
        ElementHolder valueHolder = new ReferenceHolder( btree, newPage, pageIos[0].getOffset() );

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
     * Get as many pages as needed to store the data which size is provided
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
     * Return a new Page. We take one of the existing free page, or we create
     * a new page at the end of the file.
     * 
     * @return The fetched PageIO
     */
    private PageIO fetchNewPage() throws IOException
    {
        if ( firstFreePage == NO_PAGE )
        {
            // We don't have any free page. Reclaim some new page at the end
            // of the file
            PageIO newPage = new PageIO( endOfFileOffset );

            endOfFileOffset += pageSize;

            ByteBuffer data = ByteBuffer.allocateDirect( pageSize );

            newPage.setData( data );
            newPage.setNextPage( NO_PAGE );
            newPage.setSize( 0 );

            return newPage;
        }
        else
        {
            // We have some existing free page. Fetch one from there.
            PageIO pageIo = fetchPage( firstFreePage );

            // Point to the next free page
            firstFreePage = pageIo.getNextPage();

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
     * Read the header. It will contain all the BTree headers. The header is stored in
     * the first block, which may be linked to some other blocks. The first block contains 
     * the header's size on 4 bytes, then the data, and if we have a linked block, the last
     * 8 bytes contain the reference on the next page.
     * <br/>
     * <pre>
     * +----+--------+-----...---+    +--------+---------...----------+
     * |Size|NextPage|    data   | -->|NextPage|        data          | --> ...
     * +----+--------+-----...---+    +--------+---------...----------+
     *   ^      ^
     *   |      |
     *   |      +--- Offset of the next page, or -1 if no linked page
     *   |
     *   +----------------------- Size of the header
     * </pre>
     * @return
     */
    private ByteBuffer readHeader() throws IOException
    {
        ByteBuffer dataBuffer = null;

        // Read the first block
        fileChannel.read( blockBuffer );

        // Now, get its size, and check if we have more pages to read
        int dataSize = blockBuffer.getInt();

        dataBuffer = ByteBuffer.allocate( dataSize );

        if ( dataSize + DATA_SIZE + LINK_SIZE > pageSize )
        {
            // We have more than one page to read
            long nextPage = blockBuffer.getLong();

            dataBuffer.put( blockBuffer );

            dataSize -= pageSize - DATA_SIZE - LINK_SIZE;

            // Loop on pages
            while ( dataSize > 0 )
            {
                blockBuffer.clear();
                fileChannel.read( blockBuffer, nextPage );

                nextPage = blockBuffer.getLong();
                dataSize -= pageSize - LINK_SIZE;

                if ( nextPage == -1L )
                {
                    dataBuffer.put( blockBuffer.array(), LINK_SIZE, dataSize );
                }
                else
                {
                    dataBuffer.put( blockBuffer );
                }
            }
        }
        else
        {
            fileChannel.read( dataBuffer );
        }

        return dataBuffer;
    }


    /**
     * Close the RecordManager and flush everything on disk
     */
    public void close() throws IOException
    {
        // TODO : we must wait for the last write to finish

        // Write the data
        fileChannel.force( true );

        // And close the channel
        fileChannel.close();
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

        System.out.println( "RecordManager" );
        System.out.println( "-------------" );
        System.out.println( "  Header " );
        System.out.println( "    '" + Strings.dumpBytes( header.array() ) + "'" );
        System.out.println( "    page size : " + pageSize );
        System.out.println( "    nbTree : " + nbBTree );
        System.out.println( "    firstFreePage : " + firstFreePage );
        System.out.println( "    lastFreePage : " + lastFreePage );

        long position = HEADER_SIZE;

        // Dump the BTrees
        for ( int i = 0; i < nbBTree; i++ )
        {
            System.out.println( "  Btree[" + i + "]" );
            PageIO[] pageIos = readPages( position, Long.MAX_VALUE );

            for ( PageIO pageIo : pageIos )
            {
                System.out.println( "    " + pageIo );
            }
        }
    }


    /**
     * Get the number of managed trees. We don't count the CopiedPage BTree.
     * 
     * @return The number of managed BTrees
     */
    public int getNbManagedTrees()
    {
        return nbBtree - 1;
    }


    /**
     * Get the managed trees. We don't return the CopiedPage BTree.
     * 
     * @return The managed BTrees
     */
    public Set<String> getManagedTrees()
    {
        Set<String> btrees = new HashSet<String>();

        for ( String btree : managedBTrees.keySet() )
        {
            btrees.add( btree );
        }

        return btrees;
    }


    /**
     * Get one managed trees, knowing its name. 
     * 
     * @return The managed BTrees
     */
    public BTree getManagedTree( String name )
    {
        return managedBTrees.get( name );
    }
}
