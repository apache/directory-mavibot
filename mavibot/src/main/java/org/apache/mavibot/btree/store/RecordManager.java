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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mavibot.btree.BTree;
import org.apache.mavibot.btree.BTreeFactory;
import org.apache.mavibot.btree.Page;
import org.apache.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.mavibot.btree.serializer.IntSerializer;
import org.apache.mavibot.btree.serializer.LongArraySerializer;
import org.apache.mavinot.btree.utils.Strings;
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
    private Map<String, BTree<?, ?>> managedBTrees;

    /** The default file name */
    private static final String DEFAULT_FILE_NAME = "mavibot.db";


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
        this.pageSize = pageSize;

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
        // The page size
        ByteBuffer header = ByteBuffer.allocate( HEADER_SIZE );

        // The page size
        header.putInt( pageSize );

        // The number of managed BTree (currently we have only one : the discardedPage BTree
        header.putInt( 1 );

        // The first free page
        header.putLong( NO_PAGE );
        firstFreePage = NO_PAGE;

        // The last free page
        header.putLong( NO_PAGE );
        lastFreePage = NO_PAGE;

        // Write the header on disk
        header.rewind();
        fileChannel.write( header );

        // Set the offset of the end of the file
        endOfFileOffset = fileChannel.size();

        // Now, initialize the Discarded Page BTree, which is a in-memory BTree
        copiedPageBTree = new BTree<Integer, long[]>( "copiedPageBTree", new IntSerializer(), new LongArraySerializer() );

        // Inject this BTree into the RecordManager
        try
        {
            managedBTrees = new HashMap<String, BTree<?, ?>>();

            manage( copiedPageBTree );
        }
        catch ( BTreeAlreadyManagedException btame )
        {
            // Can't happen here.
        }

        // We are all set !
    }


    /**
     * We will load the header and all the existing BTrees in this record manager.
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
            PageIO[] pageIos = readPages( 0L );

            // Create the BTree
            copiedPageBTree = BTreeFactory.createBTree();

            long position = loadBTree( pageIos, 0L, copiedPageBTree );

            // Then process the next ones
            for ( int i = 0; i < nbBtree; i++ )
            {
                BTree<?, ?> btree = new BTree();
                position = loadBTree( pageIos, position, btree );

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
    private PageIO[] readPages( long position ) throws IOException, EndOfFileExceededException
    {
        PageIO firstPage = fetchPage( position );
        long nextPage = 0L;
        List<PageIO> listPages = new ArrayList<PageIO>();
        listPages.add( firstPage );

        // Iterate on the pages
        while ( ( nextPage = firstPage.getNextPage() ) != NO_PAGE )
        {
            PageIO page = fetchPage( nextPage );
            listPages.add( page );
        }

        // Return 
        return listPages.toArray( new PageIO[]
            {} );
    }


    /**
     * Read a BTree from the disk. The meta-data are at the given position in the list of pages.
     * 
     * @param pageIos The list of pages containing the meta-data
     * @param position The position in the pageIos
     * @param btree The BTree we have to initialize
     * @return The new position in the list of given pages
     * @throws InstantiationException 
     * @throws IllegalAccessException 
     * @throws ClassNotFoundException 
     */
    private long loadBTree( PageIO[] pageIos, long position, BTree<?, ?> btree ) throws EndOfFileExceededException,
        IOException, ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        // The tree name
        byte[] btreeNameBytes = readBytes( pageIos, position );

        position += INT_SIZE;

        if ( btreeNameBytes != null )
        {
            position += btreeNameBytes.length;
            String btreeName = Strings.utf8ToString( btreeNameBytes );
            BTreeFactory.setName( btree, btreeName );
        }
        else
        {
            BTreeFactory.setName( btree, "" );
        }

        // The keySerializer FQCN
        byte[] keySerializerBytes = readBytes( pageIos, position );

        String keySerializerFqcn = null;
        position += INT_SIZE;

        if ( keySerializerBytes != null )
        {
            position += keySerializerBytes.length;
            keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );
        }
        else
        {
            keySerializerFqcn = "";
        }

        BTreeFactory.setKeySerializer( btree, keySerializerFqcn );

        // The valueSerialier FQCN
        byte[] valueSerializerBytes = readBytes( pageIos, position );

        String valueSerializerFqcn = null;
        position += INT_SIZE;

        if ( valueSerializerBytes != null )
        {
            position += valueSerializerBytes.length;
            valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );
        }
        else
        {
            valueSerializerFqcn = "";
        }

        BTreeFactory.setValueSerializer( btree, valueSerializerFqcn );

        // The BTree page size
        int btreePageSize = readInt( pageIos, position );
        BTreeFactory.setPageSize( btree, btreePageSize );
        position += INT_SIZE;

        // The BTree current revision
        long revision = readLong( pageIos, position );
        BTreeFactory.setRevision( btree, revision );
        position += LONG_SIZE;

        // The nb elems in the tree
        int nbElems = readInt( pageIos, position );
        BTreeFactory.setNbElems( btree, nbElems );
        position += LONG_SIZE;

        // The BTree rootPage offset
        long rootPageOffset = readLong( pageIos, position );

        PageIO[] rootPage = readPages( rootPageOffset );
        position += LONG_SIZE;

        // Now, load the rootPage, which can be a Leaf or a Node, depending 
        // on the number of elements in the tree : if it's above the pageSize,
        // it's a Node, otherwise it's a Leaf
        Page btreeRoot = null;

        if ( nbElems > btreePageSize )
        {
            // It's a Node
            btreeRoot = BTreeFactory.createNode( btree, revision, nbElems );
        }
        else
        {
            // it's a leaf
            btreeRoot = BTreeFactory.createLeaf( btree, revision, nbElems );
        }

        BTreeFactory.setRoot( btree, btreeRoot );

        return position;
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
        String keySerializerFqcn = btree.getKeySerializer().getClass().getName();
        byte[] keySerializerBytes = Strings.getBytesUtf8( keySerializerFqcn );
        String valueSerializerFqcn = btree.getKeySerializer().getClass().getName();
        byte[] valueSerializerBytes = Strings.getBytesUtf8( valueSerializerFqcn );

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
                LONG_SIZE; // The root offset

        // Get the pageIOs we need to store the data. We may need more than one.
        PageIO[] pageIos = getFreePageIOs( bufferSize );

        // Get a free page to store the RootPage
        PageIO rootPageIo = fetchNewPage();

        // Update the number of elements to 0, as it's a new page
        // We have to do that as the page might contain garbage
        store( 0L, 0L, rootPageIo );
        rootPageIo.setSize( LONG_SIZE );

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

        // The tree name
        position = store( position, btreeNameBytes, pageIos );

        // The keySerializer FQCN
        position = store( position, keySerializerBytes, pageIos );

        // The valueSerialier FQCN
        position = store( position, valueSerializerBytes, pageIos );

        // The BTree page size
        position = store( position, btree.getPageSize(), pageIos );

        // The BTree current revision
        position = store( position, btree.getRevision(), pageIos );

        // The nb elems in the tree
        position = store( position, btree.getNbElems(), pageIos );

        // The BTree rootPage offset
        position = store( position, rootPageIo.getOffset(), pageIos );

        // And flush the pages to disk now
        flushPages( pageIos );
        flushPages( rootPageIo );
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
                fileChannel.write( pageIo.getData(), pageIo.getOffset() );
            }
            else
            {
                // This is a page we have to add to the file
                fileChannel.write( pageIo.getData(), fileChannel.size() );
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

        // Compute the number of pages needed.
        // Considering that each page coan contain PageSize bytes,
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
        if ( fileChannel.size() <= offset + pageSize )
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
}
