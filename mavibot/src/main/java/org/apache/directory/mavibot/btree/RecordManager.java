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


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.CursorException;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.FileException;
import org.apache.directory.mavibot.btree.exception.InvalidOffsetException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.exception.RecordManagerException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongArraySerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;


/**
 * The RecordManager is used to manage the file in which we will store the b-trees.
 * A RecordManager will manage more than one B-tree.<br/>
 *
 * It stores data in fixed size pages (default size is 512 bytes), which may be linked one to
 * the other if the data we want to store is too big for a page.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RecordManager implements TransactionManager
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( RecordManager.class );

    /** The LoggerFactory used to trace TXN operations */
    protected static final Logger TXN_LOG = LoggerFactory.getLogger( "TXN_LOG" );

    /** The LoggerFactory used for Pages operations logging */
    protected static final Logger LOG_PAGES = LoggerFactory.getLogger( "org.apache.directory.mavibot.LOG_PAGES" );

    /** A dedicated logger for the check */
    protected static final Logger LOG_CHECK = LoggerFactory.getLogger( "org.apache.directory.mavibot.LOG_CHECK" );

    /** The associated file */
    private File file;

    /** The channel used to read and write data */
    /* no qualifier */FileChannel fileChannel;
    
    /** The RecordManager header */
    private AtomicReference<RecordManagerHeader> recordManagerHeaderReference = new AtomicReference<>();

    /** Some counters to track the number of free pages */
    private AtomicLong nbFreedPages = new AtomicLong( 0 );
    private AtomicLong nbCreatedPages = new AtomicLong( 0 );
    private AtomicLong nbReusedPages = new AtomicLong( 0 );
    private AtomicLong nbUpdateRMHeader = new AtomicLong( 0 );
    private AtomicLong nbUpdateBtreeHeader = new AtomicLong( 0 );
    private AtomicLong nbUpdatePageIOs = new AtomicLong( 0 );
    public AtomicLong nbCacheHits = new AtomicLong( 0 );
    public AtomicLong nbCacheMisses = new AtomicLong( 0 );

    /**
     * A B-tree used to manage the page that has been copied in a new version.
     * Those pages can be reclaimed when the associated version is dead.
     **/
    /* no qualifier */BTree<RevisionName, long[]> copiedPageBtree;

    /** The RecordManager header size */
    /* no qualifier */static int recordManagerHeaderSize = BTreeConstants.DEFAULT_PAGE_SIZE;

    /** A global buffer used to store the RecordManager header */
    private ByteBuffer recordManagerHeaderBuffer;

    /** A static buffer used to store the RecordManager header */
    private byte[] recordManagerHeaderBytes;

    /** The length of an Offset, as a negative value */
    //private byte[] LONG_LENGTH = new byte[]
    //    { ( byte ) 0xFF, ( byte ) 0xFF, ( byte ) 0xFF, ( byte ) 0xF8 };

    /** The set of managed B-trees */
    private Map<String, BTree<Object, Object>> managedBtrees;

    /** The queue of recently closed transactions */
    private Queue<RevisionName> closedTransactionsQueue = new LinkedBlockingQueue<>();

    /** A flag set to true if we want to keep old revisions */
    private boolean keepRevisions;

    /** The B-tree of B-trees */
    /* no qualifier */BTree<NameRevision, Long> btreeOfBtrees;

    /** A lock to protect the transaction handling */
    private ReentrantLock transactionLock = new ReentrantLock();

    /** A ThreadLocalStorage used to store the current transaction */
    private static final ThreadLocal<Integer> CONTEXT = new ThreadLocal<>();

    /** The list of PageIO that can be freed after a commit */
    List<PageIO> freedPages = new ArrayList<>();

    /** The list of PageIO that can be freed after a roolback */
    private List<PageIO> allocatedPages = new ArrayList<>();

    /** A Map keeping the latest revisions for each managed BTree */
    private Map<String, BTreeHeader<?, ?>> currentBTreeHeaders = new HashMap<>();

    /** A Map storing the new revisions when some change have been made in some BTrees */
    private Map<String, BTreeHeader<?, ?>> newBTreeHeaders = new HashMap<>();

    /** A lock to protect the BtreeHeader maps */
    private ReadWriteLock btreeHeadersLock = new ReentrantReadWriteLock();

    /** A lock to protect the freepage pointers */
    private ReentrantLock freePageLock = new ReentrantLock();

    /** the space reclaimer */
    private PageReclaimer reclaimer;

    /** variable to keep track of the write commit count */
    private int commitCount = 0;

    /** the threshold at which the PageReclaimer will be run to free the copied pages */
    // FIXME the below value is derived after seeing that anything higher than that
    // is resulting in a "This thread does not hold the transactionLock" error
    private int pageReclaimerThreshold = 70;

    /* a flag used to disable the free page reclaimer (used for internal testing only) */
    private boolean disableReclaimer = false;

    public Map<Long, Integer> writeCounter = new HashMap<>();
    
    /** The transaction context */
    private TransactionContext context;
    
    
    /** The page cache */
    private Cache<Long, Page> pageCache;



    /**
     * Create a Record manager which will either create the underlying file
     * or load an existing one. If a folder is provided, then we will create
     * a file with a default name : mavibot.db
     *
     * @param name The file name, or a folder name
     */
    public RecordManager( String fileName )
    {
        this( fileName, BTreeConstants.DEFAULT_PAGE_SIZE );
    }


    /**
     * Create a Record manager which will either create the underlying file
     * or load an existing one. If a folder is provider, then we will create
     * a file with a default name : mavibot.db
     *
     * @param name The file name, or a folder name
     * @param pageSize the size of a page on disk, in bytes
     */
    public RecordManager( String fileName, int pageSize )
    {
        // Create the RMH
        RecordManagerHeader recordManagerHeader = new RecordManagerHeader();
        
        // Create the map of managed b-trees
        managedBtrees = new LinkedHashMap<>();

        // The page size can't be lower than 512
        if ( pageSize < BTreeConstants.MIN_PAGE_SIZE )
        {
            recordManagerHeader.pageSize = BTreeConstants.MIN_PAGE_SIZE;
        }
        else
        {
            recordManagerHeader.pageSize = pageSize;
        }

        recordManagerHeaderReference.set( recordManagerHeader );

        recordManagerHeaderBuffer = ByteBuffer.allocate( recordManagerHeader.pageSize );
        recordManagerHeaderBytes = new byte[recordManagerHeader.pageSize];
        recordManagerHeaderSize = recordManagerHeader.pageSize;

        // Open the file or create it
        File tmpFile = new File( fileName );

        if ( tmpFile.isDirectory() )
        {
            // It's a directory. Check that we don't have an existing mavibot file
            tmpFile = new File( tmpFile, BTreeConstants.DEFAULT_FILE_NAME );
        }
        
        // Create the cache
        CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
            .withCache( "pageCache", CacheConfigurationBuilder.
                newCacheConfigurationBuilder( Long.class, Page.class, 
                ResourcePoolsBuilder.heap( 10000 ) ) ).build( true );

        pageCache = cacheManager.getCache( "pageCache", Long.class, Page.class ); 

        // We have to create a new file, if it does not already exist
        boolean isNewFile = createFile( tmpFile );

        // Create the file, or load it if it already exists
        try
        {
            RandomAccessFile randomFile = new RandomAccessFile( file, "rw" );
            
            fileChannel = randomFile.getChannel();

            if ( isNewFile )
            {
                initRecordManager();
            }
            else
            {
                loadRecordManager();
            }

            // Create the unused pages reclaimer
            reclaimer = new PageReclaimer( this );
            //runReclaimer();
        }
        catch ( Exception e )
        {
            LOG.error( "Error while initializing the RecordManager : {}", e.getMessage() );
            LOG.error( "", e );
            throw new RecordManagerException( e );
        }
    }


    /**
     * runs the PageReclaimer to free the copied pages
     */
    private void runReclaimer()
    {
        if ( disableReclaimer )
        {
            LOG.warn( "Free page reclaimer is disabled, this should not be disabled on production systems." );
            return;
        }

        try
        {
            commitCount = 0;
            reclaimer.reclaim();
            // must update the headers after reclaim operation
            RecordManagerHeader recordManagerHeader = recordManagerHeaderReference.get();
            writeRecordManagerHeader( recordManagerHeader );
        }
        catch ( Exception e )
        {
            LOG.warn( "PageReclaimer failed to free the pages", e );
        }
    }


    /**
     * Create the mavibot file if it does not exist
     */
    private boolean createFile( File mavibotFile )
    {
        try
        {
            boolean creation = mavibotFile.createNewFile();

            file = mavibotFile;

            if ( mavibotFile.length() == 0 )
            {
                return true;
            }
            else
            {
                return creation;
            }
        }
        catch ( IOException ioe )
        {
            LOG.error( "Cannot create the file {}", mavibotFile.getName() );
            return false;
        }
    }


    /**
     * We will create a brand new RecordManager file, containing nothing, but the RecordManager header,
     * a B-tree to manage the old revisions we want to keep and
     * a B-tree used to manage pages associated with old versions.
     * <br/>
     * The RecordManager header contains the following details :
     * <pre>
     * +--------------------------+
     * | PageSize                 | 4 bytes : The size of a physical page (default to 4096)
     * +--------------------------+
     * |  NbTree                  | 4 bytes : The number of managed B-trees (zero or more)
     * +--------------------------+
     * | FirstFree                | 8 bytes : The offset of the first free page
     * +--------------------------+
     * | current BoB offset       | 8 bytes : The offset of the current BoB
     * +--------------------------+
     * | previous BoB offset      | 8 bytes : The offset of the previous BoB
     * +--------------------------+
     * | current CP btree offset  | 8 bytes : The offset of the current BoB
     * +--------------------------+
     * | previous CP btree offset | 8 bytes : The offset of the previous BoB
     * +--------------------------+
     * </pre>
     *
     * We then store the B-tree managing the pages that have been copied when we have added
     * or deleted an element in the B-tree. They are associated with a version.
     *
     * Last, we add the bTree that keep a track on each revision we can have access to.
     */
    private void initRecordManager() throws IOException
    {
        // Create a new Header
        RecordManagerHeader recordManagerHeader = recordManagerHeaderReference.get();
        recordManagerHeader.nbBtree = 0;
        recordManagerHeader.firstFreePage = BTreeConstants.NO_PAGE;
        recordManagerHeader.currentBtreeOfBtreesOffset = BTreeConstants.NO_PAGE;
        recordManagerHeader.idCounter = 0L;

        writeRecordManagerHeader( recordManagerHeader );
        
        try ( WriteTransaction transaction = beginWriteTransaction() )
        {
            // First, create the btree of btrees <NameRevision, Long>
            btreeOfBtrees = createBtreeOfBtrees( transaction );
    
            // Now, create the Copied Page B-tree
            BTree<RevisionName, long[]> copiedPagesBtree = createCopiedPagesBtree( transaction );
    
            // Inject these B-trees into the RecordManager. They are internal B-trees.
            writeManagementTree( transaction, btreeOfBtrees );
            transaction.recordManagerHeader.btreeOfBtrees = btreeOfBtrees;
            transaction.recordManagerHeader.nbBtree++;
            recordManagerHeader.btreeOfBtrees = btreeOfBtrees;

            // The Copied Pages B-tree
            writeManagementTree( transaction, copiedPagesBtree );
            transaction.recordManagerHeader.copiedPagesBtree = copiedPagesBtree;
            transaction.recordManagerHeader.nbBtree++;
            recordManagerHeader.copiedPagesBtree = copiedPagesBtree;
        }
    }


    /**
     * Create the B-treeOfBtrees
     */
    private BTree<NameRevision, Long> createBtreeOfBtrees( Transaction transaction )
    {
        BTreeConfiguration<NameRevision, Long> configuration = new BTreeConfiguration<>();
        configuration.setKeySerializer( NameRevisionSerializer.INSTANCE );
        configuration.setName( BTreeConstants.BTREE_OF_BTREES_NAME );
        configuration.setValueSerializer( LongSerializer.INSTANCE );
        configuration.setBtreeType( BTreeTypeEnum.BTREE_OF_BTREES );

        return BTreeFactory.createBTree( transaction, configuration );
    }


    /**
     * Create the CopiedPagesBtree
     */
    private BTree<RevisionName, long[]> createCopiedPagesBtree( Transaction transaction )
    {
        BTreeConfiguration<RevisionName, long[]> configuration = new BTreeConfiguration<>();
        configuration.setKeySerializer( RevisionNameSerializer.INSTANCE );
        configuration.setName( BTreeConstants.COPIED_PAGE_BTREE_NAME );
        configuration.setValueSerializer( LongArraySerializer.INSTANCE );
        configuration.setBtreeType( BTreeTypeEnum.COPIED_PAGES_BTREE );

        return BTreeFactory.createBTree( transaction, configuration );
    }


    /**
     * Load the BTrees from the disk.
     *
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalArgumentException
     * @throws CursorException 
     */
    private void loadRecordManager() throws IOException, ClassNotFoundException, IllegalAccessException,
        InstantiationException, IllegalArgumentException, SecurityException, NoSuchFieldException, KeyNotFoundException, CursorException
    {
        RecordManagerHeader recordManagerHeader = recordManagerHeaderReference.get();
        
        if ( fileChannel.size() != 0 )
        {
            ByteBuffer rmhBytes = ByteBuffer.allocate( recordManagerHeaderSize );

            // The file exists, we have to load the data now
            fileChannel.read( rmhBytes );

            rmhBytes.rewind();

            // read the RecordManager Header :
            // +---------------------+
            // | PageSize            | 4 bytes : The size of a physical page (default to 4096)
            // +---------------------+
            // | NbTree              | 4 bytes : The number of managed B-trees (at least 1)
            // +---------------------+
            // | Revision            | 8 bytes : The current revision
            // +---------------------+
            // | FirstFree           | 8 bytes : The offset of the first free page
            // +---------------------+
            // | BoB offset          | 8 bytes : The offset of the current B-tree of B-trees
            // +---------------------+
            // | CPB offset          | 8 bytes : The offset of the current Copied Pages B-tree
            // +---------------------+
            // | ID                  | 8 bytes : The ID counter
            // +---------------------+
            recordManagerHeader.pageSize = rmhBytes.getInt();

            // The number of managed B-trees
            recordManagerHeader.nbBtree = rmhBytes.getInt();

            // The current revision
            recordManagerHeader.revision = rmhBytes.getLong();

            // The first free page
            recordManagerHeader.firstFreePage = rmhBytes.getLong();

            // Read all the free pages
            checkFreePages();

            // The BOB offset
            recordManagerHeader.currentBtreeOfBtreesOffset = rmhBytes.getLong();

            // The current Copied Pages B-tree offset
            recordManagerHeader.currentCopiedPagesBtreeOffset = rmhBytes.getLong();

            // The current Copied Pages B-tree offset
            recordManagerHeader.idCounter = rmhBytes.getLong();
            
            // Set the last offset, which is the file's size
            recordManagerHeader.lastOffset = fileChannel.size();

            // read the B-tree of B-trees
            try ( Transaction transaction = beginReadTransaction() )
            {
                PageIO[] bobHeaderPageIos = readPageIOs( recordManagerHeader.pageSize, recordManagerHeader.currentBtreeOfBtreesOffset, Long.MAX_VALUE );
    
                recordManagerHeader.btreeOfBtrees = new BTree<>();
                
                //recordManagerHeader.btreeOfBtrees = BTreeFactory.<NameRevision, Long> createBTree( BTreeTypeEnum.BTREE_OF_BTREES );
                //( ( BTree<NameRevision, Long> ) recordManagerHeader.btreeOfBtrees ).setRecordManagerHeader( transaction.getRecordManagerHeader() );
    
                loadBtree( transaction, bobHeaderPageIos, recordManagerHeader.btreeOfBtrees );
                recordManagerHeader.btreeOfBtrees.setType( BTreeTypeEnum.BTREE_OF_BTREES );

                // read the copied page B-tree
                PageIO[] copiedPagesPageIos = readPageIOs( recordManagerHeader.pageSize, recordManagerHeader.currentCopiedPagesBtreeOffset, Long.MAX_VALUE );
    
                recordManagerHeader.copiedPagesBtree = BTreeFactory.<RevisionName, long[]> createBTree( BTreeTypeEnum.COPIED_PAGES_BTREE );
                //( ( BTree<RevisionName, long[]> ) recordManagerHeader.copiedPagesBtree ).setRecordManagerHeader( transaction.getRecordManagerHeader() );
    
                loadBtree( transaction, copiedPagesPageIos, recordManagerHeader.copiedPagesBtree );
            }

            Map<String, Long> loadedBtrees = new HashMap<>();

            // Now, read all the B-trees from the btree of btrees
            try ( Transaction transaction = beginReadTransaction() )
            {
            
                TupleCursor<NameRevision, Long> btreeCursor = transaction.getRecordManagerHeader().btreeOfBtrees.browse( transaction );
    
                // loop on all the btrees we have, and keep only the latest revision
                long currentRevision = -1L;
    
                while ( btreeCursor.hasNext() )
                {
                    Tuple<NameRevision, Long> btreeTuple = btreeCursor.next();
                    NameRevision nameRevision = btreeTuple.getKey();
                    long btreeOffset = btreeTuple.getValue();
                    long revision = nameRevision.getValue();
    
                    // Check if we already have processed this B-tree
                    Long loadedBtreeRevision = loadedBtrees.get( nameRevision.getName() );
    
                    if ( loadedBtreeRevision != null )
                    {
                        // The btree has already been loaded. The revision is necessarily higher
                        if ( revision > currentRevision )
                        {
                            // We have a newer revision : switch to the new revision (we keep the offset atm)
                            loadedBtrees.put( nameRevision.getName(), btreeOffset );
                            currentRevision = revision;
                        }
                    }
                    else
                    {
                        // This is a new B-tree
                        loadedBtrees.put( nameRevision.getName(), btreeOffset );
                        currentRevision = nameRevision.getRevision();
                    }
                }

                // TODO : clean up the old revisions...

                // Now, we can load the real btrees using the offsets
                for ( Map.Entry<String, Long> loadedBtree : loadedBtrees.entrySet() )
                {
                    long btreeOffset = loadedBtree.getValue();

                    PageIO[] btreePageIos = readPageIOs( recordManagerHeader.pageSize, btreeOffset, Long.MAX_VALUE );
                
                    BTree<?, ?> btree = new BTree<>();
                    loadBtree( transaction, btreePageIos, btree );

                    // Add the btree into the map of managed B-trees
                    recordManagerHeader.btreeMap.put( loadedBtree.getKey(), ( BTree<Object, Object> ) btree );
                }
            }
        }
    }

    
    private <K, V> BTree<K, V> readBTree( Transaction transaction, long btreeOffset ) throws NoSuchFieldException, InstantiationException, EndOfFileExceededException,
        IOException, IllegalAccessException, ClassNotFoundException
    {
        PageIO[] btreePageIos = readPageIOs( transaction.getRecordManagerHeader().pageSize, btreeOffset, Long.MAX_VALUE );

        BTree<K, V> btree = BTreeFactory.<K, V> createBTree();
        loadBtree( transaction, btreePageIos, btree );
        
        return btree;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transaction beginReadTransaction()
    {
        return beginReadTransaction( Transaction.DEFAULT_TIMEOUT );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Transaction beginReadTransaction( long timeout )
    {
        if ( TXN_LOG.isDebugEnabled() )
        {
            TXN_LOG.debug( "Begining a new Read Transaction on thread {}",
                Thread.currentThread().getName() );
        }

        return new ReadTransaction( this, timeout );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public WriteTransaction beginWriteTransaction()
    {
        if ( TXN_LOG.isDebugEnabled() )
        {
            TXN_LOG.debug( "Begining a new write transaction on thread {}",
                Thread.currentThread().getName() );
        }

        // First, take the lock if it's not already taken
        if ( !transactionLock.isHeldByCurrentThread() )
        {
            TXN_LOG.debug( "--> Lock taken" );
            transactionLock.lock();
        }
        else
        {
            TXN_LOG.debug( "..o The current thread already holds the lock" );
        }

        // Create the transaction
        return new WriteTransaction( this );
    }


    /**
     * Reads all the PageIOs that are linked to the page at the given position, including
     * the first page.
     *
     * @param position The position of the first page
     * @param limit The maximum bytes to read. Set this value to -1 when the size is unknown.
     * @return An array of pages
     */
    /*no qualifier*/PageIO[] readPageIOs( int pageSize, long position, long limit ) 
        throws IOException, EndOfFileExceededException
    {
        LOG.debug( "Read PageIOs at position {}", position );

        if ( limit <= 0 )
        {
            limit = Long.MAX_VALUE;
        }

        PageIO firstPage = fetchPageIO( pageSize, position );
        firstPage.setSize();
        List<PageIO> listPages = new ArrayList<>();
        listPages.add( firstPage );
        long dataRead = pageSize - BTreeConstants.LONG_SIZE - BTreeConstants.INT_SIZE;

        // Iterate on the pages, if needed
        long nextPage = firstPage.getNextPage();

        if ( ( dataRead < limit ) && ( nextPage != BTreeConstants.NO_PAGE ) )
        {
            while ( dataRead < limit )
            {
                PageIO page = fetchPageIO( pageSize, nextPage );
                listPages.add( page );
                nextPage = page.getNextPage();
                dataRead += pageSize - BTreeConstants.LONG_SIZE;

                if ( nextPage == BTreeConstants.NO_PAGE )
                {
                    page.setNextPage( BTreeConstants.NO_PAGE );
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
     * Check the offset to be sure it's a valid one :
     * <ul>
     * <li>It's >= 0</li>
     * <li>It's below the end of the file</li>
     * <li>It's a multiple of the pageSize
     * </ul>
     * @param offset The offset to check
     * @throws InvalidOffsetException If the offset is not valid
     */
    /* no qualifier */void checkOffset( long offset )
    {
        RecordManagerHeader recordManagerHeader = recordManagerHeaderReference.get();
        
        if ( ( offset < 0 ) || ( offset > recordManagerHeader.lastOffset ) || ( ( offset % recordManagerHeader.pageSize ) != 0 ) )
        {
            throw new InvalidOffsetException( "Bad Offset : " + offset );
        }
    }


    /**
     * Read a B-tree from the disk. The meta-data are at the given position in the list of pages.
     * We load a B-tree in two steps : first, we load the B-tree header, then the common informations
     *
     * @param pageIos The list of pages containing the meta-data
     * @param btree The B-tree we have to initialize
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalArgumentException
     */
    private <K, V> void loadBtree( Transaction transaction, PageIO[] pageIos, BTree<K, V> btree ) throws EndOfFileExceededException,
        IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, IllegalArgumentException,
        SecurityException, NoSuchFieldException
    {
        loadBtree( transaction, pageIos, btree, null );
    }


    /**
     * Read a B-tree from the disk. The meta-data are at the given position in the list of pages.
     * We load a B-tree in two steps : first, we load the B-tree header, then the common informations
     *
     * @param pageIos The list of pages containing the meta-data
     * @param btree The b-tree we have to initialize
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws NoSuchFieldException
     */
    /* no qualifier */<K, V> void loadBtree( Transaction transaction, PageIO[] pageIos, BTree<K, V> btree, BTree<K, V> parentBTree )
        throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchFieldException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        int pageSize = recordManagerHeader.pageSize;
        BTreeInfo<K, V> btreeInfo = new BTreeInfo<>();
        long dataPos = 0L;

        // Process the B-tree header
        BTreeHeader<K, V> btreeHeader = new BTreeHeader<>( btreeInfo );

        // The BtreeHeader offset
        btreeHeader.setOffset( pageIos[0].getOffset() );

        // The B-tree header page ID
        long id = readLong( pageSize, pageIos, dataPos );
        btreeHeader.setId( id );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree current revision
        long revision = readLong( pageSize, pageIos, dataPos );
        btreeHeader.setRevision( revision );
        dataPos += BTreeConstants.LONG_SIZE;

        // The nb elems in the tree
        long nbElems = readLong( pageSize, pageIos, dataPos );
        btreeHeader.setNbElems( nbElems );
        dataPos += BTreeConstants.LONG_SIZE;

        // The b-tree rootPage offset
        long rootPageOffset = readLong( pageSize, pageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree information offset
        long btreeInfoOffset = readLong( pageSize, pageIos, dataPos );
        btree.setBtreeInfo( btreeInfo );

        // Now, process the common informations
        PageIO[] infoPageIos = readPageIOs( recordManagerHeader.pageSize, btreeInfoOffset, Long.MAX_VALUE );
        dataPos = 0L;

        // The B-tree page numbers of elements
        int btreePageNbElem = readInt( pageSize, infoPageIos, dataPos );
        BTreeFactory.setPageNbElem( btree, btreePageNbElem );
        dataPos += BTreeConstants.INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = readBytes( pageSize, infoPageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE + btreeNameBytes.limit();
        String btreeName = Strings.utf8ToString( btreeNameBytes );
        BTreeFactory.setName( btree, btreeName );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = readBytes( pageSize, infoPageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE + keySerializerBytes.limit();

        String keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );

        BTreeFactory.setKeySerializer( btree, keySerializerFqcn );

        // The valueSerialier FQCN
        ByteBuffer valueSerializerBytes = readBytes( pageSize, infoPageIos, dataPos );

        String valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );

        BTreeFactory.setValueSerializer( btree, valueSerializerFqcn );

        // Update the BtreeHeader reference
        btree.setBtreeHeader( btreeHeader );

        // Read the rootPage pages on disk
        PageIO[] rootPageIos = readPageIOs( recordManagerHeader.pageSize, rootPageOffset, Long.MAX_VALUE );
        BTreeFactory.setRecordManager( btree, this );

        Page<K, V> btreeRoot = readPage( recordManagerHeader.pageSize, btreeInfo, rootPageIos );

        BTreeFactory.setRootPage( btree, btreeRoot );
    }


    /**
     * Read a page from some PageIO for a given B-tree
     * @param btree The B-tree we want to read a page for
     * @param pageIos The PageIO containing the raw data
     * @return The read Page if successful
     * @throws IOException If the deserialization failed
     */
    /* No qualifier*/ <K, V> Page<K, V> readPage( int pageSize, BTreeInfo<K, V> btreeInfo, PageIO[] pageIos ) throws IOException
    {
        long position = 0L;

        // The id
        long id = readLong( pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The revision
        long revision = readLong( pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The number of elements in the page
        int nbElems = readInt( pageSize, pageIos, position );
        position += BTreeConstants.INT_SIZE;

        // The size of the data containing the keys and values
        Page<K, V> page = null;

        // Reads the bytes containing all the keys and values, if we have some
        // We read  big blog of data into  ByteBuffer, then we will process
        // this ByteBuffer
        ByteBuffer byteBuffer = readBytes( pageSize, pageIos, position );

        // Now, deserialize the data block. If the number of elements
        // is positive, it's a Leaf, otherwise it's a Node
        // Note that only a leaf can have 0 elements, and it's the root page then.
        if ( nbElems >= 0 )
        {
            // It's a leaf
            page = readLeafKeysAndValues( btreeInfo, nbElems, revision, byteBuffer, pageIos );
        }
        else
        {
            // It's a node
            page = readNodeKeysAndValues( btreeInfo, -nbElems, revision, byteBuffer, pageIos );
        }

        ( ( AbstractPage<K, V> ) page ).setOffset( pageIos[0].getOffset() );
        ( ( AbstractWALObject<K, V> ) page ).setId( id );
        
        return page;
    }


/**
 * Deserialize a Leaf from some PageIOs
 */
    private <K, V> Leaf<K, V> readLeafKeysAndValues( BTreeInfo<K, V> btreeInfo, int nbElems, long revision,
        ByteBuffer byteBuffer, PageIO[] pageIos ) throws IOException
    {
        // Its a leaf, create it
        Leaf<K, V> leaf = new Leaf<>( btreeInfo, revision, nbElems );
    
        // Store the page offset on disk
        leaf.setOffset( pageIos[0].getOffset() );
    
        return leaf.deserialize( byteBuffer );
    }


    /**
     * Deserialize a Node from some PageIos
     */
    private <K, V> Node<K, V> readNodeKeysAndValues( BTreeInfo<K, V> btreeInfo, int nbElems, long revision,
        ByteBuffer byteBuffer, PageIO[] pageIos ) throws IOException
    {
        Node<K, V> node = new Node<>( btreeInfo, revision, nbElems );
        
        // Store the page offset on disk
        node.setOffset( pageIos[0].getOffset() );
        
        return node.deserialize( byteBuffer );
    }


    /**
     * Read a byte[] from pages.
     *
     * @param pageIos The pages we want to read the byte[] from
     * @param position The position in the data stored in those pages
     * @return The byte[] we have read
     */
    /* no qualifier */static ByteBuffer readBytes( int pageSize, PageIO[] pageIos, long position )
    {
        // Read the byte[] length first
        int length = readInt( pageSize, pageIos, position );
        position += BTreeConstants.INT_SIZE;

        // Compute the page in which we will store the data given the
        // current position
        int pageNb = computePageNb( pageSize, position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE ) - pageNb * pageSize;

        // Check that the length is correct : it should fit in the provided pageIos
        int pageEnd = computePageNb( pageSize, position + length );

        if ( pageEnd > pageIos.length )
        {
            // This is wrong...
            LOG.error( "Wrong size : {}, it's larger than the number of provided pages {}", length, pageIos.length );
            throw new ArrayIndexOutOfBoundsException();
        }

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
                pagePos = BTreeConstants.LINK_SIZE;
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
    /* no qualifier */static int readInt( int pageSize, PageIO[] pageIos, long position )
    {
        // Compute the page in which we will store the data given the
        // current position
        int pageNb = computePageNb( pageSize, position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE ) - pageNb * pageSize;

        ByteBuffer pageData = pageIos[pageNb].getData();
        int remaining = pageData.capacity() - pagePos;
        int value;

        if ( remaining >= BTreeConstants.INT_SIZE )
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
            pagePos = BTreeConstants.LINK_SIZE;

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
    private byte readByte( int pageSize, PageIO[] pageIos, long position )
    {
        // Compute the page in which we will store the data given the
        // current position
        int pageNb = computePageNb( pageSize, position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE ) - pageNb * pageSize;

        ByteBuffer pageData = pageIos[pageNb].getData();

        return pageData.get( pagePos );
    }


    /**
     * Read a long from pages
     * @param pageIos The pages we want to read the long from
     * @param position The position in the data stored in those pages
     * @return The long we have read
     */
    /* no qualifier */static long readLong( int pageSize, PageIO[] pageIos, long position )
    {
        // Compute the page in which we will store the data given the
        // current position
        int pageNb = computePageNb( pageSize, position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE ) - pageNb * pageSize;

        ByteBuffer pageData = pageIos[pageNb].getData();
        int remaining = pageData.capacity() - pagePos;
        long value = 0L;

        if ( remaining >= BTreeConstants.LONG_SIZE )
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
            pagePos = BTreeConstants.LINK_SIZE;

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
     * Manage a B-tree. The btree will be added and managed by this RecordManager. We will create a
     * new RootPage for this added B-tree, which will contain no data.<br/>
     * This method is threadsafe.
     * Managing a btree is a matter of storing an reference to the managed B-tree in the B-tree Of B-trees.
     * We store a tuple of NameRevision (where revision is 0L) and a offset to the B-tree header.
     * At the same time, we keep a track of the managed B-trees in a Map.
     *
     * @param btree The new B-tree to manage.
     * @param treeType flag indicating if this is an internal tree
     *
     * @throws BTreeAlreadyManagedException If the B-tree is already managed
     * @throws IOException if there was a problem while accessing the file
     */
    public synchronized <K, V> void manage( WriteTransaction transaction, BTree<K, V> btree ) throws BTreeAlreadyManagedException, IOException
    {
        long revision = transaction.getRevision();
        RecordManagerHeader recordManagerHeader = transaction.recordManagerHeader;

        try
        {
            LOG.debug( "Managing the btree {}", btree.getName() );
            BTreeFactory.setRecordManager( btree, this );

            String name = btree.getName();

            if ( recordManagerHeader.containsBTree( name ) )
            {
                // There is already a B-tree with this name in the recordManager...
                LOG.error( "There is already a B-tree named '{}' managed by this recordManager", name );
                transaction.abort();
                throw new BTreeAlreadyManagedException( name );
            }

            // Create the B-tree info
            BTreeInfo<K, V> btreeInfo = btree.getBtreeInfo();
            btreeInfo.initId( recordManagerHeader );
            transaction.addWALObject( btreeInfo );
            btree.setBtreeInfo( btreeInfo );

            // Create the first root page, with the context revision. It will be empty
            Leaf<K, V> rootPage = transaction.newLeaf( btreeInfo, 0 );
            transaction.addWALObject( rootPage );

            // Create a B-tree header, and initialize it
            BTreeHeader<K, V> btreeHeader = new BTreeHeader<>( btreeInfo );
            btreeHeader.setRootPage( rootPage );
            btreeHeader.setRevision( revision );
            btree.setBtreeHeader( btreeHeader );
            btreeHeader.initId( recordManagerHeader );

            transaction.addWALObject( btreeHeader );

            // We can safely increment the number of managed B-trees
            recordManagerHeader.nbBtree++;
            
            // Finally add the new B-tree in the map of managed B-trees.
            recordManagerHeader.addBTree( btree );
            
        }
        catch ( IOException ioe )
        {
            transaction.abort();
            throw ioe;
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
    public <K, V> InsertResult<K, V> insert( WriteTransaction transaction, String btreeName, K key, V value ) throws KeyNotFoundException, IOException
    {
        // Fetch the B-tree from the BOB 
        long revision = transaction.getRevision();
        
        BTree<K, V> btree = getBtree( transaction, btreeName, revision );
        
        return btree.insert( transaction, key, value );
    }

    
    /**
     * Inject a newly created BTree in the BtreeOfBtrees
     */
    /* No qualifier */ <K, V> void insertInBtreeOfBtrees( WriteTransaction transaction, BTree<K, V> btree ) throws IOException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        BTree<NameRevision, Long> btreeOfBtrees = recordManagerHeader.btreeOfBtrees;
        
        // Create the new NameRevision
        NameRevision nameRevision = new NameRevision( btree.getName(), recordManagerHeader.getRevision() );

        // Inject it into the B-tree of B-tree
        btreeOfBtrees.insert( transaction, nameRevision, btree.getBtreeHeader().getOffset() );
    }
    
    
    /**
     * Inject the copied pages in the copiedPagesBtree. We may have many pages from various B-trees
     * to inject. As the pages are mixed, we first have to gather them by B-tree.
     */
    /* No qualifier */ void insertInCopiedPagesBtree( WriteTransaction transaction ) throws IOException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        Map<String, List<Long>> pagesList = new HashMap<>();

        // Order the WALObject by B-tree name
        for ( WALObject<?, ?> walObject : transaction.getCopiedPageMap().values() )
        {
            if ( walObject.getBtreeInfo().getType() != BTreeTypeEnum.COPIED_PAGES_BTREE )
            {
                String name = walObject.getName();
                
                List<Long> offsets = pagesList.get( name );
                
                if ( offsets == null )
                {
                    offsets = new ArrayList<>();
                }
                
                offsets.add( walObject.getOffset() );
                pagesList.put( name, offsets );
            }
        }

        // Inject it into the copied pages B-tree
        BTree<RevisionName, long[]> copiedPagesBTree = recordManagerHeader.copiedPagesBtree;

        for ( Map.Entry<String, List<Long>> entry : pagesList.entrySet() )
        {
            // Create the new RevisionName
            RevisionName revisionName = new RevisionName( recordManagerHeader.getRevision(), entry.getKey() );
            List<Long> offsetList = entry.getValue();

            long[] offsetArray = new long[offsetList.size()];
            
            for ( int i = 0; i < offsetList.size(); i++ )
            {
                offsetArray[i] = offsetList.get( i );
            }
            
            copiedPagesBTree.insert( transaction, revisionName, offsetArray );
        }
    }


    /**
     * Write the management BTrees (BOB and CPB). There are special BTrees, we can write them on disk immediately 
     * (this is done only once anyway : when we create the RecordManager).
     *
     * @param btree The new B-tree to manage.
     * @param treeType flag indicating if this is an internal tree
     *
     * @throws BTreeAlreadyManagedException If the B-tree is already managed
     * @throws IOException
     */
    private synchronized <K, V> void writeManagementTree( WriteTransaction transaction, BTree<K, V> btree )
    {
        LOG.debug( "Managing the sub-btree {}", btree.getName() );
        BTreeInfo<K, V> btreeInfo = btree.getBtreeInfo();
        
        // Create the first root page, with version 0L. It will be empty
        // and increment the revision at the same time
        Leaf<K, V> rootPage = transaction.newLeaf( btreeInfo, 0 );

        LOG.debug( "Flushing the newly managed '{}' btree rootpage", btree.getName() );
        
        // Store the B-tree root Page in the WAL
        transaction.addWALObject( rootPage );

        // Now, create the b-tree info
        btreeInfo.initId( transaction.getRecordManagerHeader() );

        // Store the B-tree info in the WAL
        transaction.addWALObject( btreeInfo );

        // Last, not least, Create a B-tree header, and initialize it
        BTreeHeader<K, V> btreeHeader = new BTreeHeader<>( btreeInfo );
        btreeHeader.setRootPage( rootPage );
        btreeHeader.setRevision( transaction.getRevision() );
        btreeHeader.initId( transaction.getRecordManagerHeader() );

        // Store the BtreeHeader in the BTree
        btree.setBtreeHeader( btreeHeader );
        
        // Store the B-tree header in the WAL
        transaction.addWALObject( btreeHeader );
    }
    
    
    /**
     * Write the RecordmanagerHeader in disk
     */
    /* No Qualifier */ void writeRecordManagerHeader( RecordManagerHeader recordManagerHeader )
    {
        recordManagerHeader.serialize( this );

        try
        {
            fileChannel.write( recordManagerHeaderBuffer, 0 );
            fileChannel.force( true );
            recordManagerHeader.lastOffset = fileChannel.size();
        }
        catch ( IOException ioe )
        {
            throw new FileException( ioe.getMessage(), ioe );
        }

        // Clean the buffer and switch the versions
        recordManagerHeaderBuffer.clear();
        recordManagerHeaderReference.set( recordManagerHeader );

        nbUpdateRMHeader.incrementAndGet();
    }


    /**
     * Update the RecordManager header, injecting the following data :
     *
     * <pre>
     * +---------------------+
     * | PageSize            | 4 bytes : The size of a physical page (default to 4096)
     * +---------------------+
     * | NbTree              | 4 bytes : The number of managed B-trees (at least 1)
     * +---------------------+
     * | Revision            | 8 bytes : The current revision
     * +---------------------+
     * | FirstFree           | 8 bytes : The offset of the first free page
     * +---------------------+
     * | current BoB offset  | 8 bytes : The offset of the current B-tree of B-trees
     * +---------------------+
     * | previous BoB offset | 8 bytes : The offset of the previous B-tree of B-trees
     * +---------------------+
     * | current CP offset   | 8 bytes : The offset of the current CopiedPages B-tree
     * +---------------------+
     * | previous CP offset  | 8 bytes : The offset of the previous CopiedPages B-tree
     * +---------------------+
     * </pre>
     */
    /* No qualifier */ void updateRecordManagerHeader( RecordManagerHeader recordManagerHeader, long newBtreeOfBtreesOffset, long newCopiedPageBtreeOffset )
    {
        if ( newBtreeOfBtreesOffset != -1L )
        {
            recordManagerHeader.currentBtreeOfBtreesOffset = newBtreeOfBtreesOffset;
        }

        if ( newCopiedPageBtreeOffset != -1L )
        {
            recordManagerHeader.currentCopiedPagesBtreeOffset = newCopiedPageBtreeOffset;
        }
    }


    /**
     * Inject an int into a byte[] at a given position.
     */
    /* No qualifier*/ int writeData( byte[] buffer, int position, int value )
    {
        buffer[position] = ( byte ) ( value >>> 24 );
        buffer[position + 1] = ( byte ) ( value >>> 16 );
        buffer[position + 2] = ( byte ) ( value >>> 8 );
        buffer[position + 3] = ( byte ) ( value );

        return position + 4;
    }


    /**
     * Inject a long into a byte[] at a given position.
     */
    /* No qualifier*/ int writeData( byte[] buffer, int position, long value )
    {
        buffer[position] = ( byte ) ( value >>> 56 );
        buffer[position + 1] = ( byte ) ( value >>> 48 );
        buffer[position + 2] = ( byte ) ( value >>> 40 );
        buffer[position + 3] = ( byte ) ( value >>> 32 );
        buffer[position + 4] = ( byte ) ( value >>> 24 );
        buffer[position + 5] = ( byte ) ( value >>> 16 );
        buffer[position + 6] = ( byte ) ( value >>> 8 );
        buffer[position + 7] = ( byte ) ( value );

        return position + 8;
    }


    /**
     * Add a new <btree, revision> tuple into the B-tree of B-trees.
     *
     * @param name The B-tree name
     * @param revision The B-tree revision
     * @param btreeHeaderOffset The B-tree offset
     * @throws IOException If the update failed
     */
    /* no qualifier */void addInBtreeOfBtrees( WriteTransaction transaction, String name, long revision, 
        long btreeHeaderOffset ) throws IOException
    {
        
        checkOffset( btreeHeaderOffset );
        NameRevision nameRevision = new NameRevision( name, revision );

        btreeOfBtrees.insert( transaction, nameRevision, btreeHeaderOffset );

        // Update the B-tree of B-trees offset
        //transaction.recordManagerHeader.currentBtreeOfBtreesOffset = getNewBTreeHeader( BTreeConstants.BTREE_OF_BTREES_NAME ).getOffset();
    }


    /**
     * Add a new <btree, revision> tuple into the CopiedPages B-tree.
     *
     * @param name The B-tree name
     * @param revision The B-tree revision
     * @param btreeHeaderOffset The B-tree offset
     * @throws IOException If the update failed
     */
    /* no qualifier */<K, V> void addInCopiedPagesBtree( WriteTransaction transaction, String name, long revision, List<Page<K, V>> pages )
        throws IOException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        RevisionName revisionName = new RevisionName( revision, name );

        long[] pageOffsets = new long[pages.size()];
        int pos = 0;

        for ( Page<K, V> page : pages )
        {
            pageOffsets[pos++] = ( ( AbstractPage<K, V> ) page ).getOffset();
        }

        copiedPageBtree.insert( transaction, revisionName, pageOffsets );

        // Update the CopiedPageBtree offset
        recordManagerHeader.currentCopiedPagesBtreeOffset = 
            ( ( BTree<RevisionName, long[]> ) copiedPageBtree ).getBtreeHeader().getOffset();
    }


    /**
     * Write the B-tree header on disk. We will write the following informations :
     * <pre>
     * +------------+
     * | revision   | The B-tree revision
     * +------------+
     * | nbElems    | The B-tree number of elements
     * +------------+
     * | rootPage   | The root page offset
     * +------------+
     * | BtreeInfo  | The B-tree info offset
     * +------------+
     * </pre>
     * @param btree The B-tree which header has to be written
     * @param btreeInfoOffset The offset of the B-tree informations
     * @param now a flag set to <tt>true</tt> if the header has to be written on disk
     * @return The B-tree header offset
     * @throws IOException If we weren't able to write the B-tree header
     */
    /* no qualifier *<K, V> PageIO[] writeBtreeHeader( WriteTransaction transaction, BTree<K, V> btree, BTreeHeader<K, V> btreeHeader, boolean now )
        throws IOException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        int bufferSize =
            BTreeConstants.LONG_SIZE + // The revision
                BTreeConstants.LONG_SIZE + // the number of element
                BTreeConstants.LONG_SIZE + // The root page offset
                BTreeConstants.LONG_SIZE; // The B-tree info page offset

        // Get the pageIOs we need to store the data. We may need more than one.
        PageIO[] pageIOs = getFreePageIOs( recordManagerHeader, bufferSize );

        // Store the B-tree header Offset into the B-tree
        long btreeHeaderOffset = btreeHeaderPageIos[0].getOffset();

        // Now store the B-tree data in the pages :
        // - the B-tree revision
        // - the B-tree number of elements
        // - the B-tree root page offset
        // - the B-tree info page offset
        // Starts at 0
        long position = 0L;

        // The B-tree current revision
        position = store( recordManagerHeader, position, btreeHeader.getRevision(), btreeHeaderPageIos );

        // The nb elems in the tree
        position = store( recordManagerHeader, position, btreeHeader.getNbElems(), btreeHeaderPageIos );

        // Now, we can inject the B-tree rootPage offset into the B-tree header
        position = store( recordManagerHeader, position, btreeHeader.getRootPageOffset(), btreeHeaderPageIos );

        // The B-tree info page offset
        position = store( recordManagerHeader, position, btree.getBtreeInfoOffset(), btreeHeaderPageIos );

        // And flush the pages to disk now
        LOG.debug( "Flushing the newly managed '{}' btree header", btree.getName() );

        if ( LOG_PAGES.isDebugEnabled() )
        {
            LOG_PAGES.debug( "Writing BTreeHeader revision {} for {}", btreeHeader.getRevision(), btree.getName() );
            StringBuilder sb = new StringBuilder();

            sb.append( "Offset : " ).append( Long.toHexString( btreeHeaderOffset ) ).append( "\n" );
            sb.append( "    Revision : " ).append( btreeHeader.getRevision() ).append( "\n" );
            sb.append( "    NbElems  : " ).append( btreeHeader.getNbElems() ).append( "\n" );
            sb.append( "    RootPage : 0x" ).append( Long.toHexString( btreeHeader.getRootPageOffset() ) )
                .append( "\n" );
            sb.append( "    Info     : 0x" )
                .append( Long.toHexString( btree.getBtreeInfoOffset() ) ).append( "\n" );

            LOG_PAGES.debug( "Btree Header[{}]\n{}", btreeHeader.getRevision(), sb.toString() );
        }

        btreeHeader.setPageIOs( btreeHeaderPageIos );
        btreeHeader.setOffset( btreeHeaderOffset );

        return btreeHeaderPageIos;
    }


    /**
     * Update the B-tree header after a B-tree modification. This will make the latest modification
     * visible.<br/>
     * We update the following fields :
     * <ul>
     * <li>the revision</li>
     * <li>the number of elements</li>
     * <li>the B-tree root page offset</li>
     * </ul>
     * <br/>
     * As a result, a new version of the BtreHeader will be created, which will replace the previous
     * B-tree header
     * @param btree TheB-tree to update
     * @param btreeHeaderOffset The offset of the modified btree header
     * @return The offset of the new B-tree Header
     * @throws IOException If we weren't able to write the file on disk
     * @throws EndOfFileExceededException If we tried to write after the end of the file
     */
    /* no qualifier */<K, V> long updateBtreeHeader( BTree<K, V> btree, long btreeHeaderOffset )
        throws EndOfFileExceededException, IOException
    {
        return updateBtreeHeader( btree, btreeHeaderOffset );
    }


    /**
     * Update the B-tree header after a B-tree modification. This will make the latest modification
     * visible.<br/>
     * We update the following fields :
     * <ul>
     * <li>the revision</li>
     * <li>the number of elements</li>
     * <li>the reference to the current B-tree revisions</li>
     * <li>the reference to the old B-tree revisions</li>
     * </ul>
     * <br/>
     * As a result, we new version of the BtreHeader will be created
     * @param btree The B-tree to update
     * @param btreeHeaderOffset The offset of the modified btree header
     * @return The offset of the new B-tree Header if it has changed (ie, when the onPlace flag is set to true)
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    /* no qualifier */<K, V> void updateBtreeHeaderOnPlace( WriteTransaction transaction, BTree<K, V> btree, long btreeHeaderOffset )
        throws EndOfFileExceededException,
        IOException
    {
        updateBtreeHeader( transaction, btree, btreeHeaderOffset, true );
    }


    /**
     * Update the B-tree header after a B-tree modification. This will make the latest modification
     * visible.<br/>
     * We update the following fields :
     * <ul>
     * <li>the revision</li>
     * <li>the number of elements</li>
     * <li>the reference to the current B-tree revisions</li>
     * <li>the reference to the old B-tree revisions</li>
     * </ul>
     * <br/>
     * As a result, a new version of the BtreHeader will be created, which may replace the previous
     * B-tree header (if the onPlace flag is set to true) or a new set of pageIos will contain the new
     * version.
     *
     * @param btree The B-tree to update
     * @param rootPageOffset The offset of the modified rootPage
     * @param onPlace Tells if we modify the B-tree on place, or if we create a copy
     * @return The offset of the new B-tree Header if it has changed (ie, when the onPlace flag is set to true)
     * @throws EndOfFileExceededException If we tried to write after the end of the file
     * @throws IOException If tehre were some error while writing the data on disk
     */
    private <K, V> long updateBtreeHeader( WriteTransaction transaction, BTree<K, V> btree, long btreeHeaderOffset, boolean onPlace )
        throws EndOfFileExceededException, IOException
    {
        // Read the pageIOs associated with this B-tree
        PageIO[] pageIos;
        long newBtreeHeaderOffset = BTreeConstants.NO_PAGE;
        long offset = btree.getBtreeOffset();
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();

        if ( onPlace )
        {
            // We just have to update the existing BTreeHeader
            long headerSize = BTreeConstants.LONG_SIZE + BTreeConstants.LONG_SIZE + BTreeConstants.LONG_SIZE;

            pageIos = readPageIOs( recordManagerHeader.pageSize, offset, headerSize );

            // Now, update the revision
            long position = 0;

            position = store( recordManagerHeader, position, transaction.getRevision(), pageIos );
            position = store( recordManagerHeader, position, btree.getNbElems(), pageIos );
            store( recordManagerHeader, position, btreeHeaderOffset, pageIos );

            // Write the pages on disk
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( "-----> Flushing the '{}' B-treeHeader", btree.getName() );
                LOG.debug( "  revision : " + transaction.getRevision() + ", NbElems : " 
                    + btree.getNbElems()
                    + ", btreeHeader offset : 0x"
                    + Long.toHexString( btreeHeaderOffset ) );
            }

            // Get new place on disk to store the modified BTreeHeader if it's not onPlace
            // Rewrite the pages at the same place
            LOG.debug( "Rewriting the B-treeHeader on place for B-tree " + btree.getName() );
            flushPages( recordManagerHeader, pageIos );
        }
        else
        {
            // We have to read and copy the existing BTreeHeader and to create a new one
            pageIos = readPageIOs( recordManagerHeader.pageSize, offset, Long.MAX_VALUE );

            // Now, copy every read page
            PageIO[] newPageIOs = new PageIO[pageIos.length];
            int pos = 0;

            for ( PageIO pageIo : pageIos )
            {
                // Fetch a free page
                newPageIOs[pos] = fetchNewPage( recordManagerHeader );

                // keep a track of the allocated and copied pages so that we can
                // free them when we do a commit or rollback, if the btree is an management one
                if ( ( btree.getType() == BTreeTypeEnum.BTREE_OF_BTREES )
                    || ( btree.getType() == BTreeTypeEnum.COPIED_PAGES_BTREE ) )
                {
                    freedPages.add( pageIo );
                    allocatedPages.add( newPageIOs[pos] );
                }

                pageIo.copy( newPageIOs[pos] );

                if ( pos > 0 )
                {
                    newPageIOs[pos - 1].setNextPage( newPageIOs[pos].getOffset() );
                }

                pos++;
            }

            // store the new btree header offset
            // and update the revision
            long position = 0;

            position = store( recordManagerHeader, position, transaction.getRevision(), newPageIOs );
            position = store( recordManagerHeader, position, btree.getNbElems(), newPageIOs );
            store( recordManagerHeader, position, btreeHeaderOffset, newPageIOs );

            // Get new place on disk to store the modified BTreeHeader if it's not onPlace
            // Flush the new B-treeHeader on disk
            LOG.debug( "Rewriting the B-treeHeader on place for B-tree " + btree.getName() );
            flushPages( recordManagerHeader, newPageIOs );

            newBtreeHeaderOffset = newPageIOs[0].getOffset();
        }

        nbUpdateBtreeHeader.incrementAndGet();

        if ( LOG_CHECK.isDebugEnabled() )
        {
            MavibotInspector.check( this );
        }

        return newBtreeHeaderOffset;
    }


    /**
     * Write the pages on disk, either at the end of the file, or at
     * the position they were taken from.
     *
     * @param pageIos The list of pages to write
     * @throws IOException If the write failed
     */
    /* No qualifier */ void flushPages( RecordManagerHeader recordManagerHeader, PageIO... pageIos ) throws IOException
    {
        if ( pageIos == null )
        {
            LOG.debug( "No PageIO to flush" );
            return;
        }
        
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
            long pos = pageIo.getOffset();

            if ( fileChannel.size() < ( pageIo.getOffset() + recordManagerHeader.pageSize ) )
            {
                LOG.debug( "Adding a page at the end of the file" );
                // This is a page we have to add to the file
                pos = fileChannel.size();
                fileChannel.write( pageIo.getData(), pageIo.getOffset() );
                long endFile = fileChannel.size();
                
                if ( endFile == pos )
                {
                    System.out.println( "----------------------------------------------------FORCING FLUSH " );
                    fileChannel.force( true );
                }
            }
            else
            {
                LOG.debug( "Writing a page at position {}", pageIo.getOffset() );
                fileChannel.write( pageIo.getData(), pageIo.getOffset() );
                //fileChannel.force( false );
            }

            //System.out.println( "Writing page at " + Long.toHexString( pos ) );
            writeCounter.put( pos, writeCounter.containsKey( pos ) ? writeCounter.get( pos ) + 1 : 1 );

            nbUpdatePageIOs.incrementAndGet();

            pageIo.getData().rewind();
        }
    }

    
    /**
     * Store  the pages in the context. If the page has no Offset, we will
     * use a virtual offset (ie, a negative one)
     *
     * @param pageIos The list of pages to write
     * @throws IOException If the store failed
     */
    /* No qualifier */void storePages( PageIO... pageIos ) throws IOException
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
            long offset = pageIo.getOffset();

            LOG.debug( "Writing a page at position {}", offset );

            writeCounter.put( offset, writeCounter.containsKey( offset ) ? writeCounter.get( offset ) + 1 : 1 );

            nbUpdatePageIOs.incrementAndGet();
        }
    }


    /**
     * Compute the page in which we will store data given an offset, when
     * we have a list of pages.
     *
     * @param offset The position in the data
     * @return The page number in which the offset will start
     */
    private static int computePageNb( int pageSize, long offset )
    {
        long pageNb = 0;

        offset -= pageSize - BTreeConstants.LINK_SIZE - BTreeConstants.PAGE_SIZE;

        if ( offset < 0 )
        {
            return ( int ) pageNb;
        }

        pageNb = 1 + offset / ( pageSize - BTreeConstants.LINK_SIZE );

        return ( int ) pageNb;
/*
        long pageOffset = offset - recordManagerHeader.pageSize - BTreeConstants.LINK_SIZE - BTreeConstants.PAGE_SIZE;

        if ( pageOffset < 0 )
        {
            return 0;
        }

        return ( int ) ( 1 + pageOffset / ( recordManagerHeader.pageSize - BTreeConstants.LINK_SIZE ) );
        */
    }


    /**
     * Stores a byte[] into one ore more pageIO (depending if the long is stored
     * across a boundary or not)
     *
     * @param position The position in a virtual byte[] if all the pages were contiguous
     * @param bytes The byte[] to serialize
     * @param pageIos The pageIOs we have to store the data in
     * @return The new offset
     */
    /* no qualifier */ long store( RecordManagerHeader recordManagerHeader, long position, byte[] bytes, PageIO... pageIos )
    {
        int pageSize = recordManagerHeader.pageSize;
        
        if ( bytes != null )
        {
            // Write the bytes length
            position = store( recordManagerHeader, position, bytes.length, pageIos );

            // Compute the page in which we will store the data given the
            // current position
            int pageNb = computePageNb( pageSize, position );

            // Get back the buffer in this page
            ByteBuffer pageData = pageIos[pageNb].getData();

            // Compute the position in the current page
            int pagePos = ( int ) ( position + ( pageNb + 1 ) * BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE ) - pageNb * pageSize;

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
                    pagePos = BTreeConstants.LINK_SIZE;
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
            position = store( recordManagerHeader, position, 0, pageIos );
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
     * @return The new offset
     */
    /* No qualifier */ long storeRaw( RecordManagerHeader recordManagerHeader, long position, byte[] bytes, PageIO... pageIos )
    {
        int pageSize = recordManagerHeader.pageSize;
        
        if ( bytes != null )
        {
            // Compute the page in which we will store the data given the
            // current position
            int pageNb = computePageNb( pageSize, position );

            // Get back the buffer in this page
            ByteBuffer pageData = pageIos[pageNb].getData();

            // Compute the position in the current page
            int pagePos = ( int ) ( position + ( pageNb + 1 ) * BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE ) - pageNb * pageSize;

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
                    pagePos = BTreeConstants.LINK_SIZE;
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
            position = store( recordManagerHeader, position, 0, pageIos );
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
     * @return The new offset
     */
    /* no qualifier */ long store( RecordManagerHeader recordManagerHeader, long position, int value, PageIO... pageIos )
    {
        int pageSize = recordManagerHeader.pageSize;
        
        // Compute the page in which we will store the data given the
        // current position
        int pageNb = computePageNb( pageSize, position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE ) - pageNb * pageSize;

        // Get back the buffer in this page
        ByteBuffer pageData = pageIos[pageNb].getData();

        // Compute the remaining size in the page
        int remaining = pageData.capacity() - pagePos;

        if ( remaining < BTreeConstants.INT_SIZE )
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
            pagePos = BTreeConstants.LINK_SIZE;

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
        position += BTreeConstants.INT_SIZE;

        return position;
    }


    /**
     * Stores a Long into one ore more pageIO (depending if the long is stored
     * across a boundary or not)
     *
     * @param position The position in a virtual byte[] if all the pages were contiguous
     * @param value The long to serialize
     * @param pageIos The pageIOs we have to store the data in
     * @return The new offset
     */
    /* no qualifier */ long store( RecordManagerHeader recordManagerHeader, long position, long value, PageIO... pageIos )
    {
        int pageSize = recordManagerHeader.pageSize;
        
        // Compute the page in which we will store the data given the
        // current position
        int pageNb = computePageNb( pageSize, position );

        // Compute the position in the current page
        int pagePos = ( int ) ( position + ( pageNb + 1 ) * BTreeConstants.LONG_SIZE + BTreeConstants.INT_SIZE ) - pageNb * pageSize;

        // Get back the buffer in this page
        ByteBuffer pageData = pageIos[pageNb].getData();

        // Compute the remaining size in the page
        int remaining = pageData.capacity() - pagePos;

        if ( remaining < BTreeConstants.LONG_SIZE )
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
            pagePos = BTreeConstants.LINK_SIZE;

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
        position += BTreeConstants.LONG_SIZE;

        return position;
    }


    /**
     * Write the page in a serialized form.
     *
     * @param btree The persistedBtree we will create a new PageHolder for
     * @param newPage The page to write on disk
     * @param newRevision The page's revision
     * @return A PageHolder containing the copied page
     * @throws IOException If the page can't be written on disk
     */
    /* No qualifier*/<K, V> long writePage( WriteTransaction transaction, BTreeInfo<K, V> btreeInfo, Page<K, V> newPage,
        long newRevision ) throws IOException
    {
        // We first need to save the new page on disk
        PageIO[] pageIos = newPage.serialize( transaction );

        if ( LOG_PAGES.isDebugEnabled() )
        {
            LOG_PAGES.debug( "Write data for '{}' btree", btreeInfo.getName() );

            logPageIos( pageIos );
        }

        // Write the page on disk
        flushPages( transaction.getRecordManagerHeader(), pageIos );

        // Build the resulting reference
        long offset = pageIos[0].getOffset();

        return offset;
    }

    
    /* No qualifier */static void logPageIos( PageIO[] pageIos )
    {
        int pageNb = 0;

        for ( PageIO pageIo : pageIos )
        {
            StringBuilder sb = new StringBuilder();
            sb.append( "PageIO[" ).append( pageNb ).append( "]:0x" );
            sb.append( Long.toHexString( pageIo.getOffset() ) ).append( "/" );
            sb.append( pageIo.getSize() );
            pageNb++;

            ByteBuffer data = pageIo.getData();

            int position = data.position();
            int dataLength = ( int ) pageIo.getSize() + 12;

            if ( dataLength > data.limit() )
            {
                dataLength = data.limit();
            }

            byte[] bytes = new byte[dataLength];

            data.get( bytes );
            data.position( position );
            int pos = 0;

            for ( byte b : bytes )
            {
                int mod = pos % 16;

                switch ( mod )
                {
                    case 0:
                        sb.append( "\n    " );
                        // No break
                    case 4:
                    case 8:
                    case 12:
                        sb.append( " " );
                    case 1:
                    case 2:
                    case 3:
                    case 5:
                    case 6:
                    case 7:
                    case 9:
                    case 10:
                    case 11:
                    case 13:
                    case 14:
                    case 15:
                        sb.append( Strings.dumpByte( b ) ).append( " " );
                }
                pos++;
            }

            LOG_PAGES.debug( sb.toString() );
        }
    }


    /**
     * Compute the number of pages needed to store some specific size of data.
     *
     * @param dataSize The size of the data we want to store in pages
     * @return The number of pages needed
     */
    /* No qualifier */ static int computeNbPages( RecordManagerHeader recordManagerHeader, int dataSize )
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
        int availableSize = recordManagerHeader.pageSize - BTreeConstants.LONG_SIZE;
        int nbNeededPages = 1;

        // Compute the number of pages that will be full but the first page
        if ( dataSize > availableSize - BTreeConstants.INT_SIZE )
        {
            int remainingSize = dataSize - ( availableSize - BTreeConstants.INT_SIZE );
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
     * Get as many pages as needed to store the data of the given size. The returned
     * PageIOs are all linked together.
     *
     * @param dataSize The data size
     * @return An array of pages, enough to store the full data
     */
    /* No qualifier */ PageIO[] getFreePageIOs( RecordManagerHeader recordManagerHeader, int dataSize ) throws IOException
    {
        if ( dataSize == 0 )
        {
            return new PageIO[]
                {};
        }

        int nbNeededPages = computeNbPages( recordManagerHeader, dataSize );

        PageIO[] pageIOs = new PageIO[nbNeededPages];

        // The first page : set the size
        pageIOs[0] = fetchNewPage( recordManagerHeader );
        pageIOs[0].setSize( dataSize );

        for ( int i = 1; i < nbNeededPages; i++ )
        {
            pageIOs[i] = fetchNewPage( recordManagerHeader );

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
    /* No qualifier*/ PageIO fetchNewPage( RecordManagerHeader recordManagerHeader ) throws IOException
    {
        //System.out.println( "Fetching new page" );
        if ( recordManagerHeader.firstFreePage == BTreeConstants.NO_PAGE )
        {
            nbCreatedPages.incrementAndGet();

            // We don't have any free page. Reclaim some new page at the end
            // of the file
            long lastOffset = recordManagerHeader.lastOffset;
            PageIO newPage = new PageIO( lastOffset );

            ByteBuffer data = ByteBuffer.allocate( recordManagerHeader.pageSize );

            // Move the last offset one page forward
            recordManagerHeader.lastOffset = lastOffset + recordManagerHeader.pageSize;
            
            newPage.setData( data );
            newPage.setNextPage( BTreeConstants.NO_PAGE );
            newPage.setSize( 0 );

            LOG.debug( "Requiring a new page at offset {}", newPage.getOffset() );

            return newPage;
        }
        else
        {
            nbReusedPages.incrementAndGet();

            freePageLock.lock();

                // We have some existing free page. Fetch it from disk
            PageIO pageIo = fetchPageIO( recordManagerHeader.pageSize, recordManagerHeader.firstFreePage );

            // Update the firstFreePage pointer
            recordManagerHeader.firstFreePage = pageIo.getNextPage();

            freePageLock.unlock();

            // overwrite the data of old page
            ByteBuffer data = ByteBuffer.allocate( recordManagerHeader.pageSize );
            pageIo.setData( data );

            pageIo.setNextPage( BTreeConstants.NO_PAGE );
            pageIo.setSize( 0 );

            LOG.debug( "Reused page at offset {}", pageIo.getOffset() );

            return pageIo;
        }
    }
    
    
    /** 
     * Fetch a page from cache, or from disk
     */
    /* no qualifier */<K, V> Page<K, V> getPage( BTreeInfo btreeInfo, int pageSize, long offset ) throws IOException
    {
        Page<K, V> page = ( Page<K, V> )pageCache.get( offset );
        
        if ( page == null )
        {
            nbCacheMisses.incrementAndGet();
            PageIO[] pageIos = readPageIOs( pageSize, offset, BTreeConstants.NO_LIMIT );
            
            Page<K, V> foundPage = ( Page<K, V> )readPage( pageSize, btreeInfo, pageIos );
            pageCache.put( offset, foundPage );
            
            return foundPage;
        }
        else
        {
            nbCacheHits.incrementAndGet();
            
            return page;
        }
    }
    
    
    /* No qualifier */ <K, V> void putPage( WALObject<K, V> walObject )
    {
        if ( walObject instanceof Page )
        {
            long key = walObject.getOffset();
            
            if ( key >= 0L )
            {
                pageCache.put( key, ( Page<K, V> )walObject );
            }
        }
    }


    /**
     * fetch a page from disk, knowing its position in the file.
     *
     * @param offset The position in the file
     * @return The found page
     */
    /* no qualifier */PageIO fetchPageIO( int pageSize, long offset ) throws IOException
    {
        checkOffset( offset );

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
    public int getPageSize( RecordManagerHeader recordManagerHeader )
    {
        return recordManagerHeader.pageSize;
    }


    /**
     * Set the page size, ie the number of bytes a page can store.
     *
     * @param pageSize The number of bytes for a page
     */
    /* no qualifier */void setPageSize( Transaction transaction, int pageSize )
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        if ( recordManagerHeader.pageSize >= 13 )
        {
            recordManagerHeader.pageSize = pageSize;
        }
        else
        {
            recordManagerHeader.pageSize = BTreeConstants.DEFAULT_PAGE_SIZE;
        }
    }


    /**
     * Close the RecordManager immediately. The pending write transaction will
     * be aborted. 
     */
    public void closeNow() throws IOException
    {
        
    }


    /**
     * Close the RecordManager and flush everything on disk
     */
    public void close() throws IOException
    {
        // Close all the managed B-trees
        for ( BTree<Object, Object> tree : managedBtrees.values() )
        {
            //tree.close();
        }

        // Close the management B-trees
        //copiedPageBtree.close();
        //btreeOfBtrees.close();

        // Write the data
        fileChannel.force( true );

        // And close the channel
        fileChannel.close();
    }

    
    public static String dump( byte octet )
    {
        return new String( new byte[]
            { BTreeConstants.HEX_CHAR[( octet & 0x00F0 ) >> 4], BTreeConstants.HEX_CHAR[octet & 0x000F] } );
    }


    /**
     * Dump a pageIO
     */
    private void dump( PageIO pageIo )
    {
        ByteBuffer buffer = pageIo.getData();
        buffer.mark();
        byte[] longBuffer = new byte[BTreeConstants.LONG_SIZE];
        byte[] intBuffer = new byte[BTreeConstants.INT_SIZE];

        // get the next page offset
        buffer.get( longBuffer );
        long nextOffset = LongSerializer.deserialize( longBuffer );

        // Get the data size
        buffer.get( intBuffer );
        int size = IntSerializer.deserialize( intBuffer );

        buffer.reset();

        System.out.println( "PageIO[" + Long.toHexString( pageIo.getOffset() ) + "], size = " + size + ", NEXT PageIO:"
            + Long.toHexString( nextOffset ) );
        System.out.println( " 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F " );
        System.out.println( "+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+" );

        for ( int i = 0; i < buffer.limit(); i += 16 )
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
    public void dump()
    {
        System.out.println( "/---------------------------- Dump ----------------------------\\" );

        try
        {
            RandomAccessFile randomFile = new RandomAccessFile( file, "r" );
            FileChannel fileChannel = randomFile.getChannel();

            ByteBuffer recordManagerHeaderBuffer = ByteBuffer.allocate( recordManagerHeaderSize );

            // load the RecordManager header
            fileChannel.read( recordManagerHeaderBuffer );

            recordManagerHeaderBuffer.rewind();

            // The page size
            long fileSize = fileChannel.size();
            int pageSize = recordManagerHeaderBuffer.getInt();
            long nbPages = fileSize / pageSize;

            // The number of managed B-trees
            int nbBtree = recordManagerHeaderBuffer.getInt();

            // The first free page
            long firstFreePage = recordManagerHeaderBuffer.getLong();

            // The current B-tree of B-trees
            long currentBtreeOfBtreesPage = recordManagerHeaderBuffer.getLong();

            // The previous B-tree of B-trees
            long previousBtreeOfBtreesPage = recordManagerHeaderBuffer.getLong();

            // The current CopiedPages B-tree
            long currentCopiedPagesBtreePage = recordManagerHeaderBuffer.getLong();

            // The previous CopiedPages B-tree
            long previousCopiedPagesBtreePage = recordManagerHeaderBuffer.getLong();

            System.out.println( "  RecordManager" );
            System.out.println( "  -------------" );
            System.out.println( "  Size = 0x" + Long.toHexString( fileSize ) );
            System.out.println( "  NbPages = " + nbPages );
            System.out.println( "    Header " );
            System.out.println( "      page size : " + pageSize );
            System.out.println( "      nbTree : " + nbBtree );
            System.out.println( "      firstFreePage : 0x" + Long.toHexString( firstFreePage ) );
            System.out.println( "      current BOB : 0x" + Long.toHexString( currentBtreeOfBtreesPage ) );
            System.out.println( "      previous BOB : 0x" + Long.toHexString( previousBtreeOfBtreesPage ) );
            System.out.println( "      current CopiedPages : 0x" + Long.toHexString( currentCopiedPagesBtreePage ) );
            System.out.println( "      previous CopiedPages : 0x" + Long.toHexString( previousCopiedPagesBtreePage ) );

            RecordManagerHeader recordManagerHeader = recordManagerHeaderReference.get();
            
            // Dump the Free pages list
            dumpFreePages( recordManagerHeader, firstFreePage );

            // Dump the B-tree of B-trees
            dumpBtreeHeader( recordManagerHeader, currentBtreeOfBtreesPage );

            // Dump the previous B-tree of B-trees if any
            if ( previousBtreeOfBtreesPage != BTreeConstants.NO_PAGE )
            {
                dumpBtreeHeader( recordManagerHeader, previousBtreeOfBtreesPage );
            }

            // Dump the CopiedPages B-tree
            dumpBtreeHeader( recordManagerHeader, currentCopiedPagesBtreePage );

            // Dump the previous B-tree of B-trees if any
            if ( previousCopiedPagesBtreePage != BTreeConstants.NO_PAGE )
            {
                dumpBtreeHeader( recordManagerHeader, previousCopiedPagesBtreePage );
            }

            // Dump all the user's B-tree
            randomFile.close();
            System.out.println( "\\---------------------------- Dump ----------------------------/" );
        }
        catch ( IOException ioe )
        {
            System.out.println( "Exception while dumping the file : " + ioe.getMessage() );
        }
    }


    /**
     * Dump the free pages
     */
    private void dumpFreePages( RecordManagerHeader recordManagerHeader, long freePageOffset ) throws IOException
    {
        System.out.println( "\n  FreePages : " );
        int pageNb = 1;

        while ( freePageOffset != BTreeConstants.NO_PAGE )
        {
            PageIO pageIo = fetchPageIO( recordManagerHeader.pageSize, freePageOffset );

            System.out.println( "    freePage[" + pageNb + "] : 0x" + Long.toHexString( pageIo.getOffset() ) );

            freePageOffset = pageIo.getNextPage();
            pageNb++;
        }
    }


    /**
     * Dump a B-tree Header
     */
    private long dumpBtreeHeader( RecordManagerHeader recordManagerHeader, long btreeOffset ) throws EndOfFileExceededException, IOException
    {
        int pageSize = recordManagerHeader.pageSize;
        
        // First read the B-tree header
        PageIO[] pageIos = readPageIOs( pageSize, btreeOffset, Long.MAX_VALUE );

        long dataPos = 0L;

        // The B-tree current revision
        long revision = readLong( pageSize, pageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The nb elems in the tree
        long nbElems = readLong( pageSize, pageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree rootPage offset
        long rootPageOffset = readLong( pageSize, pageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree page size
        int btreePageSize = readInt( pageSize, pageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = readBytes( pageSize, pageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE + btreeNameBytes.limit();
        String btreeName = Strings.utf8ToString( btreeNameBytes );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = readBytes( pageSize, pageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE + keySerializerBytes.limit();

        String keySerializerFqcn = "";

        if ( keySerializerBytes != null )
        {
        keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );
        }

        // The valueSerialier FQCN
        ByteBuffer valueSerializerBytes = readBytes( pageSize, pageIos, dataPos );

        String valueSerializerFqcn = "";
        dataPos += BTreeConstants.INT_SIZE + valueSerializerBytes.limit();

        if ( valueSerializerBytes != null )
        {
        valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );
        }

        // The B-tree allowDuplicates flag
        int allowDuplicates = readInt( pageSize, pageIos, dataPos );
        boolean dupsAllowed = allowDuplicates != 0;

        dataPos += BTreeConstants.INT_SIZE;

        //        System.out.println( "\n  B-Tree " + btreeName );
        //        System.out.println( "  ------------------------- " );

        //        System.out.println( "    nbPageIOs[" + pageIos.length + "] = " + pageIoList );
        if ( LOG.isDebugEnabled() )
        {
            StringBuilder sb = new StringBuilder();
            boolean isFirst = true;

            for ( PageIO pageIo : pageIos )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }

                sb.append( "0x" ).append( Long.toHexString( pageIo.getOffset() ) );
            }

            String pageIoList = sb.toString();

            LOG.debug( "    PageIOs[{}] = {}", pageIos.length, pageIoList );

            //        System.out.println( "    dataSize = "+ pageIos[0].getSize() );
            LOG.debug( "    dataSize = {}", pageIos[0].getSize() );

            LOG.debug( "    B-tree '{}'", btreeName );
            LOG.debug( "    revision : {}", revision );
            LOG.debug( "    nbElems : {}", nbElems );
            LOG.debug( "    rootPageOffset : 0x{}", Long.toHexString( rootPageOffset ) );
            LOG.debug( "    B-tree page size : {}", btreePageSize );
            LOG.debug( "    keySerializer : '{}'", keySerializerFqcn );
            LOG.debug( "    valueSerializer : '{}'", valueSerializerFqcn );
            LOG.debug( "    dups allowed : {}", dupsAllowed );
            //
            //        System.out.println( "    B-tree '" + btreeName + "'" );
            //        System.out.println( "    revision : " + revision );
            //        System.out.println( "    nbElems : " + nbElems );
            //        System.out.println( "    rootPageOffset : 0x" + Long.toHexString( rootPageOffset ) );
            //        System.out.println( "    B-tree page size : " + btreePageSize );
            //        System.out.println( "    keySerializer : " + keySerializerFqcn );
            //        System.out.println( "    valueSerializer : " + valueSerializerFqcn );
            //        System.out.println( "    dups allowed : " + dupsAllowed );
        }

        return rootPageOffset;
    }


    /**
     * Get the number of managed trees. We don't count the CopiedPage B-tree and the B-tree of B-trees
     *
     * @return The number of managed B-trees
     */
    public int getNbManagedTrees( RecordManagerHeader recordManagerHeader )
    {
        return recordManagerHeader.nbBtree;
    }


    /**
     * Get the managed B-trees. We don't return the CopiedPage B-tree nor the B-tree of B-trees.
     *
     * @return The managed B-trees
     */
    public Set<String> getManagedTrees()
    {
        return new HashSet<>( getCurrentRecordManagerHeader().btreeMap.keySet() );
    }


    /**
     * Stores the copied pages into the CopiedPages B-tree
     *
     * @param name The B-tree name
     * @param revision The revision
     * @param copiedPages The pages that have been copied while creating this revision
     * @throws IOException If we weren't able to store the data on disk
     */
    /* No Qualifier */void storeCopiedPages( WriteTransaction transaction, String name, long revision, long[] copiedPages ) throws IOException
    {
        RevisionName revisionName = new RevisionName( revision, name );

        copiedPageBtree.insert( transaction, revisionName, copiedPages );
    }


    /**
     * Store a reference to an old rootPage into the Revision B-tree
     *
     * @param btree The B-tree we want to keep an old RootPage for
     * @param rootPage The old rootPage
     * @throws IOException If we have an issue while writing on disk
     */
    /* No qualifier */<K, V> void storeRootPage( WriteTransaction transaction, BTree<K, V> btree, Page<K, V> rootPage ) throws IOException
    {
        if ( !isKeepRevisions() )
        {
            return;
        }

        if ( btree == copiedPageBtree )
        {
            return;
        }

        NameRevision nameRevision = new NameRevision( btree.getName(), rootPage.getRevision() );

        btreeOfBtrees.insert( transaction, nameRevision, ( ( AbstractPage<K, V> ) rootPage ).getOffset() );

        if ( LOG_CHECK.isDebugEnabled() )
        {
            MavibotInspector.check( this );
        }
    }


    /**
     * Get one managed B-tree, knowing its name. It will return the B-tree latest version.
     *
     * @param transaction The {@link Transaction} we are running in
     * @param name The B-tree name we are looking for
     * @return The managed b-tree
     * @throws IOException If we can't find the B-tree in the file
     */
    public <K, V> BTree<K, V> getBtree( Transaction transaction, String name ) throws IOException
    {
        return getBtree( transaction, name, 0L );
    }


    /**
     * Get one managed B-tree, knowing its name and its version. It will return the B-tree requested version
     *
     * @param transaction The {@link Transaction} we are running in
     * @param name The B-tree name we are looking for
     * @param revision The B-tree} revision we are looking for (if <=0, the latest one)
     * @return The managed b-tree
     * @throws IOException If we can't find the B-tree in the file
     */
    public <K, V> BTree<K, V> getBtree( Transaction transaction, String name, long revision ) throws IOException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        // Get the btree from the recordManagerHeader if we have it 
        BTree<K, V> btree = transaction.getBTree( name );
        
        if ( btree != null )
        {
            return btree;
        }
        
        // Not already loaded, so load it
        BTree<NameRevision, Long> btreeOfBtrees = recordManagerHeader.getBtreeOfBtrees();
        
        // Find the next B-tree reference in the BOB (so revision +1, as we will pick the prev revision)
        if ( ( revision <= 0L ) || ( revision == Long.MAX_VALUE ) )
        {
            revision = Long.MAX_VALUE;
        }
        else
        {
            revision++;
        }
        
        NameRevision nameRevision = new NameRevision( name, revision  );
        
        TupleCursor<NameRevision, Long> cursor = btreeOfBtrees.browseFrom( transaction, nameRevision );
        
        if ( cursor.hasPrev() )
        {
            Tuple<NameRevision, Long> tuple = cursor.prev();
            
            if ( tuple != null )
            {
                try
                {
                    // The B-tree has been found. We got back its offset, so load it
                    return readBTree( transaction, tuple.getValue() );
                }
                catch ( Exception e )
                {
                    throw new IOException( e.getMessage() );
                }
            }
        }
        
        return null;
    }


    /**
     * Move a list of pages to the free page list. A logical page is associated with one
     * or more physical PageIOs, which are on the disk. We have to move all those PagIO instances
     * to the free list, and do the same in memory (we try to keep a reference to a set of
     * free pages.
     *
     * @param btree The B-tree which were owning the pages
     * @param revision The current revision
     * @param pages The pages to free
     * @throws IOException If we had a problem while updating the file
     * @throws EndOfFileExceededException If we tried to write after the end of the file
     */
    /* Package protected */<K, V> void freePages( WriteTransaction transaction, BTree<K, V> btree, long revision, List<Page<K, V>> pages )
        throws EndOfFileExceededException, IOException
    {
        if ( ( pages == null ) || pages.isEmpty() )
        {
            return;
        }
        
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();

        if ( !keepRevisions )
        {
            // if the B-tree doesn't keep revisions, we can safely move
            // the pages to the freed page list.
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( "Freeing the following pages :" );

                for ( Page<K, V> page : pages )
                {
                    LOG.debug( "    {}", page );
                }
            }

            for ( Page<K, V> page : pages )
            {
                long pageOffset = ( ( AbstractPage<K, V> ) page ).getOffset();

                PageIO[] pageIos = readPageIOs( recordManagerHeader.pageSize, pageOffset, Long.MAX_VALUE );

                for ( PageIO pageIo : pageIos )
                {
                    freedPages.add( pageIo );
                }
            }
        }
        else
        {
            // We are keeping revisions of standard B-trees, so we move the pages to the CopiedPages B-tree
            // but only for non managed B-trees
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( "Moving the following pages to the CopiedBtree :" );

                for ( Page<K, V> page : pages )
                {
                    LOG.debug( "    {}", page );
                }
            }

            long[] pageOffsets = new long[pages.size()];
            int pos = 0;

            for ( Page<K, V> page : pages )
            {
                pageOffsets[pos++] = ( ( AbstractPage<K, V> ) page ).offset;
            }

            if ( ( btree.getType() != BTreeTypeEnum.BTREE_OF_BTREES )
                && ( btree.getType() != BTreeTypeEnum.COPIED_PAGES_BTREE ) )
            {
                // Deal with standard B-trees
                RevisionName revisionName = new RevisionName( revision, btree.getName() );

                copiedPageBtree.insert( transaction, revisionName, pageOffsets );

                // Update the RecordManager Copiedpage Offset
                recordManagerHeader.currentCopiedPagesBtreeOffset = ( ( BTree<RevisionName, long[]> ) copiedPageBtree )
                    .getBtreeOffset();
            }
            else
            {
                // Managed B-trees : we simply free the copied pages
                for ( long pageOffset : pageOffsets )
                {
                    PageIO[] pageIos = readPageIOs( recordManagerHeader.pageSize, pageOffset, Long.MAX_VALUE );

                    for ( PageIO pageIo : pageIos )
                    {
                        freedPages.add( pageIo );
                    }
                }
            }
        }
    }


    /**
     * Add a PageIO to the list of free PageIOs
     *
     * @param pageIo The page to free
     * @throws IOException If we weren't capable of updating the file
     */
    /* no qualifier */ void free( WriteTransaction transaction, PageIO pageIo ) throws IOException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        freePageLock.lock();

            // We add the Page's PageIOs before the
            // existing free pages.
            // Link it to the first free page
            pageIo.setNextPage( recordManagerHeader.firstFreePage );

            LOG.debug( "Flushing the first free page" );

            // And flush it to disk
            //FIXME can be flushed last after releasing the lock
            flushPages( recordManagerHeader, pageIo );

            // We can update the firstFreePage offset
            recordManagerHeader.firstFreePage = pageIo.getOffset();

            freePageLock.unlock();
        }


    /**
     * Add an array of PageIOs to the list of free PageIOs
     *
     * @param offsets The offsets of the pages whose associated PageIOs will be fetched and freed.
     * @throws IOException If we weren't capable of updating the file
     */
    /*no qualifier*/ void free( Transaction transaction, long... offsets ) throws IOException
    {
        RecordManagerHeader recordManagerHeader = transaction.getRecordManagerHeader();
        
        freePageLock.lock();

        List<PageIO> pageIos = new ArrayList<>();
        int pageIndex = 0;
            for ( int i = 0; i < offsets.length; i++ )
            {
                PageIO[] ios = readPageIOs( recordManagerHeader.pageSize, offsets[i], Long.MAX_VALUE );

                for ( PageIO io : ios )
                {
                    pageIos.add( io );

                    if ( pageIndex > 0 )
                    {
                        pageIos.get( pageIndex - 1 ).setNextPage( io.getOffset() );
                    }

                    pageIndex++;
                }
            }

        // We add the Page's PageIOs before the
        // existing free pages.
        // Link it to the first free page
        pageIos.get( pageIndex - 1 ).setNextPage( recordManagerHeader.firstFreePage );

        LOG.debug( "Flushing the first free page" );

        // And flush it to disk
        //FIXME can be flushed last after releasing the lock
        flushPages( recordManagerHeader, pageIos.toArray( new PageIO[0] ) );

        // We can update the firstFreePage offset
        recordManagerHeader.firstFreePage = pageIos.get( 0 ).getOffset();

        freePageLock.unlock();
    }


    /**
     * @return the keepRevisions flag
     */
    public boolean isKeepRevisions()
    {
        return keepRevisions;
    }


    /**
     * @param keepRevisions the keepRevisions flag to set
     */
    public void setKeepRevisions( boolean keepRevisions )
    {
        this.keepRevisions = keepRevisions;
    }


    /**
     * Creates a B-tree and automatically adds it to the list of managed btrees
     *
     * @param name the name of the B-tree
     * @param keySerializer key serializer
     * @param valueSerializer value serializer
     * @return a managed B-tree
     * @throws IOException If we weren't able to update the file on disk
     * @throws BTreeAlreadyManagedException If the B-tree is already managed
     */
    public <K, V> BTree<K, V> addBTree( WriteTransaction transaction, String name, ElementSerializer<K> keySerializer,
        ElementSerializer<V> valueSerializer )
        throws IOException, BTreeAlreadyManagedException
    {
        BTreeConfiguration<K, V> config = new BTreeConfiguration<>();

        config.setName( name );
        config.setKeySerializer( keySerializer );
        config.setValueSerializer( valueSerializer );

        BTree<K, V> btree = new BTree<>( transaction, config );
        
        manage( transaction, btree );

        return btree;
    }


    /**
     * Get the current BTreeHeader for a given Btree. It might not exist
     */
    public BTreeHeader getBTreeHeader( String name )
    {
        // Get a lock
        btreeHeadersLock.readLock().lock();

        // get the current BTree Header for this BTree and revision
        BTreeHeader<?, ?> btreeHeader = currentBTreeHeaders.get( name );

        // And unlock 
        btreeHeadersLock.readLock().unlock();

        return btreeHeader;
    }


    /**
     * Get the new BTreeHeader for a given Btree. It might not exist
     */
    public BTreeHeader getNewBTreeHeader( String name )
    {
        // get the current BTree Header for this BTree and revision
        return newBTreeHeaders.get( name );
    }
    
    
    /**
     * @return The byte[] that will contain the Recoed Manager Header 
     */
    /* No qualifier */ byte[] getRecordManagerHeaderBytes()
    {
        return recordManagerHeaderBytes;
    }
    
    
    /**
     * @return The ByteBuffer that will contain the Record Manager Header 
     */
    /* No qualifier */ ByteBuffer getRecordManagerHeaderBuffer()
    {
        return recordManagerHeaderBuffer;
    }


    /**
     * {@inheritDoc}
     */
    public void updateNewBTreeHeaders( BTreeHeader btreeHeader )
    {
        newBTreeHeaders.put( btreeHeader.getName(), btreeHeader );
    }


    private void checkFreePages() throws EndOfFileExceededException, IOException
    {
        RecordManagerHeader recordManagerHeader = recordManagerHeaderReference.get();
        
        //System.out.println( "Checking the free pages, starting from " + Long.toHexString( firstFreePage ) );

        // read all the free pages, add them into a set, to be sure we don't have a cycle
        Set<Long> freePageOffsets = new HashSet<>();

        long currentFreePageOffset = recordManagerHeader.firstFreePage;

        while ( currentFreePageOffset != BTreeConstants.NO_PAGE )
        {
            //System.out.println( "Next page offset :" + Long.toHexString( currentFreePageOffset ) );

            if ( ( currentFreePageOffset % recordManagerHeader.pageSize ) != 0 )
            {
                throw new InvalidOffsetException( "Wrong offset : " + Long.toHexString( currentFreePageOffset ) );
            }

            if ( freePageOffsets.contains( currentFreePageOffset ) )
            {
                throw new InvalidOffsetException( "Offset : " + Long.toHexString( currentFreePageOffset )
                    + " already read, there is a cycle" );
            }

            freePageOffsets.add( currentFreePageOffset );
            PageIO pageIO = fetchPageIO( recordManagerHeader.pageSize, currentFreePageOffset );

            currentFreePageOffset = pageIO.getNextPage();
        }

        return;
    }


    /**
     * sets the threshold of the number of commits to be performed before
     * reclaiming the free pages.
     * 
     * @param pageReclaimerThreshold the number of commits before the reclaimer runs
     */
    /* no qualifier */ void setPageReclaimerThreshold( int pageReclaimerThreshold )
    {
        this.pageReclaimerThreshold = pageReclaimerThreshold;
    }


    /* no qualifier */void _disableReclaimer( boolean toggle )
    {
        this.disableReclaimer = toggle;
    }
    
    
    /* No qualifier */ TransactionContext getContext()
    {
        return context;
    }
    
    
    /**
     * @return A copy of the current RecordManager header
     */
    /* No qualifier */ RecordManagerHeader getRecordManagerHeaderCopy()
    {
        return recordManagerHeaderReference.get().copy();
    }
    
    
    /**
     * @return The current RecordManager header
     */
    /* No qualifier */ RecordManagerHeader getCurrentRecordManagerHeader()
    {
        return recordManagerHeaderReference.get();
    }
    
    
    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "RM free pages : [" );
        
        RecordManagerHeader recordManagerHeader = recordManagerHeaderReference.get();

        if ( recordManagerHeader.firstFreePage != BTreeConstants.NO_PAGE )
        {
            long current = recordManagerHeader.firstFreePage;
            boolean isFirst = true;

            while ( current != BTreeConstants.NO_PAGE )
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
                    pageIo = fetchPageIO( recordManagerHeader.pageSize, current );
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
