/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */
package org.apache.directory.mavibot.btree;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.InvalidBTreeException;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.apache.directory.mavibot.btree.util.Strings;


/**
 * A class to examine a Mavibot database file.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class MavibotInspector
{
    // The file to be read
    private File dbFile;

    // The recordManager
    private RecordManager rm;

    private BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );


    /**
     * Create an instance of MavibotInspector
     * @param dbFile The file to read
     */
    public MavibotInspector( File dbFile )
    {
        this.dbFile = dbFile;
    }


    /**
     * Check that the file exists
     */
    private boolean checkFilePresence()
    {
        if ( dbFile == null )
        {
            System.out.println( "No mavibot database file was given" );
            return false;
        }

        if ( !dbFile.exists() )
        {
            System.out.println( "Given mavibot database file " + dbFile + " doesn't exist" );
            return false;
        }

        return true;
    }


    /**
     * Pretty print the file size
     */
    public void printFileSize() throws IOException
    {
        FileChannel fileChannel = new RandomAccessFile( dbFile, "r" ).getChannel();

        long l = fileChannel.size();

        fileChannel.close();

        String msg;

        if ( l < 1024 )
        {
            msg = l + " bytes";
        }
        else
        {
            msg = ( l / 1024 ) + " KB";
        }

        System.out.println( msg );
        
        fileChannel.close();
    }


    /**
     * Print the number of B-trees
     */
    public void printNumberOfBTrees()
    {
        int nbBtrees = 0;
        
        if ( rm != null )
        {
            nbBtrees = rm.getNbManagedTrees();
        }

        // The number of trees. It must be at least 2 and > 0
        System.out.println( "Total Number of BTrees: " + nbBtrees );
    }


    /**
     * Print the B-tree's name
     */
    public void printBTreeNames()
    {
        if ( rm == null )
        {
            System.out.println( "Couldn't find the number of managed btrees" );
            return;
        }

        Set<String> trees = rm.getManagedTrees();
        System.out.println( "\nManaged BTrees:" );
        
        for ( String tree : trees )
        {
            System.out.println( tree );
        }
        
        System.out.println();
    }


    /**
     * Check a B-tree
     */
    public void checkBTree()
    {
        if ( rm == null )
        {
            System.out.println( "Cannot check BTree(s)" );
            return;
        }

        System.out.print( "BTree Name: " );
        String name = readLine();

        PersistedBTree<?, ?> pb = ( PersistedBTree<?, ?> ) rm.getManagedTree( name );

        if ( pb == null )
        {
            System.out.println( "No BTree exists with the name '" + name + "'" );
            return;
        }

        System.out.println( "\nBTree offset: " + pb.getBtreeOffset() );
        System.out.println( "BTree _info_ offset: " + pb.getBtreeInfoOffset() );
        System.out.println( "BTree root page offset: " + pb.getRootPageOffset() );
        System.out.println( "Number of elements present: " + pb.getNbElems() );
        System.out.println( "BTree Page size: " + pb.getPageSize() );
        System.out.println( "BTree revision: " + pb.getRevision() );
        System.out.println( "Key serializer: " + pb.getKeySerializerFQCN() );
        System.out.println( "Value serializer: " + pb.getValueSerializerFQCN() );
        System.out.println();
    }


    /**
     * Load the full fie into a new RecordManager
     */
    private boolean loadRm()
    {
        try
        {
            if( rm != null )
            {
                System.out.println("Closing record manager");
                rm.close();
            }
            
            rm = new RecordManager( dbFile.getAbsolutePath() );
            System.out.println("Loaded record manager");
        }
        catch ( Exception e )
        {
            System.out.println( "Given database file seems to be corrupted. " + e.getMessage() );
            return false;
        }

        return true;
    }
    
    
    /**
     * Check the whole file
     */
    /* no qualifier */ static void check( RecordManager recordManager )
    {
        try
        {
            // First check the RMheader
            ByteBuffer recordManagerHeader = ByteBuffer.allocate( RecordManager.RECORD_MANAGER_HEADER_SIZE );
            long fileSize = recordManager.fileChannel.size();

            if ( fileSize < RecordManager.RECORD_MANAGER_HEADER_SIZE )
            {
                throw new InvalidBTreeException( "File size too small : " + fileSize );
            }

            // Read the RMHeader
            recordManager.fileChannel.read( recordManagerHeader, 0L );
            recordManagerHeader.flip();

            // The page size. It must be a power of 2, and above 16.
            int pageSize = recordManagerHeader.getInt();

            if ( ( pageSize != recordManager.pageSize ) || ( pageSize < 32 ) || ( ( pageSize & ( ~pageSize + 1 ) ) != pageSize ) )
            {
                throw new InvalidBTreeException( "Wrong page size : " + pageSize );
            }

            // Compute the number of pages in this file
            long nbPages = ( fileSize - RecordManager.RECORD_MANAGER_HEADER_SIZE ) / pageSize;

            // The number of trees. It must be at least 2 and > 0
            int nbBtrees = recordManagerHeader.getInt();

            if ( ( nbBtrees < 0 ) || ( nbBtrees != recordManager.nbBtree ) )
            {
                throw new InvalidBTreeException( "Wrong nb trees : " + nbBtrees );
            }

            // The first free page offset. It must be either -1 or below file size
            // and its value must be a modulo of pageSize
            long firstFreePage = recordManagerHeader.getLong();

            checkOffset( recordManager, firstFreePage );

            int nbPageBits = ( int ) ( nbPages / 32 );

            // Create an array of pages to be checked
            // We use one bit per page. It's 0 when the page
            // hasn't been checked, 1 otherwise.
            int[] checkedPages = new int[nbPageBits + 1];

            // Then the free files
            checkFreePages( recordManager, checkedPages );

            // The B-trees offsets
            long currentBtreeOfBtreesOffset = recordManagerHeader.getLong();
            long previousBtreeOfBtreesOffset = recordManagerHeader.getLong();
            long currentCopiedPagesBtreeOffset = recordManagerHeader.getLong();
            long previousCopiedPagesBtreeOffset = recordManagerHeader.getLong();

            // Check that the previous BOB offset is not pointing to something
            if ( previousBtreeOfBtreesOffset != RecordManager.NO_PAGE )
            {
                System.out.println( "The previous Btree of Btrees offset is not valid : "
                    + previousBtreeOfBtreesOffset );
                return;
            }

            // Check that the previous CPB offset is not pointing to something
            if ( previousCopiedPagesBtreeOffset != RecordManager.NO_PAGE )
            {
                System.out.println( "The previous Copied Pages Btree offset is not valid : "
                    + previousCopiedPagesBtreeOffset );
                return;
            }

            // Check that the current BOB offset is valid
            checkOffset( recordManager, currentBtreeOfBtreesOffset );

            // Check that the current CPB offset is valid
            checkOffset( recordManager, currentCopiedPagesBtreeOffset );

            // Now, check the BTree of Btrees
            BTree<NameRevision, Long> btreeOfBtrees = null; //BTreeFactory.<NameRevision, Long> createPersistedBTree();
            checkBtreeOfBtrees( recordManager, checkedPages, btreeOfBtrees );

            // And the Copied Pages BTree
            //checkCopiedPagesBtree( recordManager, checkedPages );

            /*
            // The B-trees
            ///////////////////////
            // Now, read all the B-trees from the btree of btrees
            TupleCursor<NameRevision, Long> btreeCursor = btreeOfBtrees.browse();
            Map<String, Long> loadedBtrees = new HashMap<String, Long>();

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

            // check the btrees
            for ( String btreeName : loadedBtrees.keySet() )
            {
                long btreeOffset = loadedBtrees.get( btreeName );

                PageIO[] btreePageIos = recordManager.readPageIOs( btreeOffset, Long.MAX_VALUE );
                checkBtree( recordManager, checkedPages, btreePageIos );
            }

            //////////////////////
            */
            
            dumpCheckedPages( recordManager, checkedPages );
        }
        catch ( Exception e )
        {
            // We catch the exception and rethrow it immediately to be able to
            // put a breakpoint here
            e.printStackTrace();
            throw new InvalidBTreeException( "Error : " + e.getMessage() );
        }
    }

    
    /**
     * Check the Btree of Btrees
     */
    private static <K, V> void checkBtreeOfBtrees( RecordManager recordManager, int[] checkedPages, BTree<K, V> btree ) throws Exception
    {
        // Read the BOB header
        PageIO[] bobHeaderPageIos = recordManager.readPageIOs( recordManager.currentBtreeOfBtreesOffset, Long.MAX_VALUE );

        // update the checkedPages
        updateCheckedPages( checkedPages, recordManager.pageSize, bobHeaderPageIos );

        checkBtree( recordManager, checkedPages, bobHeaderPageIos );
    }

    
    /**
     * Check the Copied pages Btree
     */
    private static void checkCopiedPagesBtree( RecordManager recordManager, int[] checkedPages ) throws EndOfFileExceededException, IOException
    {
        // Read the CPB header
        PageIO[] cpbHeaderPageIos = recordManager.readPageIOs( recordManager.currentCopiedPagesBtreeOffset, Long.MAX_VALUE );

        // update the checkedPages
        updateCheckedPages( checkedPages, recordManager.pageSize, cpbHeaderPageIos );

        //checkBtree( recordManager, checkedPages, pageSize, bobHeaderPageIos );
    }
    

    /**
     * Check a B-tree
     */
    private static <K, V> void checkBtree( RecordManager recordManager, int[] checkedPages, PageIO[] pageIos ) throws IOException
    {
        long dataPos = 0L;

        // The B-tree current revision
        recordManager.readLong( pageIos, dataPos );
        dataPos += RecordManager.LONG_SIZE;

        // The nb elems in the tree
        recordManager.readLong( pageIos, dataPos );
        dataPos += RecordManager.LONG_SIZE;

        // The B-tree rootPage offset
        long rootPageOffset = recordManager.readLong( pageIos, dataPos );

        checkOffset( recordManager, rootPageOffset );

        dataPos += RecordManager.LONG_SIZE;

        // The B-tree info offset
        long btreeInfoOffset = recordManager.readLong( pageIos, dataPos );

        checkOffset( recordManager, btreeInfoOffset );

        checkBtreeInfo( recordManager, checkedPages, btreeInfoOffset );

        // Check the elements in the btree itself
        // We will read every single page
        checkBtreeOfBtreesPage( recordManager, checkedPages, rootPageOffset );
    }
    
    
    /**
     * Check the Btree info page
     */
    private static <K, V> void checkBtreeInfo( RecordManager recordManager, int[] checkedPages, long btreeInfoOffset ) throws IOException
    {
        PageIO[] btreeInfoPagesIos = recordManager.readPageIOs( btreeInfoOffset, Long.MAX_VALUE );

        // update the checkedPages
        updateCheckedPages( checkedPages, recordManager.pageSize, btreeInfoPagesIos );

        //((InMemoryBTree<K, V>)btree).setBtreeInfoOffset( btreeInfoPagesIos[0].getOffset() );
        long dataPos = 0L;

        // The B-tree page size
        recordManager.readInt( btreeInfoPagesIos, dataPos );
        dataPos += RecordManager.INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = recordManager.readBytes( btreeInfoPagesIos, dataPos );
        dataPos += RecordManager.INT_SIZE + btreeNameBytes.limit();
        Strings.utf8ToString( btreeNameBytes );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = recordManager.readBytes( btreeInfoPagesIos, dataPos );
        dataPos += RecordManager.INT_SIZE + keySerializerBytes.limit();

        // The valueSerialier FQCN
        ByteBuffer valueSerializerBytes = recordManager.readBytes( btreeInfoPagesIos, dataPos );

        dataPos += RecordManager.INT_SIZE + valueSerializerBytes.limit();

        // The B-tree allowDuplicates flag
        recordManager.readInt( btreeInfoPagesIos, dataPos );
        dataPos += RecordManager.INT_SIZE;
    }

    
    /**
     * Check the Btree of Btrees rootPage
     */
    private static <K, V> void checkBtreeOfBtreesPage( RecordManager recordManager, int[] checkedPages, long pageOffset ) throws EndOfFileExceededException, IOException
    {
        PageIO[] pageIos = recordManager.readPageIOs( pageOffset, Long.MAX_VALUE );

        // Update the checkedPages array
        updateCheckedPages( checkedPages, recordManager.pageSize, pageIos );

        // Deserialize the page now
        long position = 0L;

        // The revision
        long revision = recordManager.readLong( pageIos, position );
        position += RecordManager.LONG_SIZE;

        // The number of elements in the page
        int nbElems = recordManager.readInt( pageIos, position );
        position += RecordManager.INT_SIZE;

        // The size of the data containing the keys and values
        // Reads the bytes containing all the keys and values, if we have some
        // We read  big blob of data into  ByteBuffer, then we will process
        // this ByteBuffer
        ByteBuffer byteBuffer = recordManager.readBytes( pageIos, position );

        // Now, deserialize the data block. If the number of elements
        // is positive, it's a Leaf, otherwise it's a Node
        // Note that only a leaf can have 0 elements, and it's the root page then.
        if ( nbElems >= 0 )
        {
            // It's a leaf, process it as we may have sub-btrees
            checkBtreeOfBtreesLeaf( recordManager, checkedPages, nbElems, revision, byteBuffer, pageIos );
        }
        else
        {
            // It's a node
            long[] children = checkBtreeOfBtreesNode( recordManager, checkedPages, -nbElems, revision, byteBuffer, pageIos );

            for ( int pos = 0; pos <= -nbElems; pos++ )
            {
                // Recursively check the children
                checkBtreeOfBtreesPage( recordManager, checkedPages, children[pos] );
            }
        }
    }
    
    
    /**
     * Check a Btree of Btrees leaf. It contains <revision, name> -> offset.
     */
    private static <K, V> void checkBtreeOfBtreesLeaf( RecordManager recordManager, int[] checkedPages, int nbElems, long revision, ByteBuffer byteBuffer, PageIO[] pageIos ) throws IOException
    {
        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // Read the number of values
                int nbValues = byteBuffer.getInt();
    
                if ( nbValues != 1 )
                {
                    throw new InvalidBTreeException( "We should have only one value for a BOB " + nbValues );
                }
    
                // This is a normal value
                // First, the value, which is an offset, which length should be 12
                int valueLength = byteBuffer.getInt();
                
                if ( valueLength != RecordManager.LONG_SIZE + RecordManager.INT_SIZE )
                {
                    throw new InvalidBTreeException( "The BOB value length is invalid " + valueLength );
                }
    
                // Second, the offset length, which should be 8
                int offsetLength = byteBuffer.getInt();
                
                if ( offsetLength != RecordManager.LONG_SIZE )
                {
                    throw new InvalidBTreeException( "The BOB value offset length is invalid " + offsetLength );
                }
                
                // Then the offset
                long btreeOffset = byteBuffer.getLong();
                
                checkOffset( recordManager, btreeOffset );
                
                // Now, process the key
                // First the key length
                int keyLength = byteBuffer.getInt();
    
                // The length should be at least 12 bytes long
                if ( keyLength < RecordManager.LONG_SIZE + RecordManager.INT_SIZE )
                {
                    throw new InvalidBTreeException( "The BOB key length is invalid " + keyLength );
                }
                
                // Read the revision
                long btreeRevision = byteBuffer.getLong();
                
                // read the btreeName
                int btreeNameLength = byteBuffer.getInt();
    
                // The length should be equals to the btreeRevision + btreeNameLength + 4
                if ( keyLength != RecordManager.LONG_SIZE + RecordManager.INT_SIZE + btreeNameLength )
                {
                    throw new InvalidBTreeException( "The BOB key length is not the expected value " + 
                        ( RecordManager.LONG_SIZE + RecordManager.INT_SIZE + btreeNameLength ) + ", expected " + keyLength );
                }
                
                byte[] bytes = new byte[btreeNameLength];
                byteBuffer.get( bytes );
                //String btreeName = Strings.utf8ToString( bytes );
                
                // Now, we can check the Btree we just found
                //System.out.println( "read <" + btreeName + "," + btreeRevision + "> : 0x" + Long.toHexString( btreeOffset ) );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The BOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }
    }
    
    
    /**
     * Check a Btree of Btrees Node
     */
    private static <K, V> long[] checkBtreeOfBtreesNode( RecordManager recordManager, int[] checkedPages, int nbElems, long revision,
        ByteBuffer byteBuffer, PageIO[] pageIos ) throws IOException
    {
        long[] children = new long[nbElems + 1];

        // Read each value
        for ( int i = 0; i < nbElems; i++ )
        {
            // The offsets of the child
            long firstOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );
            
            checkOffset( recordManager, firstOffset );
            
            long lastOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );

            checkOffset( recordManager, lastOffset );
            
            children[i] = firstOffset;

            // Read the key length
            int keyLength = byteBuffer.getInt();
            
            // The length should be at least 12 bytes long
            if ( keyLength < RecordManager.LONG_SIZE + RecordManager.INT_SIZE )
            {
                throw new InvalidBTreeException( "The BOB key length is invalid " + keyLength );
            }
            
            // Read the revision
            byteBuffer.getLong();
            
            // read the btreeName
            int btreeNameLength = byteBuffer.getInt();

            // The length should be equals to the btreeRevision + btreeNameLength + 4
            if ( keyLength != RecordManager.LONG_SIZE + RecordManager.INT_SIZE + btreeNameLength )
            {
                throw new InvalidBTreeException( "The BOB key length is not the expected value " + 
                    ( RecordManager.LONG_SIZE + RecordManager.INT_SIZE + btreeNameLength ) + ", expected " + keyLength );
            }
            
            // Read the Btree name
            byte[] bytes = new byte[btreeNameLength];
            byteBuffer.get( bytes );
        }

        // And read the last child
        // The offsets of the child
        long firstOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );
        
        checkOffset( recordManager, firstOffset );
        
        long lastOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );

        checkOffset( recordManager, lastOffset );
        
        children[nbElems] = firstOffset;

        // and read the last value, as it's a node
        return children;
    }


    /**
     * Update the array of seen pages.
     */
    private static void updateCheckedPages( int[] checkedPages, int pageSize, PageIO... pageIos )
    {
        for ( PageIO pageIO : pageIos )
        {
            long offset = pageIO.getOffset();

            if ( ( offset % pageSize ) != 0 )
            {
                throw new InvalidBTreeException( "Offset invalid : " + offset );
            }

            int pageNumber = (int)(offset / pageSize);
            int nbBitsPage = ( RecordManager.INT_SIZE << 3 );
            int pageMask = checkedPages[ pageNumber / nbBitsPage ];
            int mask = 1 << pageNumber % nbBitsPage;

            if ( ( pageMask & mask ) != 0 )
            {
                throw new InvalidBTreeException( "The page " + offset + " has already been referenced" );
            }

            pageMask |= mask;
            checkedPages[ pageNumber / nbBitsPage ] = pageMask;
        }
    }


    /**
     * Check the offset to be sure it's a valid one :
     * <ul>
     * <li>It's >= 0</li>
     * <li>It's below the end of the file</li>
     * <li>It's a multiple of the pageSize
     * </ul>
     * @param offset The offset to check
     * @param pageSize the page size
     * @throws InvalidOffsetException If the offset is not valid
     */
    private static void checkOffset( RecordManager recordManager, long offset ) throws IOException
    {
        if ( ( offset == RecordManager.NO_PAGE ) ||
             ( ( ( offset - RecordManager.RECORD_MANAGER_HEADER_SIZE ) % recordManager.pageSize ) != 0 ) ||
             ( offset > recordManager.fileChannel.size() ) )
        {
            throw new InvalidBTreeException( "Invalid Offset : " + offset );
        }
    }


    /**
     * Check the free pages
     */
    private static void checkFreePages( RecordManager recordManager, int[] checkedPages )
        throws IOException
    {
        if ( recordManager.firstFreePage == RecordManager.NO_PAGE )
        {
            return;
        }

        // Now, read all the free pages
        long currentOffset = recordManager.firstFreePage;
        long fileSize = recordManager.fileChannel.size();

        while ( currentOffset != RecordManager.NO_PAGE )
        {
            if ( currentOffset > fileSize )
            {
                System.out.println( "Wrong free page offset, above file size : " + currentOffset );
                return;
            }

            try
            {
                PageIO pageIo = recordManager.fetchPage( currentOffset );

                if ( currentOffset != pageIo.getOffset() )
                {
                    System.out.println( "PageIO offset is incorrect : " + currentOffset + "-"
                        + pageIo.getOffset() );
                    return;
                }

                setCheckedPage( recordManager, checkedPages, currentOffset );

                long newOffset = pageIo.getNextPage();
                currentOffset = newOffset;
            }
            catch ( IOException ioe )
            {
                throw new InvalidBTreeException( "Cannot fetch page at : " + currentOffset );
            }
        }
    }

    
    /**
     * Update the ChekcedPages array
     */
    private static void setCheckedPage( RecordManager recordManager, int[] checkedPages, long offset )
    {
        int pageNumber = ( int ) offset / recordManager.pageSize;
        int nbBitsPage = ( RecordManager.INT_SIZE << 3 );
        long pageMask = checkedPages[ pageNumber / nbBitsPage ];
        long mask = 1L << pageNumber % nbBitsPage;
        
        if ( ( pageMask & mask ) != 0 )
        {
            throw new InvalidBTreeException( "The page " + offset + " has already been referenced" );
        }

        pageMask |= mask;
        
        checkedPages[ pageNumber / nbBitsPage ] |= pageMask;
    }


    /**
     * Output the pages that has been seen ('1') and those which has not been seen ('0'). The '.' represent non-pages
     * at the end of the file.
     */
    private static void dumpCheckedPages( RecordManager recordManager, int[] checkedPages ) throws IOException
    {
        StringBuilder sb = new StringBuilder();
        int i = -1;
        int nbPagesChecked = 0;
        long fileSize = recordManager.fileChannel.size();
        long nbPages = ( fileSize - RecordManager.RECORD_MANAGER_HEADER_SIZE ) / recordManager.pageSize;

        for ( int checkedPage : checkedPages )
        {
            if ( i == 0 )
            {
                sb.append( " " );
                i++;
            }
            else
            {
                i = 0;
            }

            sb.append( "[" ).append( i ).append(  "] " );


            for ( int j = 0; j < 32; j++ )
            {
                if ( nbPagesChecked >= nbPages + 1 )
                {
                    sb.append( "." );
                }
                else
                {
                    if ( ( checkedPage & ( 1 << j ) )  == 0 )
                    {
                        sb.append( "0" );
                    }
                    else
                    {
                        sb.append( "1" );
                    }
                }

                nbPagesChecked++;
            }
        }

        System.out.println( sb.toString() );
    }


    /**
     * The entry point method
     */
    public void start() throws Exception
    {
        if ( !checkFilePresence() )
        {
            return;
        }

        if ( !loadRm() )
        {
            return;
        }

        boolean stop = false;

        while ( !stop )
        {
            System.out.println( "Choose an option:" );
            System.out.println( "1. Print Number of BTrees" );
            System.out.println( "2. Print BTree Names" );
            System.out.println( "3. Inspect BTree" );
            System.out.println( "4. Check Free Pages" );
            System.out.println( "5. Get database file size" );
            System.out.println( "6. Dump RecordManager" );
            System.out.println( "7. Reload RecordManager" );
            System.out.println( "q. Quit" );

            char c = readOption();

            switch ( c )
            {
                case '1':
                    printNumberOfBTrees();
                    break;

                case '2':
                    printBTreeNames();
                    break;

                case '3':
                    checkBTree();
                    break;

                case '4':
                    long fileSize = rm.fileChannel.size();
                    long nbPages = fileSize / rm.pageSize;
                    int nbPageBits = ( int ) ( nbPages / RecordManager.INT_SIZE );
                    int[] checkedPages = new int[nbPageBits + 1];

                    checkFreePages( rm, checkedPages );
                    break;

                case '5':
                    printFileSize();
                    break;

                case '6':
                    check( rm );
                    break;

                case '7':
                    loadRm();
                    break;

                case 'q':
                    stop = true;
                    break;

                default:
                    System.out.println( "Invalid option" );
                    //c = readOption( br );
                    break;
            }
        }

        try
        {
            rm.close();
            br.close();
        }
        catch ( Exception e )
        {
            //ignore
        }
    }


    /**
     * Read the user's interaction
     */
    private String readLine()
    {
        try
        {
            return br.readLine().trim();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    /**
     * Process the input and get back the selected choice
     */
    private char readOption()
    {
        try
        {
            String s = br.readLine();

            if ( s.length() == 0 )
            {
                return ' ';
            }

            return s.charAt( 0 );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    /**
     * Main method
     */
    public static void main( String[] args ) throws Exception
    {
        File f = new File( "/tmp/mavibotispector.db" );

        RecordManager rm = new RecordManager( f.getAbsolutePath() );
        String name = "corpus";
        
        if ( !rm.getManagedTrees().contains( name ) )
        {
            rm.addBTree( name, StringSerializer.INSTANCE, StringSerializer.INSTANCE, true );
        }

        
        BTree<String, String> btree = rm.getManagedTree( name );
        
        for ( int i = 0; i < 10; i++ )
        {
            btree.insert( "" + i, "" + i );
        }

        rm.close();
        
        MavibotInspector mi = new MavibotInspector( f );
        mi.start();
    }
}
