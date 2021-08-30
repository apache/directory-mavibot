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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.InvalidBTreeException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.apache.directory.mavibot.btree.serializer.LongArraySerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
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
    private static RecordManager rm;

    private BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );

    // The name of the two page arrays for the global file and teh free pages
    private static final String GLOBAL_PAGES_NAME = "__global__";
    private static final String FREE_PAGES_NAME = "__free-pages__";

    // The set of page array we already know about
    private static Set<String> knownPagesArrays = new HashSet<String>();

    // Create an array of pages to be checked for each B-tree, plus
    // two others for the free pages and the global one
    // We use one bit per page. It's 0 when the page
    // hasn't been checked, 1 otherwise.
    private static Map<String, int[]> checkedPages = new HashMap<String, int[]>();

    static
    {
        knownPagesArrays.add( GLOBAL_PAGES_NAME );
        knownPagesArrays.add( FREE_PAGES_NAME );
        knownPagesArrays.add( BTreeConstants.BTREE_OF_BTREES_NAME );
        knownPagesArrays.add( BTreeConstants.COPIED_PAGE_BTREE_NAME );
    }


    /**
     * A private class to store a few informations about a btree
     *
    
    private static BtreeInfo btreeInfo;
    
    static
    {
        btreeInfo = new BtreeInfo();
    }

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
    public void printNumberOfBTrees( RecordManagerHeader recordManagerHeader )
    {
        int nbBtrees = 0;

        if ( rm != null )
        {
            nbBtrees = rm.getNbManagedTrees( recordManagerHeader );
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
    public void inspectBTree( RecordManager recordManager ) throws IOException
    {
        if ( recordManager == null )
        {
            System.out.println( "Cannot check BTree(s)" );
            return;
        }

        System.out.print( "BTree Name: " );
        String name = readLine();
        
        Transaction transaction = null;

        try
        {
            transaction = rm.beginReadTransaction();
            BTree<?, ?> pb = ( BTree<?, ?> ) rm.getBtree( transaction, name, Long.MAX_VALUE - 1 );
    
            if ( pb == null )
            {
                System.out.println( "No BTree exists with the name '" + name + "'" );
                return;
            }
    
            System.out.println( "\nBTree offset: " + String.format( "0x%1$08x", pb.getBtreeOffset() ) );
            System.out.println( "BTree _info_ offset: " + String.format( "0x%1$08x", pb.getBtreeInfoOffset() ) );
            System.out.println( "BTree root page offset: " + String.format( "0x%1$08x", pb.getRootPageOffset() ) );
            System.out.println( "Number of elements present: " + pb.getNbElems() );
            System.out.println( "BTree Page size: " + pb.getNbElems() );
            System.out.println( "BTree revision: " + pb.getBtreeHeader().getRevision() );
            System.out.println( "Key serializer: " + pb.getKeySerializerFQCN() );
            System.out.println( "Value serializer: " + pb.getValueSerializerFQCN() );
            System.out.println();
            
            transaction.commit();
        }
        catch ( IOException ioe )
        {
            if ( transaction != null )
            {
                transaction.abort();
            }
        }
    }


    /**
     * Load the full fie into a new RecordManager
     */
    private boolean loadRm()
    {
        try
        {
            if ( rm != null )
            {
                System.out.println( "Closing record manager" );
                rm.close();
            }

            rm = new RecordManager( dbFile.getAbsolutePath() );
            System.out.println( "Loaded record manager" );
        }
        catch ( Exception e )
        {
            System.out.println( "Given database file seems to be corrupted. " + e.getMessage() );
            return false;
        }

        return true;
    }


    /**
     * Check the whole file.
     */
    /* no qualifier */static void check( RecordManager recordManager )
    {
        RecordManagerHeader recordManagerHeader = recordManager.getCurrentRecordManagerHeader();
        
        try
        {
            // Dump the infos
            dumpInfos( recordManager, recordManagerHeader );
            
            rm = recordManager;

            // First check the RMheader
            ByteBuffer recordManagerHeaderBuffer = ByteBuffer.allocate( RecordManager.recordManagerHeaderSize );
            
            long fileSize = recordManager.fileChannel.size();

            if ( fileSize < RecordManager.recordManagerHeaderSize )
            {
                throw new InvalidBTreeException( "File size too small : " + fileSize );
            }

            // Read the RMHeader
            recordManager.fileChannel.read( recordManagerHeaderBuffer, 0L );
            recordManagerHeaderBuffer.flip();

            // The page size. It must be a power of 2, and above 16.
            int pageSize = recordManagerHeaderBuffer.getInt();

            if ( ( pageSize != recordManager.getPageSize( recordManagerHeader ) ) || ( pageSize < 32 )
                || ( ( pageSize & ( ~pageSize + 1 ) ) != pageSize ) )
            {
                throw new InvalidBTreeException( "Wrong page size : " + pageSize );
            }

            // Compute the number of pages in this file
            long nbPages = ( fileSize - RecordManager.recordManagerHeaderSize ) / pageSize;

            // The number of trees. It must be at least >= 2
            int nbBtrees = recordManagerHeaderBuffer.getInt();

            if ( ( nbBtrees < 0 ) || ( nbBtrees != recordManager.getNbManagedTrees( recordManagerHeader ) ) )
            {
                throw new InvalidBTreeException( "Wrong nb trees : " + nbBtrees );
            }
            
            // The current revision
            long revision = recordManagerHeaderBuffer.getLong();
            
            if ( revision != recordManagerHeader.getRevision() )
            {
                throw new InvalidBTreeException( "Wrong revision number : " + revision );
            }

            // The first free page offset. It must be either -1 or below file size
            // and its value must be a modulo of pageSize
            long firstFreePage = recordManagerHeaderBuffer.getLong();

            if ( firstFreePage != BTreeConstants.NO_PAGE )
            {
                checkOffset( recordManager, recordManagerHeader, firstFreePage );
            }

            int nbPageBits = ( int ) ( nbPages / 32 );

            // The global page array
            checkedPages.put( GLOBAL_PAGES_NAME, new int[nbPageBits + 1] );

            // The freePages array
            checkedPages.put( FREE_PAGES_NAME, new int[nbPageBits + 1] );

            // The B-tree of B-trees array
            checkedPages.put( BTreeConstants.BTREE_OF_BTREES_NAME, new int[nbPageBits + 1] );

            // Last, the Copied Pages B-tree array
            checkedPages.put( BTreeConstants.COPIED_PAGE_BTREE_NAME, new int[nbPageBits + 1] );

            // Check the free files
            checkFreePages( recordManager, recordManagerHeader, checkedPages );

            // The B-trees offsets
            long currentBtreeOfBtreesOffset = recordManagerHeaderBuffer.getLong();
            long currentCopiedPagesBtreeOffset = recordManagerHeaderBuffer.getLong();

            // Check that the current LOB offset is valid
            checkOffset( recordManager, recordManagerHeader, currentBtreeOfBtreesOffset );

            // Check that the current CPB offset is valid
            checkOffset( recordManager, recordManagerHeader, currentCopiedPagesBtreeOffset );

            // Now, check the BTree of Btrees
            checkListOfBtrees( recordManager, recordManagerHeader, checkedPages );

            // And the Copied Pages BTree
            checkBtree( recordManager, recordManagerHeader, currentCopiedPagesBtreeOffset, checkedPages );

            // We can now dump the checked pages
            //dumpCheckedPages( recordManager, recordManagerHeader, checkedPages );
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
     * Dump the Mavibot file infos :
     * <ul>
     *   <li>The recordManager header</li>
     *   <li><The copiedPages B-tree/li>
     *   <li>The B-tree of B-trees</li>
     * </ul>
     * @param recordManager The recordMnaager instance
     * @param recordManagerHeader The current RecordManagerHeader 
     * @throws Exception
     */
    public static void dumpInfos( RecordManager recordManager, RecordManagerHeader recordManagerHeader ) throws Exception
    {
        StringBuilder sb = new StringBuilder();
        long nbPages = recordManagerHeader.lastOffset/recordManagerHeader.pageSize;
        String[] pages = new String[(int)nbPages];
        
        // RMH
        sb.append( String.format( "0x%04X", 0L ) ).append( " RMH : " );
        sb.append( "<LOB h[" ).append( recordManagerHeader.getRevision() ).append( "] : " ).append( String.format( "0x%04X",recordManagerHeader.currentListOfBtreesOffset ) );
        sb.append( ">, " );
        sb.append( "<CPB h[" ).append( recordManagerHeader.getRevision() ).append( "] : " ).append( String.format( "0x%04X",recordManagerHeader.currentCopiedPagesBtreeOffset ) );
        sb.append( '>' );
        
        pages[0] = sb.toString();
        
        // The CPB
        dumpCopiedPagesBtree( recordManager, recordManagerHeader, pages );

        // The LOB
        dumpListOfBtrees( recordManager, recordManagerHeader, pages );

        boolean previousEmpty = false;
        
        for ( String page : pages )
        {
            if ( page == null ) 
            {
                if ( !previousEmpty )
                {
                    previousEmpty = true;
                    System.out.println();
                }
                
                System.out.print( "." );
            }
            else
            {
                System.out.println();
                System.out.print( page );
                previousEmpty = false;
            }
        }
        
        System.out.println();
    }


    /**
     * Check the List of Btrees
     */
    private static void checkListOfBtrees( RecordManager recordManager, RecordManagerHeader recordManagerHeader, Map<String, int[]> checkedPages )
        throws Exception
    {
            
        // Read the LOB header
        PageIO[] bobHeaderPageIos = recordManager
            .readPageIOs( recordManagerHeader.pageSize, recordManagerHeader.currentListOfBtreesOffset, Long.MAX_VALUE );

        // update the checkedPages
        updateCheckedPages( recordManagerHeader, checkedPages.get( BTreeConstants.BTREE_OF_BTREES_NAME ), recordManager.getPageSize( recordManagerHeader ),
            bobHeaderPageIos );
        updateCheckedPages( recordManagerHeader, checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.getPageSize( recordManagerHeader ), bobHeaderPageIos );

        long dataPos = 0L;
        
        // The B-tree header page ID
        recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The number of transactions
        int nbTransactions = recordManager.readInt( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE;

        // The current RMH
        // The number of managed B-trees
        int nbBtrees = recordManager.readInt( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE;

        // The current revision
        recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        for ( int i = 0; i < nbBtrees; i++ )
        {
            // The b-tree offset
            long offset = recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
            dataPos += BTreeConstants.LONG_SIZE;

            checkOffset( recordManager, recordManagerHeader, offset );
            checkBtree( recordManager, recordManagerHeader, offset, checkedPages );
        }
        
        // The pending transactions
        
        for ( int i = 0; i < nbTransactions; i++ )
        {
            // The number of managed B-trees
            nbBtrees = recordManager.readInt( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
            dataPos += BTreeConstants.INT_SIZE;

            // The current revision
            recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
            dataPos += BTreeConstants.LONG_SIZE;

            for ( int j = 0; j < nbBtrees; j++ )
            {
                // The b-tree offset
                long offset = recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
                dataPos += BTreeConstants.LONG_SIZE;

                checkOffset( recordManager, recordManagerHeader, offset );
                checkBtree( recordManager, recordManagerHeader, offset, checkedPages );
            }
        }

        // Check the elements in the btree itself
        // We will read every single page
        //checkBtreeOfBtreesPage( recordManager, recordManagerHeader, checkedPages, rootPageOffset );
    }


    /**
     * Dump the List of Btrees
     */
    private static void dumpListOfBtrees( RecordManager recordManager, RecordManagerHeader recordManagerHeader, String[] pages ) throws Exception
    {
        StringBuilder sb = new StringBuilder();
        
        // Read the LOB header
        PageIO[] bobHeaderPageIos = recordManager
            .readPageIOs( recordManagerHeader.pageSize, recordManagerHeader.currentListOfBtreesOffset, Long.MAX_VALUE );

        // Get the offset
        long bobHeaderOffset = bobHeaderPageIos[0].getOffset();

        long dataPos = 0L;
        
        // The B-tree header page ID
        long pageId = recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The number of transactions
        int nbTransactions = recordManager.readInt( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE;

        // The current RMH
        // The number of B-trees
        int nbBtrees = recordManager.readInt( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE;

        // The revision
        long revision = recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // Dump the header
        sb.append( String.format( "0x%04X", bobHeaderOffset ) ).append( " LOB H[" ).append( revision ).append( "] : " );

        for ( int i = 0; i < nbBtrees; i++ )
        { 
            // The B-tree offset
            long offset = recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
            dataPos += BTreeConstants.LONG_SIZE;

            sb.append( "<BH[" ).append( revision ).append( "] : " ).append( String.format( "0x%04X", offset ) ).append( ">, " );
            
            dumpUserBtree( recordManager, recordManagerHeader, "aaa", offset, pages );
        }
        
        sb.append( '\n' );
        nbTransactions --;

        // The pending transactions
        for ( int i = 0; i < nbTransactions; i++ )
        {
            // The current RMH
            // The number of B-trees
            nbBtrees = recordManager.readInt( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
            dataPos += BTreeConstants.INT_SIZE;

            revision = recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
            dataPos += BTreeConstants.LONG_SIZE;
            
            sb.append( String.format( "0x%04X", bobHeaderOffset ) ).append( " LOB H[" ).append( revision ).append( "] : " );

            for ( int j = 0; j < nbBtrees; j++ )
            { 
                // The B-tree offset
                long offset = recordManager.readLong( recordManagerHeader.pageSize, bobHeaderPageIos, dataPos );
                dataPos += BTreeConstants.LONG_SIZE;

                sb.append( "<BH[" ).append( revision ).append( "] : " ).append( String.format( "0x%04X", offset ) ).append( ">, " );
            }

            sb.append( '\n' );
        }
        
        System.out.println( sb.toString() );
    }


    /**
     * Dump the Copied Pages B-tree
     */
    private static void dumpCopiedPagesBtree( RecordManager recordManager, RecordManagerHeader recordManagerHeader, String[] pages ) throws EndOfFileExceededException, IOException
    {
        StringBuilder sb = new StringBuilder();
        
        // Read the CPB header
        PageIO[] cpbbHeaderPageIos = recordManager
            .readPageIOs( recordManagerHeader.pageSize, recordManagerHeader.currentCopiedPagesBtreeOffset, Long.MAX_VALUE );

        // Get the offset
        long cpbHeaderOffset = cpbbHeaderPageIos[0].getOffset();

        long dataPos = 0L;

        // The B-tree page ID
        long id = recordManager.readLong( recordManagerHeader.pageSize, cpbbHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree current revision
        long revision = recordManager.readLong( recordManagerHeader.pageSize, cpbbHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The nb elems in the tree
        recordManager.readLong( recordManagerHeader.pageSize, cpbbHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree rootPage offset
        long rootPageOffset = recordManager.readLong( recordManagerHeader.pageSize, cpbbHeaderPageIos, dataPos );

        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree info offset
        long infoOffset = recordManager.readLong( recordManagerHeader.pageSize, cpbbHeaderPageIos, dataPos );
        
        // Dump the header
        sb.append( String.format( "0x%04X", cpbHeaderOffset ) ).append( " CPB H[" ).append( revision ).append( "] : " );
        sb.append( "<RP[" ).append( revision ).append( "] : " ).append( String.format( "0x%04X", rootPageOffset ) ).append( ">, " );
        sb.append( "<Info:" ).append( String.format( "0x%04X", infoOffset ) ).append( '>' );
        pages[(int)cpbHeaderOffset/recordManagerHeader.pageSize] = sb.toString();
        
        // Dump the info
        dumpBtreeInfo( recordManager, recordManagerHeader, infoOffset, "CPB", pages );
        
        // Dump the CPB content
        dumpCopiedPagesBtreeContent( recordManager, recordManagerHeader, rootPageOffset, RevisionNameSerializer.INSTANCE, LongArraySerializer.INSTANCE, pages );
    }
    
    
    /**
     * Dump a BTree info
     */
    private static <K, V> BTreeInfo<K, V> dumpBtreeInfo( RecordManager recordManager, RecordManagerHeader recordManagerHeader, long offset, String btreeName, String[] pages ) 
        throws EndOfFileExceededException, IOException
    {
        PageIO[] infoPageIos = recordManager .readPageIOs( recordManagerHeader.pageSize, offset, Long.MAX_VALUE );
        StringBuilder sb = new StringBuilder();
        sb.append( String.format( "0x%04X", offset ) ).append( ' ' ).append( btreeName ).append( " Info" );
        BTreeInfo<K, V> btreeInfo = new BTreeInfo<>();
        
        // The info page may contain more than one PageIO
        int nbPage = 0;
        
        for ( PageIO pageIO : infoPageIos )
        {
            if ( nbPage > 0 )
            {
                sb.append( '-' ).append( nbPage );
            }
            
            nbPage++;
            
            int pos = (int)(pageIO.getOffset()/recordManagerHeader.pageSize);
            
            if ( ( pages[pos] != null ) && pages[pos].contains( "---copied page at" ) )
            {
                pages[pos] = new StringBuilder().append( pages[pos] ).append( ' ' ).append( sb.toString() ).toString();
            }
            else
            {
                pages[pos] = sb.toString();
            }
        }
        
        // The B-tree page size
        int dataPos = 0;
        btreeInfo.setPageNbElem( recordManager.readInt( recordManagerHeader.pageSize, infoPageIos, dataPos ) );
        dataPos += BTreeConstants.INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = recordManager.readBytes( recordManagerHeader.pageSize, infoPageIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE + btreeNameBytes.limit();
        btreeInfo.setName( Strings.utf8ToString( btreeNameBytes ) );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = recordManager.readBytes( recordManagerHeader.pageSize, infoPageIos, dataPos );

        if ( keySerializerBytes != null )
        {
            String keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );
            btreeInfo.setKeySerializerFQCN( keySerializerFqcn );

            btreeInfo.setKeySerializer( ( ( ElementSerializer<K> ) getSerializer( keySerializerFqcn ) ) );
        }

        dataPos += BTreeConstants.INT_SIZE + keySerializerBytes.limit();

        // The valueSerialier FQCN
        ByteBuffer valueSerializerBytes = recordManager.readBytes( recordManagerHeader.pageSize, infoPageIos, dataPos );

        if ( valueSerializerBytes != null )
        {
            String valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );
            btreeInfo.setValueSerializerFQCN( valueSerializerFqcn );

            btreeInfo.setValueSerializer( ( ( ElementSerializer<V> ) getSerializer( valueSerializerFqcn ) ) );
        }

        return btreeInfo;
    }

    
    /**
     * Dump a CPB content
     */
    private static void dumpCopiedPagesBtreeContent( RecordManager recordManager, RecordManagerHeader recordManagerHeader, long rootPageOffset, RevisionNameSerializer keySerializer, 
        LongArraySerializer valueSerializer, String[] pages ) throws IOException
    {
        // Process the rootPage
        PageIO[] pageIos = recordManager.readPageIOs( recordManagerHeader.pageSize, rootPageOffset, Long.MAX_VALUE );

        // Deserialize the page now
        long position = 0L;

        // The page ID
        long id = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The revision
        long revision = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The number of elements in the page
        int nbElems = recordManager.readInt( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.INT_SIZE;
        
        if ( nbElems == 0 )
        {
            // No elems, we are done
            StringBuilder sb = new StringBuilder();
            
            sb.append( String.format( "0x%04X", rootPageOffset ) ).append( " CPB RP[" ).append( revision ).append( "] : {}" );
            pages[(int)(rootPageOffset/recordManagerHeader.pageSize)] = sb.toString();
            
            return;
        }

        // The size of the data containing the keys and values
        // Reads the bytes containing all the keys and values, if we have some
        // We read  big blob of data into  ByteBuffer, then we will process
        // this ByteBuffer
        ByteBuffer byteBuffer = recordManager.readBytes( recordManagerHeader.pageSize, pageIos, position );

        // Now, deserialize the data block. If the number of elements
        // is positive, it's a Leaf, otherwise it's a Node
        // Note that only a leaf can have 0 elements, and it's the root page then.
        if ( nbElems > 0 )
        {
            // It's a leaf, process it as we may have sub-btrees
            dumpCopiedPagesBtreeLeaf( recordManager, recordManagerHeader, keySerializer, valueSerializer, nbElems, revision, byteBuffer, pageIos[0].getOffset(), pages );
            
            // Update the attached PageIOS
            if ( pageIos.length > 0 )
            {
                for ( int i = 1; i < pageIos.length; i++ )
                {
                    PageIO pageIo = pageIos[i];
                    StringBuilder sb = new StringBuilder();
                    
                    sb.append( String.format( "0x%04X", pageIo.getOffset() ) ).append( " CPB RP[" ).append( revision ).append( "]-" ).append( i );
                    pages[(int)(pageIo.getOffset()/recordManagerHeader.pageSize)] = sb.toString();
                }
            }
        }
        else
        {
            // It's a node, dump its content, and iterate on the children
            dumpCopiedPagesBtreeNode( recordManager, recordManagerHeader, keySerializer, valueSerializer, -nbElems, revision, byteBuffer, pageIos[0].getOffset(), pages );
            
            // Update the attached PageIOS
            if ( pageIos.length > 0 )
            {
                for ( int i = 1; i < pageIos.length; i++ )
                {
                    PageIO pageIo = pageIos[i];
                    StringBuilder sb = new StringBuilder();
                    
                    sb.append( String.format( "0x%04X", pageIo.getOffset() ) ).append( " CPB RP[" ).append( revision ).append( "]-" ).append( i );
                    pages[(int)(pageIo.getOffset()/recordManagerHeader.pageSize)] = sb.toString();
                }
            }
        }
    }

    
    /**
     * Dump a LOB content
     */
    private static void dumpBtreeOfBtreesContent( RecordManager recordManager, RecordManagerHeader recordManagerHeader, long rootPageOffset, NameRevisionSerializer keySerializer, 
        LongSerializer valueSerializer, String[] pages ) throws IOException
    {
        // Process the rootPage
        PageIO[] pageIos = recordManager.readPageIOs( recordManagerHeader.pageSize, rootPageOffset, Long.MAX_VALUE );

        // Deserialize the page now
        long position = 0L;

        // The page ID
        long id = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The revision
        long revision = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The number of elements in the page
        int nbElems = recordManager.readInt( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.INT_SIZE;
        
        if ( nbElems == 0 )
        {
            // No elems, we are done
            StringBuilder sb = new StringBuilder();
            
            sb.append( String.format( "0x%04X", rootPageOffset ) ).append( " LOB RP[" ).append( revision ).append( "] : {}" );
            pages[(int)(rootPageOffset/recordManagerHeader.pageSize)] = sb.toString();
            
            return;
        }

        // The size of the data containing the keys and values
        // Reads the bytes containing all the keys and values, if we have some
        // We read  big blob of data into  ByteBuffer, then we will process
        // this ByteBuffer
        ByteBuffer byteBuffer = recordManager.readBytes( recordManagerHeader.pageSize, pageIos, position );

        // Now, deserialize the data block. If the number of elements
        // is positive, it's a Leaf, otherwise it's a Node
        // Note that only a leaf can have 0 elements, and it's the root page then.
        if ( nbElems > 0 )
        {
            // It's a leaf, process it as we may have sub-btrees
            dumpBtreeOfBtreesLeaf( recordManager, recordManagerHeader, keySerializer, valueSerializer, nbElems, revision, byteBuffer, pageIos[0].getOffset(), pages );
            
            // Update the attached PageIOS
            if ( pageIos.length > 0 )
            {
                for ( int i = 1; i < pageIos.length; i++ )
                {
                    PageIO pageIo = pageIos[i];
                    StringBuilder sb = new StringBuilder();
                    
                    sb.append( String.format( "0x%04X", pageIo.getOffset() ) ).append( " LOB RP[" ).append( revision ).append( "]-" ).append( i );
                    int pos = (int)(pageIo.getOffset()/recordManagerHeader.pageSize);
                    
                    if ( pages[pos] != null )
                    {
                        pages[pos] = new StringBuilder().append( pages[pos] ).append( ' ' ).append( sb.toString() ).toString();
                    }
                    else
                    {
                        pages[pos] = sb.toString();
                    }
                }
            }
        }
        else
        {
            // It's a node, dump its content, and iterate on the children
            dumpBtreeOfBtreesNode( recordManager, recordManagerHeader, keySerializer, valueSerializer, -nbElems, revision, byteBuffer, pageIos[0].getOffset(), pages );
            
            // Update the attached PageIOS
            if ( pageIos.length > 0 )
            {
                for ( int i = 1; i < pageIos.length; i++ )
                {
                    PageIO pageIo = pageIos[i];
                    StringBuilder sb = new StringBuilder();
                    
                    sb.append( String.format( "0x%04X", pageIo.getOffset() ) ).append( " LOB RP[" ).append( revision ).append( "]-" ).append( i );
                    pages[(int)(pageIo.getOffset()/recordManagerHeader.pageSize)] = sb.toString();
                }
            }
        }
    }

    
    /**
     * Dump a LOB Btree leaf.
     */
    private static void dumpBtreeOfBtreesLeaf( RecordManager recordManager, RecordManagerHeader recordManagerHeader, NameRevisionSerializer keySerializer,
        LongSerializer valueSerializer, int nbElems, long revision, ByteBuffer byteBuffer, long cpbOffset, String[] pages ) throws IOException
    {
        StringBuilder sb = new StringBuilder();

        sb.append( String.format( "0x%04X", cpbOffset ) ).append( " LOB RP[" ).append( revision ).append( "](l) : " );
        
        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // just deserialize the keys and values
                // The value
                long btreeOffset = valueSerializer.deserialize( byteBuffer );

                // the key
                NameRevision key = keySerializer.deserialize( byteBuffer );;
                String btreeName = key.getName();
                
                if ( i > 0 )
                {
                    sb.append( ", " );
                }

                sb.append( key ).append( "->" ).append( String.format( "0x%04X", btreeOffset ) );
                
                // Dump the user btree
                dumpUserBtree( recordManager, recordManagerHeader, btreeName, btreeOffset, pages );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The LOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }

        pages[(int)(cpbOffset/recordManagerHeader.pageSize)] = sb.toString();
    }
    
    
    /**
     * Dump a LOB Btree Node.
     */
    private static void dumpBtreeOfBtreesNode( RecordManager recordManager, RecordManagerHeader recordManagerHeader, NameRevisionSerializer keySerializer,
        LongSerializer valueSerializer, int nbElems, long revision, ByteBuffer byteBuffer, long cpbOffset, String[] pages ) throws IOException
    {
        StringBuilder sb = new StringBuilder();

        sb.append( String.format( "0x%04X", cpbOffset ) ).append( " LOB RP[" ).append( revision ).append( "](n) : " );
        
        long[] children = new long[nbElems + 1];
        int i = 0;

        // Read each key and value
        for ( ; i < nbElems; i++ )
        {
            try
            {
                // The offsets of the child
                children[i] = LongSerializer.INSTANCE.deserialize( byteBuffer );

                // the key
                NameRevision key = keySerializer.deserialize( byteBuffer );
                
                if ( i > 0 )
                {
                    sb.append( " | " );
                }

                sb.append( String.format( "0x%04X", children[i] ) ).append( " | " ).append( key );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The LOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }

        // The last child
        children[i] = LongSerializer.INSTANCE.deserialize( byteBuffer );
        sb.append( " | " ).append( String.format( "0x%04X", children[i] ) );

        pages[(int)(cpbOffset/recordManagerHeader.pageSize)] = sb.toString();
        
        // Now iterate on each children
        for ( long child : children )
        {
            dumpBtreeOfBtreesContent( recordManager, recordManagerHeader, child, keySerializer, valueSerializer, pages );
        }
    }

    
    /**
     * Dump a user Btree
     */
    private static <K, V> void dumpUserBtree( RecordManager recordManager, RecordManagerHeader recordManagerHeader, String btreeName, long headerOffset, String[] pages ) 
        throws EndOfFileExceededException, IOException
    {
        StringBuilder sb = new StringBuilder();
        
        // Read the CPB header
        PageIO[] btreeOffsetPageIos = recordManager.readPageIOs( recordManagerHeader.pageSize, headerOffset, Long.MAX_VALUE );

        long dataPos = 0L;

        // The B-tree header page ID
        long id = recordManager.readLong( recordManagerHeader.pageSize, btreeOffsetPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree current revision
        long revision = recordManager.readLong( recordManagerHeader.pageSize, btreeOffsetPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The nb elems in the tree
        recordManager.readLong( recordManagerHeader.pageSize, btreeOffsetPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree rootPage offset
        long rootPageOffset = recordManager.readLong( recordManagerHeader.pageSize, btreeOffsetPageIos, dataPos );

        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree info offset
        long infoOffset = recordManager.readLong( recordManagerHeader.pageSize, btreeOffsetPageIos, dataPos );
        
        // Dump the header
        sb.append( String.format( "0x%04X", headerOffset ) ).append( ' ' ).append( btreeName ).append( " H[" ).append( revision ).append( "] : " );
        sb.append( "<RP[" ).append( revision ).append( "] : " ).append( String.format( "0x%04X", rootPageOffset ) ).append( ">, " );
        sb.append( "<Info:" ).append( String.format( "0x%04X", infoOffset ) ).append( '>' );

        int pos = (int)headerOffset/recordManagerHeader.pageSize;
        
        if ( pages[pos] != null )
        {
            pages[pos] = new StringBuilder().append( pages[pos] ).append( ' ' ).append( sb.toString() ).toString();
        }
        else
        {
            pages[pos] = sb.toString();
        }
        
        // Dump the info
        BTreeInfo<K,V> btreeInfo = dumpBtreeInfo( recordManager, recordManagerHeader, infoOffset, btreeName, pages );
        
        // Dump the user B-tree content
        dumpUserBtreeContent( recordManager, recordManagerHeader, btreeInfo, rootPageOffset, pages );
    }

    
    /**
     * Dump a User B-tree content
     */
    private static <K, V> void dumpUserBtreeContent( RecordManager recordManager, RecordManagerHeader recordManagerHeader, BTreeInfo<K, V> btreeInfo, long rootPageOffset, 
        String[] pages ) throws IOException
    {
        // Process the rootPage
        PageIO[] pageIos = recordManager.readPageIOs( recordManagerHeader.pageSize, rootPageOffset, Long.MAX_VALUE );

        // Deserialize the page now
        long position = 0L;

        // The pageID
        long id = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The revision
        long revision = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The number of elements in the page
        int nbElems = recordManager.readInt( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.INT_SIZE;
        
        if ( nbElems == 0 )
        {
            // No elems, we are done
            StringBuilder sb = new StringBuilder();
            
            sb.append( String.format( "0x%04X", rootPageOffset ) ).append( ' ' ).append( btreeInfo.getName() ).append( " RP[" ).append( revision ).append( "] : {}" );
            pages[(int)(rootPageOffset/recordManagerHeader.pageSize)] = sb.toString();
            
            return;
        }

        // The size of the data containing the keys and values
        // Reads the bytes containing all the keys and values, if we have some
        // We read  big blob of data into  ByteBuffer, then we will process
        // this ByteBuffer
        ByteBuffer byteBuffer = recordManager.readBytes( recordManagerHeader.pageSize, pageIos, position );

        // Now, deserialize the data block. If the number of elements
        // is positive, it's a Leaf, otherwise it's a Node
        // Note that only a leaf can have 0 elements, and it's the root page then.
        if ( nbElems > 0 )
        {
            // It's a leaf, process it as we may have sub-btrees
            dumpUserBtreeLeaf( recordManager, recordManagerHeader, btreeInfo, nbElems, revision, byteBuffer, pageIos[0].getOffset(), pages );
            
            // Update the attached PageIOS
            if ( pageIos.length > 0 )
            {
                for ( int i = 1; i < pageIos.length; i++ )
                {
                    PageIO pageIo = pageIos[i];
                    StringBuilder sb = new StringBuilder();
                    
                    sb.append( String.format( "0x%04X", pageIo.getOffset() ) ).append( ' ' ).append( btreeInfo.getName() );
                    sb.append( " RP[" ).append( revision ).append( "]-" ).append( i );
                    int pos = (int)(pageIo.getOffset()/recordManagerHeader.pageSize);
                    
                    if ( pages[pos] != null )
                    {
                        pages[pos] = new StringBuilder().append( pages[pos] ).append( ' ' ).append( sb.toString() ).toString();
                    }
                    else
                    {
                        pages[pos] = sb.toString();
                    }
                }
            }
        }
        else
        {
            // It's a node, dump its content, and iterate on the children
            dumpUserBtreeNode( recordManager, recordManagerHeader, btreeInfo, -nbElems, revision, byteBuffer, pageIos[0].getOffset(), pages );
            
            // Update the attached PageIOS
            if ( pageIos.length > 0 )
            {
                for ( int i = 1; i < pageIos.length; i++ )
                {
                    PageIO pageIo = pageIos[i];
                    StringBuilder sb = new StringBuilder();
                    
                    sb.append( String.format( "0x%04X", pageIo.getOffset() ) ).append( ' ' ).append( btreeInfo.getName() );
                    sb.append( " RP[" ).append( revision ).append( "]-" ).append( i );
                    pages[(int)(pageIo.getOffset()/recordManagerHeader.pageSize)] = sb.toString();
                }
            }
        }
    }
    
    
    /**
     * Dump a User Btree leaf.
     */
    private static <K, V> void dumpUserBtreeLeaf( RecordManager recordManager, RecordManagerHeader recordManagerHeader, BTreeInfo<K, V> btreeInfo,
        int nbElems, long revision, ByteBuffer byteBuffer, long cpbOffset, String[] pages ) throws IOException
    {
        StringBuilder sb = new StringBuilder();

        sb.append( String.format( "0x%04X", cpbOffset ) ).append( ' ' ).append( btreeInfo.getName() ).append( " RP[" ).append( revision ).append( "](l) : " );
        
        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // just deserialize the keys and values
                // The value
                V value = btreeInfo.getValueSerializer().deserialize( byteBuffer );

                // the key
                K key = btreeInfo.getKeySerializer().deserialize( byteBuffer );
                
                if ( i > 0 )
                {
                    sb.append( ", " );
                }

                sb.append( '<' ).append( key ).append( ", " ).append( value ).append( '>' );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The user leaf byte buffer is too short : " + bue.getMessage() );
            }
        }

        pages[(int)(cpbOffset/recordManagerHeader.pageSize)] = sb.toString();
    }
    
    
    /**
     * Dump a User Btree Node.
     */
    private static <K, V> void dumpUserBtreeNode( RecordManager recordManager, RecordManagerHeader recordManagerHeader, BTreeInfo<K, V> btreeInfo,
        int nbElems, long revision, ByteBuffer byteBuffer, long pageOffset, String[] pages ) throws IOException
    {
        StringBuilder sb = new StringBuilder();

        sb.append( String.format( "0x%04X", pageOffset ) ).append( ' ' ).append( btreeInfo.getName() ).append( " RP[" ).append( revision ).append( "](n) : " );
        
        long[] children = new long[nbElems + 1];
        int i = 0;

        // Read each key and value
        for ( ; i < nbElems; i++ )
        {
            try
            {
                // The offsets of the child
                children[i] = LongSerializer.INSTANCE.deserialize( byteBuffer );

                // the key
                K key = btreeInfo.getKeySerializer().deserialize( byteBuffer );
                
                if ( i > 0 )
                {
                    sb.append( " | " );
                }

                sb.append( String.format( "0x%04X", children[i] ) ).append( " | " ).append( key );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The User leaf byte buffer is too short : " + bue.getMessage() );
            }
        }

        // The last child
        children[i] = LongSerializer.INSTANCE.deserialize( byteBuffer );
        sb.append( " | " ).append( String.format( "0x%04X", children[i] ) );

        pages[(int)(pageOffset/recordManagerHeader.pageSize)] = sb.toString();
        
        // Now iterate on each children
        for ( long child : children )
        {
            dumpUserBtreeContent( recordManager, recordManagerHeader, btreeInfo, child, pages );
        }
        
        System.out.println( sb.toString() );
    }

    
    /**
     * Dump a CPB Btree leaf.
     */
    private static void dumpCopiedPagesBtreeLeaf( RecordManager recordManager, RecordManagerHeader recordManagerHeader, RevisionNameSerializer keySerializer,
        LongArraySerializer valueSerializer, int nbElems, long revision, ByteBuffer byteBuffer, long cpbOffset, String[] pages ) throws IOException
    {
        StringBuilder sb = new StringBuilder();

        sb.append( String.format( "0x%04X", cpbOffset ) ).append( " CPB RP[" ).append( revision ).append( "](l) : " );
        
        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // just deserialize the keys and values
                // The value
                long[] value = valueSerializer.deserialize( byteBuffer );

                // the key
                RevisionName key = keySerializer.deserialize( byteBuffer );
                
                if ( i > 0 )
                {
                    sb.append( ", " );
                }

                sb.append( key ).append( "->{" );
                
                boolean isFirst = true;
                
                for ( long offset : value )
                {
                    if ( isFirst )
                    {
                        isFirst = false;
                    }
                    else
                    {
                        sb.append( ", " );
                    }
                    
                    sb.append( String.format( "0x%04X", offset ) );
                    
                    // Process the copied pages offset now
                    StringBuilder sb1 = new StringBuilder();
                    
                    sb1.append( String.format( "0x%04X", offset ) );
                    sb1.append( " ---copied page at r" ).append( key.getRevision() ).append( "---" );
                    pages[(int)(offset/recordManagerHeader.pageSize)] = sb1.toString();
                }
                
                sb.append( '}' );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The LOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }

        pages[(int)(cpbOffset/recordManagerHeader.pageSize)] = sb.toString();
    }
    
    
    /**
     * Dump a CPB Btree Node.
     */
    private static void dumpCopiedPagesBtreeNode( RecordManager recordManager, RecordManagerHeader recordManagerHeader, RevisionNameSerializer keySerializer,
        LongArraySerializer valueSerializer, int nbElems, long revision, ByteBuffer byteBuffer, long cpbOffset, String[] pages ) throws IOException
    {
        StringBuilder sb = new StringBuilder();

        sb.append( String.format( "0x%04X", cpbOffset ) ).append( " CPB RP[" ).append( revision ).append( "](n) : " );
        
        long[] children = new long[nbElems + 1];
        int i = 0;

        // Read each key and value
        for ( ; i < nbElems; i++ )
        {
            try
            {
                // The offsets of the child
                children[i] = LongSerializer.INSTANCE.deserialize( byteBuffer );

                // the key
                RevisionName key = keySerializer.deserialize( byteBuffer );
                
                if ( i > 0 )
                {
                    sb.append( " | " );
                }

                sb.append( String.format( "0x%04X", children[i] ) ).append( " | " ).append( key );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The CPB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }

        // The last child
        children[i] = LongSerializer.INSTANCE.deserialize( byteBuffer );
        sb.append( " | " ).append( String.format( "0x%04X", children[i] ) );

        pages[(int)(cpbOffset/recordManagerHeader.pageSize)] = sb.toString();
        
        // Now iterate on each children
        for ( long child : children )
        {
            dumpCopiedPagesBtreeContent( recordManager, recordManagerHeader, child, keySerializer, valueSerializer, pages );
        }
    }


    /**
     * Check a user's B-tree
     */
    private static <K, V> void checkBtree( RecordManager recordManager, RecordManagerHeader recordManagerHeader, long btreeOffset,
        Map<String, int[]> checkedPages ) throws Exception
    {
        // Read the B-tree header
        PageIO[] btreeHeaderPageIos = recordManager.readPageIOs( recordManagerHeader.pageSize, btreeOffset, Long.MAX_VALUE );

        long dataPos = 0L;

        // The B-tree page ID
        long id = recordManager.readLong( recordManagerHeader.pageSize, btreeHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree current revision
        long btreeRevision = recordManager.readLong( recordManagerHeader.pageSize, btreeHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The nb elems in the tree
        recordManager.readLong( recordManagerHeader.pageSize, btreeHeaderPageIos, dataPos );
        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree rootPage offset
        long rootPageOffset = recordManager.readLong( recordManagerHeader.pageSize, btreeHeaderPageIos, dataPos );

        checkOffset( recordManager, recordManagerHeader, rootPageOffset );

        dataPos += BTreeConstants.LONG_SIZE;

        // The B-tree info offset
        long btreeInfoOffset = recordManager.readLong( recordManagerHeader.pageSize, btreeHeaderPageIos, dataPos );

        checkOffset( recordManager, recordManagerHeader, btreeInfoOffset );

        BTreeInfo<K, V> btreeInfo = checkBtreeInfo( recordManager, recordManagerHeader, checkedPages, btreeInfoOffset, btreeRevision );

        // Update the checked pages
        updateCheckedPages( recordManagerHeader, checkedPages.get( btreeInfo.getName() ), recordManager.getPageSize( recordManagerHeader ), btreeHeaderPageIos );
        updateCheckedPages( recordManagerHeader, checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.getPageSize( recordManagerHeader ), btreeHeaderPageIos );

        // And now, process the rootPage
        checkBtreePage( recordManager, recordManagerHeader, btreeInfo, checkedPages, rootPageOffset );
    }


    /**
     * Check the Btree of Btrees rootPage
     */
    private static <K, V> void checkBtreePage( RecordManager recordManager, RecordManagerHeader recordManagerHeader, BTreeInfo<K, V> btreeInfo,
        Map<String, int[]> checkedPages, long pageOffset ) throws Exception
    {
        PageIO[] pageIos = recordManager.readPageIOs( recordManagerHeader.pageSize, pageOffset, Long.MAX_VALUE );

        // Update the checkedPages array
        updateCheckedPages( recordManagerHeader, checkedPages.get( btreeInfo.getName() ), recordManager.getPageSize( recordManagerHeader ), pageIos );
        updateCheckedPages( recordManagerHeader, checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.getPageSize( recordManagerHeader ), pageIos );

        // Deserialize the page now
        long position = 0L;

        // The page ID
        long id = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The revision
        long revision = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The number of elements in the page
        int nbElems = recordManager.readInt( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.INT_SIZE;

        // The size of the data containing the keys and values
        // Reads the bytes containing all the keys and values, if we have some
        // We read  big blob of data into  ByteBuffer, then we will process
        // this ByteBuffer
        ByteBuffer byteBuffer = recordManager.readBytes( recordManagerHeader.pageSize, pageIos, position );

        // Now, deserialize the data block. If the number of elements
        // is positive, it's a Leaf, otherwise it's a Node
        // Note that only a leaf can have 0 elements, and it's the root page then.
        if ( nbElems >= 0 )
        {
            // It's a leaf, process it as we may have sub-btrees
            checkBtreeLeaf( recordManager, recordManagerHeader, btreeInfo, checkedPages, nbElems, revision, byteBuffer, pageIos );
        }
        else
        {
            // It's a node
            long[] children = checkBtreeNode( recordManager, recordManagerHeader, btreeInfo, checkedPages, -nbElems, revision, byteBuffer,
                pageIos );

            for ( int pos = 0; pos <= -nbElems; pos++ )
            {
                // Recursively check the children
                checkBtreePage( recordManager, recordManagerHeader, btreeInfo, checkedPages, children[pos] );
            }
        }
    }


    /**
     * Check the Btree info page
     * @throws ClassNotFoundException 
     */
    private static <K, V> BTreeInfo<K, V> checkBtreeInfo( RecordManager recordManager, RecordManagerHeader recordManagerHeader, Map<String, int[]> checkedPages,
        long btreeInfoOffset, long btreeRevision ) throws IOException
    {
        BTreeInfo<K, V> btreeInfo = new BTreeInfo<>();

        PageIO[] btreeInfoPagesIos = recordManager.readPageIOs( recordManagerHeader.pageSize, btreeInfoOffset, Long.MAX_VALUE );

        long dataPos = 0L;

        // The B-tree page size
        recordManager.readInt( recordManagerHeader.pageSize, btreeInfoPagesIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = recordManager.readBytes( recordManagerHeader.pageSize, btreeInfoPagesIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE + btreeNameBytes.limit();
        String btreeName = Strings.utf8ToString( btreeNameBytes );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = recordManager.readBytes( recordManagerHeader.pageSize, btreeInfoPagesIos, dataPos );

        if ( keySerializerBytes != null )
        {
            String keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );

            btreeInfo.setKeySerializer( ( ( ElementSerializer<K> ) getSerializer( keySerializerFqcn ) ) );
        }

        dataPos += BTreeConstants.INT_SIZE + keySerializerBytes.limit();

        // The valueSerialier FQCN
        ByteBuffer valueSerializerBytes = recordManager.readBytes( recordManagerHeader.pageSize, btreeInfoPagesIos, dataPos );

        if ( valueSerializerBytes != null )
        {
            String valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );

            btreeInfo.setValueSerializer( ( ( ElementSerializer<V> ) getSerializer( valueSerializerFqcn ) ) );
        }

        dataPos += BTreeConstants.INT_SIZE + valueSerializerBytes.limit();

        // The B-tree allowDuplicates flag
        recordManager.readInt( recordManagerHeader.pageSize, btreeInfoPagesIos, dataPos );
        dataPos += BTreeConstants.INT_SIZE;

        // update the checkedPages
        if ( !BTreeConstants.COPIED_PAGE_BTREE_NAME.equals( btreeName )
            && !BTreeConstants.BTREE_OF_BTREES_NAME.equals( btreeName ) )
        {
            //btreeName = btreeName + "<" + btreeRevision + ">";
        }

        btreeInfo.setName( btreeName );

        // Update the checkedPages
        int[] checkedPagesArray = checkedPages.get( btreeName );

        if ( checkedPagesArray == null )
        {
            // Add the new name in the checkedPage name if it's not already there
            checkedPagesArray = createPageArray( recordManager, recordManagerHeader );
            checkedPages.put( btreeName, checkedPagesArray );
        }

        updateCheckedPages( recordManagerHeader, checkedPagesArray, recordManager.getPageSize( recordManagerHeader ), btreeInfoPagesIos );
        updateCheckedPages( recordManagerHeader, checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.getPageSize( recordManagerHeader ), btreeInfoPagesIos );

        return btreeInfo;
    }


    /**
     * Get back the serializer instance
     */
    @SuppressWarnings("unchecked")
    private static <T> ElementSerializer<T> getSerializer( String serializerFqcn )
    {
        try
        {
            Class<?> serializerClass = Class.forName( serializerFqcn );
            ElementSerializer<T> serializer = null;

            try
            {
                serializer = ( ElementSerializer<T> ) serializerClass.getDeclaredField( "INSTANCE" ).get( null );
            }
            catch ( NoSuchFieldException e )
            {
                // ignore
            }

            if ( serializer == null )
            {
                serializer = ( ElementSerializer<T> ) serializerClass.newInstance();
            }

            return serializer;
        }
        catch ( Exception e )
        {
            throw new InvalidBTreeException( "Error : " + e.getMessage() );
        }
    }


    /**
     * Check the Btree of Btrees rootPage
     */
    private static <K, V> void checkBtreeOfBtreesPage( RecordManager recordManager, RecordManagerHeader recordManagerHeader, Map<String, int[]> checkedPages,
        long pageOffset ) throws Exception
    {
        PageIO[] pageIos = recordManager.readPageIOs( recordManagerHeader.pageSize, pageOffset, Long.MAX_VALUE );

        // Update the checkedPages array
        updateCheckedPages( recordManagerHeader, checkedPages.get( BTreeConstants.BTREE_OF_BTREES_NAME ), recordManager.getPageSize( recordManagerHeader ), pageIos );
        updateCheckedPages( recordManagerHeader, checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.getPageSize( recordManagerHeader ), pageIos );

        // Deserialize the page now
        long position = 0L;

        // The page ID
        long id = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The revision
        long revision = recordManager.readLong( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.LONG_SIZE;

        // The number of elements in the page
        int nbElems = recordManager.readInt( recordManagerHeader.pageSize, pageIos, position );
        position += BTreeConstants.INT_SIZE;

        // The size of the data containing the keys and values
        // Reads the bytes containing all the keys and values, if we have some
        // We read  big blob of data into  ByteBuffer, then we will process
        // this ByteBuffer
        ByteBuffer byteBuffer = recordManager.readBytes( recordManagerHeader.pageSize, pageIos, position );

        // Now, deserialize the data block. If the number of elements
        // is positive, it's a Leaf, otherwise it's a Node
        // Note that only a leaf can have 0 elements, and it's the root page then.
        if ( nbElems >= 0 )
        {
            // It's a leaf, process it as we may have sub-btrees
            checkBtreeOfBtreesLeaf( recordManager, recordManagerHeader, checkedPages, nbElems, revision, byteBuffer, pageIos );
        }
        else
        {
            // It's a node
            long[] children = checkBtreeOfBtreesNode( recordManager, recordManagerHeader, checkedPages, -nbElems, revision, byteBuffer,
                pageIos );

            for ( int pos = 0; pos <= -nbElems; pos++ )
            {
                // Recursively check the children
                checkBtreeOfBtreesPage( recordManager, recordManagerHeader, checkedPages, children[pos] );
            }
        }
    }


    /**
     * Check a Btree of Btrees leaf. It contains <revision, name> -> offset.
     */
    private static void checkBtreeOfBtreesLeaf( RecordManager recordManager, RecordManagerHeader recordManagerHeader, Map<String, int[]> checkedPages,
        int nbElems, long revision, ByteBuffer byteBuffer, PageIO[] pageIos ) throws Exception
    {
        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // This is a normal value, an offset
                long btreeOffset = byteBuffer.getLong();

                checkOffset( recordManager, recordManagerHeader , btreeOffset );

                // Now, process the key
                RevisionName revisionName = RevisionNameSerializer.INSTANCE.deserialize( byteBuffer );

                // Add the new name in the checkedPage name if it's not already there
                int[] btreePagesArray = createPageArray( recordManager, recordManagerHeader );
                checkedPages.put( revisionName.getName(), btreePagesArray );

                // Now, we can check the Btree we just found
                checkBtree( recordManager, recordManagerHeader, btreeOffset, checkedPages );

                //System.out.println( "read <" + btreeName + "," + btreeRevision + "> : 0x" + Long.toHexString( btreeOffset ) );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The LOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }
    }


    /**
     * Check a Btree leaf.
     */
    private static <K, V> void checkBtreeLeaf( RecordManager recordManager, RecordManagerHeader recordManagerHeader, BTreeInfo<K, V> btreeInfo,
        Map<String, int[]> checkedPages, int nbElems, long revision, ByteBuffer byteBuffer, PageIO[] pageIos )
        throws Exception
    {
        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // just deserialize the keys and values
                // The value
                btreeInfo.getValueSerializer().deserialize( byteBuffer );

                // the key
                btreeInfo.getKeySerializer().deserialize( byteBuffer );
            }
            catch ( BufferUnderflowException bue )
            {
                bue.printStackTrace();
                throw new InvalidBTreeException( "The LOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }
    }


    /**
     * Check a Btree of Btrees Node
     */
    private static <K, V> long[] checkBtreeOfBtreesNode( RecordManager recordManager, RecordManagerHeader recordManagerHeader, Map<String, int[]> checkedPages,
        int nbElems, long revision,
        ByteBuffer byteBuffer, PageIO[] pageIos ) throws IOException
    {
        long[] children = new long[nbElems + 1];

        // Read each value
        for ( int i = 0; i < nbElems; i++ )
        {
            // The offsets of the child
            long firstOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );

            checkOffset( recordManager, recordManagerHeader, firstOffset );

            children[i] = firstOffset;

            // Read the revision
            byteBuffer.getLong();

            // read the btreeName
            int btreeNameLength = byteBuffer.getInt();

            // Read the Btree name
            byte[] bytes = new byte[btreeNameLength];
            byteBuffer.get( bytes );
        }

        // And read the last child
        // The offsets of the child
        long firstOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );

        checkOffset( recordManager, recordManagerHeader, firstOffset );

        children[nbElems] = firstOffset;

        // and read the last value, as it's a node
        return children;
    }


    /**
     * Check a Btree node.
     */
    private static <K, V> long[] checkBtreeNode( RecordManager recordManager, RecordManagerHeader recordManagerHeader, BTreeInfo<K, V> btreeInfo,
        Map<String, int[]> checkedPages, int nbElems, long revision, ByteBuffer byteBuffer, PageIO[] pageIos )
        throws Exception
    {
        long[] children = new long[nbElems + 1];

        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // The offsets of the child
                long firstOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );

                checkOffset( recordManager, recordManagerHeader, firstOffset );

                children[i] = firstOffset;

                // Now, read the key
                btreeInfo.getKeySerializer().deserialize( byteBuffer );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The LOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }

        // The last child
        // The offsets of the child
        long firstOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );

        checkOffset( recordManager, recordManagerHeader, firstOffset );

        children[nbElems] = firstOffset;

        return children;
    }


    /**
     * Create an array of bits for pages 
     */
    private static int[] createPageArray( RecordManager recordManager, RecordManagerHeader recordManagerHeader ) throws IOException
    {
        long fileSize = recordManager.fileChannel.size();
        int pageSize = recordManager.getPageSize( recordManagerHeader );
        long nbPages = ( fileSize - RecordManager.recordManagerHeaderSize ) / pageSize;
        int nbPageBits = ( int ) ( nbPages / 32 );

        return new int[nbPageBits + 1];
    }


    /**
     * Update the array of seen pages.
     */
    private static void updateCheckedPages( RecordManagerHeader recordManagerHeader, int[] checkedPages, int pageSize, PageIO... pageIos )
    {
        for ( PageIO pageIO : pageIos )
        {
            long offset = pageIO.getOffset();

            setCheckedPage( rm, recordManagerHeader, checkedPages, offset );
        }
    }


    /**
     * Check the offset to be sure it's a valid one :
     * <ul>
     * <li>It's >= 0</li>
     * <li>It's below the end of the file</li>
     * <li>It's a multiple of the pageSize
     * </ul>
     */
    private static void checkOffset( RecordManager recordManager, RecordManagerHeader recordManagerHeader, long offset ) throws IOException
    {
        if ( ( offset == BTreeConstants.NO_PAGE ) ||
            ( ( ( offset - RecordManager.recordManagerHeaderSize ) % recordManager.getPageSize( recordManagerHeader ) ) != 0 ) ||
            ( offset > recordManager.fileChannel.size() ) )
        {
            throw new InvalidBTreeException( "Invalid Offset : " + offset );
        }
    }


    /**
     * Check the free pages
     */
    private static void checkFreePages( RecordManager recordManager, RecordManagerHeader recordManagerHeader, Map<String, int[]> checkedPages )
        throws IOException
    {
        if ( recordManagerHeader.firstFreePage == BTreeConstants.NO_PAGE )
        {
            return;
        }

        // Now, read all the free pages
        long currentOffset = recordManagerHeader.firstFreePage;
        long fileSize = recordManager.fileChannel.size();

        while ( currentOffset != BTreeConstants.NO_PAGE )
        {
            if ( currentOffset > fileSize )
            {
                System.out.println( "Wrong free page offset, above file size : " + currentOffset );
                return;
            }

            try
            {
                PageIO pageIo = recordManager.fetchPageIO( recordManagerHeader.pageSize, currentOffset );

                if ( currentOffset != pageIo.getOffset() )
                {
                    System.out.println( "PageIO offset is incorrect : " + currentOffset + "-"
                        + pageIo.getOffset() );
                    return;
                }

                setCheckedPage( recordManager, recordManagerHeader, checkedPages.get( GLOBAL_PAGES_NAME ), currentOffset );
                setCheckedPage( recordManager, recordManagerHeader, checkedPages.get( FREE_PAGES_NAME ), currentOffset );

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
     * Update the CheckedPages array
     */
    private static void setCheckedPage( RecordManager recordManager, RecordManagerHeader recordManagerHeader, int[] checkedPages, long offset )
    {
        int pageNumber = ( int ) offset / recordManager.getPageSize( recordManagerHeader );
        int nbBitsPage = ( BTreeConstants.INT_SIZE << 3 );
        long pageMask = checkedPages[pageNumber / nbBitsPage];
        long mask = 1L << pageNumber % nbBitsPage;

        if ( ( pageMask & mask ) != 0 )
        {
            //throw new InvalidBTreeException( "The page " + offset + " has already been referenced" );
        }

        pageMask |= mask;

        checkedPages[pageNumber / nbBitsPage] |= pageMask;
    }


    /**
     * Output the pages that has been seen ('1') and those which has not been seen ('0'). The '.' represent non-pages
     * at the end of the file.
     */
    private static void dumpCheckedPages( RecordManager recordManager, RecordManagerHeader recordManagerHeader, Map<String, int[]> checkedPages )
        throws IOException
    {
        // First dump the global array
        int[] globalArray = checkedPages.get( GLOBAL_PAGES_NAME );
        String result = dumpPageArray( recordManager, recordManagerHeader, globalArray );

        String dump = String.format( "%1$-40s : %2$s", GLOBAL_PAGES_NAME, result );
        System.out.println( dump );

        // The free pages array
        int[] freePagesArray = checkedPages.get( FREE_PAGES_NAME );
        result = dumpPageArray( recordManager, recordManagerHeader, freePagesArray );

        dump = String.format( "%1$-40s : %2$s", FREE_PAGES_NAME, result );
        System.out.println( dump );

        // The B-tree of B-trees pages array
        int[] btreeOfBtreesArray = checkedPages.get( BTreeConstants.BTREE_OF_BTREES_NAME );
        result = dumpPageArray( recordManager, recordManagerHeader, btreeOfBtreesArray );

        dump = String.format( "%1$-40s : %2$s", BTreeConstants.BTREE_OF_BTREES_NAME, result );
        System.out.println( dump );

        // The Copied page B-tree pages array
        int[] copiedPagesArray = checkedPages.get( BTreeConstants.COPIED_PAGE_BTREE_NAME );
        result = dumpPageArray( recordManager, recordManagerHeader, copiedPagesArray );

        dump = String.format( "%1$-40s : %2$s", BTreeConstants.COPIED_PAGE_BTREE_NAME, result );
        System.out.println( dump );

        // And now, all the other btree arrays
        for ( String btreeName : checkedPages.keySet() )
        {
            // Don't do the array we have already processed
            if ( knownPagesArrays.contains( btreeName ) )
            {
                continue;
            }

            int[] btreePagesArray = checkedPages.get( btreeName );
            result = dumpPageArray( recordManager, recordManagerHeader, btreePagesArray );

            dump = String.format( "%1$-40s : %2$s", btreeName, result );
            System.out.println( dump );
        }
    }


    /**
     * @see #getPageOffsets()
     */
    public static List<Long> getFreePages( RecordManagerHeader recordManagerHeader ) throws IOException
    {
        return getPageOffsets( recordManagerHeader, FREE_PAGES_NAME );
    }

    
    /**
     * @see #getPageOffsets()
     */
    public static List<Long> getGlobalPages( RecordManagerHeader recordManagerHeader ) throws IOException
    {
        return getPageOffsets( recordManagerHeader, GLOBAL_PAGES_NAME );
    }

    
    /**
     * Gives a list of offsets of pages from the page array associated wit the given name.
     * 
     * This method should always be called after calling check() method.
     * 
     * @return a list of offsets
     * @throws IOException
     */
    public static List<Long> getPageOffsets( RecordManagerHeader recordManagerHeader, String pageArrayName ) throws IOException
    {
        List<Long> lst = new ArrayList<>();
        
        int[] fparry = checkedPages.get( pageArrayName );

        long nbPagesChecked = 0; // the 0th page will always be of RM header
        long fileSize = rm.fileChannel.size();
        long nbPages = ( fileSize - RecordManager.recordManagerHeaderSize ) / rm.getPageSize( recordManagerHeader );

        for ( int checkedPage : fparry )
        {
            for ( int j = 0; j < 32; j++ )
            {

                if ( nbPagesChecked > nbPages + 1 )
                {
                    break;
                }
                else
                {
                    int mask = ( checkedPage & ( 1 << j ) );
                    if ( mask != 0 )
                    {
                        lst.add( nbPagesChecked * rm.getPageSize( recordManagerHeader ));
                    }
                }
                
                nbPagesChecked++;
            }
        }
        
        return lst;
    }
    
    
    /**
     * Process a page array
     */
    private static String dumpPageArray( RecordManager recordManager, RecordManagerHeader recordManagerHeader, int[] checkedPages ) throws IOException
    {
        StringBuilder sb = new StringBuilder();
        int i = -1;
        int nbPagesChecked = 0;
        long fileSize = recordManager.fileChannel.size();
        long nbPages = ( fileSize - RecordManager.recordManagerHeaderSize ) / recordManager.getPageSize( recordManagerHeader );

        for ( int checkedPage : checkedPages )
        {
            i++;

            sb.append( " [" ).append( i ).append( "] " );

            for ( int j = 0; j < 32; j++ )
            {
                if ( nbPagesChecked >= nbPages + 1 )
                {
                    sb.append( "." );
                }
                else
                {
                    if ( ( checkedPage & ( 1 << j ) ) == 0 )
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

        return sb.toString();
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
            System.out.println( "n - Print Number of BTrees" );
            System.out.println( "b - Print BTree Names" );
            System.out.println( "i - Inspect BTree" );
            System.out.println( "c - Check Free Pages" );
            System.out.println( "s - Get database file size" );
            System.out.println( "d - Dump RecordManager" );
            System.out.println( "r - Reload RecordManager" );
            System.out.println( "o - Read page at offset" );
            System.out.println( "q - Quit" );

            char c = readOption();

            switch ( c )
            {
                case 'n':
                    printNumberOfBTrees( rm.getCurrentRecordManagerHeader() );
                    break;

                case 'b':
                    printBTreeNames();
                    break;

                case 'i':
                    inspectBTree( rm );
                    break;

                case 'c':
                    long fileSize = rm.fileChannel.size();
                    long nbPages = fileSize / rm.getPageSize( rm.getCurrentRecordManagerHeader() );
                    int nbPageBits = ( int ) ( nbPages / BTreeConstants.INT_SIZE );

                    Map<String, int[]> checkedPages = new HashMap<String, int[]>( 2 );

                    // The global page array
                    checkedPages.put( GLOBAL_PAGES_NAME, new int[nbPageBits + 1] );

                    // The freePages array
                    checkedPages.put( FREE_PAGES_NAME, new int[nbPageBits + 1] );

                    checkFreePages( rm, rm.getCurrentRecordManagerHeader(), checkedPages );
                    break;

                case 's':
                    printFileSize();
                    break;

                case 'd':
                    check( rm );
                    break;

                case 'r':
                    loadRm();
                    break;

                case 'o':
                    readPageAt(rm.getCurrentRecordManagerHeader() );
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
            String line = br.readLine();

            if ( line != null )
            {
                return line.trim();
            }
            else
            {
                return "";
            }
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

            if ( ( s == null ) || ( s.length() == 0 ) )
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

    
    private void readPageAt( RecordManagerHeader recordManagerHeader ) throws IOException
    {
        System.out.println();
        System.out.print( "Offset: " );
        
        String s = readLine();
        
        long offset = -1;
        
        try
        {
            offset = Long.parseLong( s.trim() );
        }
        catch( Exception e )
        {
            offset = -1;
        }
        
        if( offset < 0 || offset > (rm.fileChannel.size() - BTreeConstants.DEFAULT_PAGE_SIZE) )
        {
            System.out.println( "Invalid offset " + s );
            return;
        }
        
        PageIO io = rm.fetchPageIO( recordManagerHeader.pageSize, offset );

        List<Long> ll = new ArrayList<Long>();
        ll.add( offset );
        
        do
        {
            long next = io.getNextPage();
            ll.add( next );
            if ( next == -1 )
            {
                break;
            }
            
            io = rm.fetchPageIO( recordManagerHeader.pageSize, next );
        }
        while( true );
        
        int i = 0;
        for ( ; i < ll.size() - 2; i++ )
        {
            System.out.print( ll.get( i ) + " --> ");
        }
        
        System.out.println( ll.get( i ) );
    }

    
    /**
     * Main method
     */
    public static void main( String[] args ) throws Exception
    {

        if ( args.length == 0 )
        {
            System.out.println( "Usage java MavibotInspector <db-file-path>" );
            System.exit( 0 );
        }
        
        File f = new File( args[0] );

        MavibotInspector mi = new MavibotInspector( f );
        mi.start();
    }
}

