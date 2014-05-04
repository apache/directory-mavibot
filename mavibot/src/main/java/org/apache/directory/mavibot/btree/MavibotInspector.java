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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Set;

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.InvalidBTreeException;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.apache.directory.mavibot.btree.util.Strings;


/**
 * A class to examine a Mavibot database file.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class MavibotInspector
{
    private File dbFile;

    private RecordManager rm;

    private BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );


    public MavibotInspector( File dbFile )
    {
        this.dbFile = dbFile;
    }


    boolean checkFilePresence()
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
    }


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


    public void printBTreeNames()
    {
        if ( rm == null )
        {
            System.out.println( "Couldn't find the number of managed btrees" );
            return;
        }

        Set<String> trees = rm.getManagedTrees();
        System.out.println( "\nManaged BTrees:" );
        for ( String t : trees )
        {
            System.out.println( t );
        }
        System.out.println();
    }


    public void checkBTree()
    {
        if ( rm == null )
        {
            System.out.println( "Cannot check BTree(s)" );
            return;
        }

        System.out.print( "BTree Name: " );
        String name = readLine();

        PersistedBTree pb = ( PersistedBTree ) rm.getManagedTree( name );

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

            if ( ( pageSize < 0 ) || ( pageSize < 32 ) || ( ( pageSize & ( ~pageSize + 1 ) ) != pageSize ) )
            {
                throw new InvalidBTreeException( "Wrong page size : " + pageSize );
            }

            // Compute the number of pages in this file
            long nbPages = ( fileSize - RecordManager.RECORD_MANAGER_HEADER_SIZE ) / pageSize;

            // The number of trees. It must be at least 2 and > 0
            int nbBtrees = recordManagerHeader.getInt();

            if ( nbBtrees < 0 )
            {
                throw new InvalidBTreeException( "Wrong nb trees : " + nbBtrees );
            }

            // The first free page offset. It must be either -1 or below file size
            // and its value must be a modulo of pageSize
            long firstFreePage = recordManagerHeader.getLong();

            checkOffset( recordManager, firstFreePage, pageSize );

            int nbPageBits = ( int ) ( nbPages / 32 );

            // Create an array of pages to be checked
            // We use one bit per page. It's 0 when the page
            // hasn't been checked, 1 otherwise.
            int[] checkedPages = new int[nbPageBits + 1];

            // Then the free files
            checkFreePages( recordManager, checkedPages, pageSize, firstFreePage );

            // The B-trees offsets
            long currentBtreeOfBtreesOffset = recordManagerHeader.getLong();
            long previousBtreeOfBtreesOffset = recordManagerHeader.getLong();
            long currentCopiedPagesBtreeOffset = recordManagerHeader.getLong();
            long previousCopiedPagesBtreeOffset = recordManagerHeader.getLong();

            // Check that the previous BOB offset is not pointing to something
            if ( previousBtreeOfBtreesOffset != RecordManager.NO_PAGE )
            {
                throw new InvalidBTreeException( "The previous Btree of Btrees offset is not valid : "
                    + previousBtreeOfBtreesOffset );
            }

            // Check that the previous CPB offset is not pointing to something
            if ( previousCopiedPagesBtreeOffset != RecordManager.NO_PAGE )
            {
                throw new InvalidBTreeException( "The previous Copied Pages Btree offset is not valid : "
                    + previousCopiedPagesBtreeOffset );
            }

            // Check that the current BOB offset is valid
            checkOffset( recordManager, currentBtreeOfBtreesOffset, pageSize );

            // Check that the current CPB offset is valid
            checkOffset( recordManager, currentCopiedPagesBtreeOffset, pageSize );

            // Now, check the BTree of Btrees
            BTree<NameRevision, Long> btreeOfBtrees = BTreeFactory.<NameRevision, Long> createInMemoryBTree();
            checkBtreeOfBtrees( recordManager, checkedPages, pageSize, btreeOfBtrees );

            // And the Copied Pages BTree
            checkCopiedPagesBtree( recordManager, checkedPages, pageSize );

            // The B-trees
            //checkBtrees( recordManager, checkedPages, pageSize, nbBtrees );
            
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
     *
     * @param checkedPages
     * @param pageSize
     * @throws IOException
     * @throws EndOfFileExceededException
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalArgumentException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     */
    private static <K, V> void checkBtreeOfBtrees( RecordManager recordManager, int[] checkedPages, int pageSize, BTree<K, V> btree ) throws EndOfFileExceededException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, IllegalArgumentException, SecurityException, NoSuchFieldException
    {
        // Read the BOB header
        PageIO[] bobHeaderPageIos = recordManager.readPageIOs( recordManager.currentBtreeOfBtreesOffset, Long.MAX_VALUE );

        // update the checkedPages
        updateCheckedPages( checkedPages, pageSize, bobHeaderPageIos );

        //checkBtree( checkedPages, pageSize, bobHeaderPageIos, btree );
    }

    
    /**
     * Check the Copied pages Btree
     *
     * @param checkedPages
     * @param pageSize
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    private static void checkCopiedPagesBtree( RecordManager recordManager, int[] checkedPages, int pageSize ) throws EndOfFileExceededException, IOException
    {
        // Read the CPB header
        PageIO[] cpbHeaderPageIos = recordManager.readPageIOs( recordManager.currentCopiedPagesBtreeOffset, Long.MAX_VALUE );

        // update the checkedPages
        updateCheckedPages( checkedPages, pageSize, cpbHeaderPageIos );

        //checkBtree( checkedPages, pageSize, bobHeaderPageIos, btree );
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
    private static void checkOffset( RecordManager recordManager, long offset, int pageSize ) throws IOException
    {
        if ( ( offset == RecordManager.NO_PAGE ) ||
             ( ( ( offset - RecordManager.RECORD_MANAGER_HEADER_SIZE ) % pageSize ) != 0 ) ||
             ( offset > recordManager.fileChannel.size() ) )
        {
            throw new InvalidBTreeException( "Invalid Offset : " + offset );
        }
    }


    /**
     * Check each B-tree we manage
     * @throws IOException
     * @throws EndOfFileExceededException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private static void checkBtrees( RecordManager recordManager, int[] checkedPages, int pageSize, int nbBtrees ) throws EndOfFileExceededException,
        IOException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        // Iterate on each B-tree until we have exhausted all of them. The number
        // of btrees is just used to check that we have the correct number
        // of stored B-trees, as they are all linked.
        long position = RecordManager.RECORD_MANAGER_HEADER_SIZE;

        for ( int i = 0; i < nbBtrees; i++ )
        {
            // Load the pageIOs containing the B-tree
            PageIO[] pageIos = recordManager.readPageIOs( position, Long.MAX_VALUE );

            // Check that they are correctly linked and not already used
            int pageNb = 0;

            for ( PageIO currentPageIo : pageIos )
            {
                //
                long nextPageOffset = currentPageIo.getNextPage();

                if ( pageNb == pageIos.length - 1 )
                {
                    if ( nextPageOffset != RecordManager.NO_PAGE )
                    {
                        throw new InvalidBTreeException( "The pointer to the next page is not valid, expected NO_PAGE" );
                    }
                }
                else
                {
                    if ( nextPageOffset == RecordManager.NO_PAGE )
                    {
                        throw new InvalidBTreeException( "The pointer to the next page is not valid, NO_PAGE" );
                    }
                }

                if ( ( nextPageOffset != RecordManager.NO_PAGE ) && ( ( nextPageOffset - RecordManager.RECORD_MANAGER_HEADER_SIZE ) % pageSize != 0 ) )
                {
                    throw new InvalidBTreeException( "The pointer to the next page is not valid" );
                }

                // Update the array of processed pages
                setCheckedPage( recordManager, checkedPages, currentPageIo.getOffset(), pageSize );
            }

            // Now check the B-tree
            long nextBtree = checkBtree( recordManager, checkedPages, pageIos, pageSize, i == nbBtrees - 1 );

            if ( ( nextBtree == RecordManager.NO_PAGE ) && ( i < nbBtrees - 1 ) )
            {
                throw new InvalidBTreeException( "The pointer to the next B-tree is incorrect" );
            }

            position = nextBtree;
        }
    }


    /**
     * Check a B-tree
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     */
    private static long checkBtree( RecordManager recordManager, int[] checkedPages, PageIO[] pageIos, int pageSize, boolean isLast )
        throws EndOfFileExceededException, IOException, InstantiationException, IllegalAccessException,
        ClassNotFoundException
    {
        long dataPos = 0L;

        // The B-tree current revision
        long revision = recordManager.readLong( pageIos, dataPos );
        dataPos += RecordManager.LONG_SIZE;

        // The nb elems in the tree
        long nbElems = recordManager.readLong( pageIos, dataPos );
        dataPos += RecordManager.LONG_SIZE;

        // The B-tree rootPage offset
        long rootPageOffset = recordManager.readLong( pageIos, dataPos );

        if ( ( rootPageOffset < 0 ) || ( rootPageOffset > recordManager.fileChannel.size() ) )
        {
            throw new InvalidBTreeException( "The rootpage is incorrect : " + rootPageOffset );
        }

        dataPos += RecordManager.LONG_SIZE;

        // The next B-tree offset
        long nextBtreeOffset = recordManager.readLong( pageIos, dataPos );

        if ( ( ( rootPageOffset < 0 ) && ( !isLast ) ) || ( nextBtreeOffset > recordManager.fileChannel.size() ) )
        {
            throw new InvalidBTreeException( "The rootpage is incorrect : " + rootPageOffset );
        }

        dataPos += RecordManager.LONG_SIZE;

        // The B-tree page size
        int btreePageSize = recordManager.readInt( pageIos, dataPos );

        if ( ( btreePageSize < 2 ) || ( ( btreePageSize & ( ~btreePageSize + 1 ) ) != btreePageSize ) )
        {
            throw new InvalidBTreeException( "The B-tree page size is not a power of 2 : " + btreePageSize );
        }

        dataPos += RecordManager.INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = recordManager.readBytes( pageIos, dataPos );
        dataPos += RecordManager.INT_SIZE;

        dataPos += btreeNameBytes.limit();
        String btreeName = Strings.utf8ToString( btreeNameBytes );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = recordManager.readBytes( pageIos, dataPos );

        String keySerializerFqcn = null;
        dataPos += RecordManager.INT_SIZE;

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
        ByteBuffer valueSerializerBytes = recordManager.readBytes( pageIos, dataPos );

        String valueSerializerFqcn = null;
        dataPos += RecordManager.INT_SIZE;

        if ( valueSerializerBytes != null )
        {
            dataPos += valueSerializerBytes.limit();
            valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );
        }
        else
        {
            valueSerializerFqcn = "";
        }

        // The B-tree allowDuplicates flag
        int allowDuplicates = recordManager.readInt( pageIos, dataPos );
        dataPos += RecordManager.INT_SIZE;

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

        return nextBtreeOffset;
    }


    /**
     * Check the free pages
     *
     * @param checkedPages
     * @throws IOException
     */
    private static void checkFreePages( RecordManager recordManager, int[] checkedPages, int pageSize, long firstFreePage )
        throws IOException
    {
        if ( firstFreePage == RecordManager.NO_PAGE )
        {
            return;
        }

        // Now, read all the free pages
        long currentOffset = firstFreePage;
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

                setCheckedPage( recordManager, checkedPages, currentOffset, pageSize );

                long newOffset = pageIo.getNextPage();
                currentOffset = newOffset;
            }
            catch ( IOException ioe )
            {
                throw new InvalidBTreeException( "Cannot fetch page at : " + currentOffset );
            }
        }
        
        dumpCheckedPages( recordManager, checkedPages );
    }

    
    private static void setCheckedPage( RecordManager recordManager, int[] checkedPages, long offset, int pageSize )
    {
        int pageNumber = ( int ) offset / pageSize;
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
            if ( i == -1 )
            {
                i = 0;
            }
            else
            {
                i++;
                sb.append( " " );
            }

            sb.append( "[" ).append( i ).append(  "] " );


            for ( int j = 0; j < 32; j++ )
            {
                if ( nbPagesChecked >= nbPages )
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
                    int pageSize = rm.getPageSize();
                    long fileSize = rm.fileChannel.size();
                    long nbPages = fileSize / pageSize;
                    int nbPageBits = ( int ) ( nbPages / RecordManager.INT_SIZE );
                    int[] checkedPages = new int[nbPageBits + 1];
                    long firstFreePage = rm.firstFreePage;

                    checkFreePages( rm, checkedPages, pageSize, firstFreePage );
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


    public static void main( String[] args ) throws Exception
    {
        File f = new File( "/tmp/mavibotispector.db" );

        RecordManager rm = new RecordManager( f.getAbsolutePath() );
        String name = "corpus";
        
        if ( !rm.getManagedTrees().contains( name ) )
        {
            rm.addBTree( name, StringSerializer.INSTANCE, StringSerializer.INSTANCE, true );
        }

        BTree btree = rm.getManagedTree( name );
        
        for ( int i = 0; i < 10; i++ )
        {
            btree.insert( "" + i, "" + i );
        }

        rm.close();

        MavibotInspector mi = new MavibotInspector( f );
        mi.start();
    }
}
