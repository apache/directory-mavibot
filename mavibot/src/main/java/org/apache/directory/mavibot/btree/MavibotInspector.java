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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.directory.mavibot.btree.exception.InvalidBTreeException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
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

    // The name of the two page arrays for the global file and teh free pages
    private static final String GLOBAL_PAGES_NAME = "__global__";
    private static final String FREE_PAGES_NAME = "__free-pages__";

    // The set of page array we already know about
    private static Set<String> knownPagesArrays =  new HashSet<String>();
    
    static
    {
        knownPagesArrays.add( GLOBAL_PAGES_NAME );
        knownPagesArrays.add( FREE_PAGES_NAME );
        knownPagesArrays.add( RecordManager.BTREE_OF_BTREES_NAME );
        knownPagesArrays.add( RecordManager.COPIED_PAGE_BTREE_NAME );
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

        System.out.println( "\nBTree offset: " + String.format( "0x%1$08x", pb.getBtreeOffset() ) );
        System.out.println( "BTree _info_ offset: " + String.format( "0x%1$08x", pb.getBtreeInfoOffset() ) );
        System.out.println( "BTree root page offset: " + String.format( "0x%1$08x", pb.getRootPageOffset() ) );
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

            if ( firstFreePage != RecordManager.NO_PAGE )
            {
                checkOffset( recordManager, firstFreePage );
            }

            int nbPageBits = ( int ) ( nbPages / 32 );

            // Create an array of pages to be checked for each B-tree, plus
            // two others for the free pages and the global one
            // We use one bit per page. It's 0 when the page
            // hasn't been checked, 1 otherwise.
            Map<String, int[]> checkedPages = new HashMap<String, int[]>(nbBtrees + 4);

            // The global page array
            checkedPages.put( GLOBAL_PAGES_NAME, new int[nbPageBits + 1] );

            // The freePages array
            checkedPages.put( FREE_PAGES_NAME, new int[nbPageBits + 1] );
            
            // The B-tree of B-trees array
            checkedPages.put( RecordManager.BTREE_OF_BTREES_NAME, new int[nbPageBits + 1] );
            
            // Last, the Copied Pages B-tree array
            checkedPages.put( RecordManager.COPIED_PAGE_BTREE_NAME, new int[nbPageBits + 1] );

            // Check the free files
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
            checkBtreeOfBtrees( recordManager, checkedPages );

            // And the Copied Pages BTree
            checkBtree( recordManager, currentCopiedPagesBtreeOffset, checkedPages );

            // We can now dump the checked pages
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
    private static <K, V> void checkBtreeOfBtrees( RecordManager recordManager, Map<String, int[]> checkedPages ) throws Exception
    {
        // Read the BOB header
        PageIO[] bobHeaderPageIos = recordManager.readPageIOs( recordManager.currentBtreeOfBtreesOffset, Long.MAX_VALUE );

        // update the checkedPages
        updateCheckedPages( checkedPages.get( RecordManager.BTREE_OF_BTREES_NAME), recordManager.pageSize, bobHeaderPageIos );
        updateCheckedPages( checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.pageSize, bobHeaderPageIos );

        long dataPos = 0L;

        // The B-tree current revision
        recordManager.readLong( bobHeaderPageIos, dataPos );
        dataPos += RecordManager.LONG_SIZE;

        // The nb elems in the tree
        recordManager.readLong( bobHeaderPageIos, dataPos );
        dataPos += RecordManager.LONG_SIZE;

        // The B-tree rootPage offset
        long rootPageOffset = recordManager.readLong( bobHeaderPageIos, dataPos );

        checkOffset( recordManager, rootPageOffset );

        dataPos += RecordManager.LONG_SIZE;

        // The B-tree info offset
        long btreeInfoOffset = recordManager.readLong( bobHeaderPageIos, dataPos );

        checkOffset( recordManager, btreeInfoOffset );

        checkBtreeInfo( recordManager, checkedPages, btreeInfoOffset, -1L );

        // Check the elements in the btree itself
        // We will read every single page
        checkBtreeOfBtreesPage( recordManager, checkedPages, rootPageOffset );
    }

    
    /**
     * Check a user's B-tree
     */
    private static <K, V> void checkBtree( RecordManager recordManager, long btreeOffset, Map<String, int[]> checkedPages ) throws Exception
    {
        // Read the B-tree header
        PageIO[] btreeHeaderPageIos = recordManager.readPageIOs( btreeOffset, Long.MAX_VALUE );

        long dataPos = 0L;

        // The B-tree current revision
        long btreeRevision = recordManager.readLong( btreeHeaderPageIos, dataPos );
        dataPos += RecordManager.LONG_SIZE;

        // The nb elems in the tree
        recordManager.readLong( btreeHeaderPageIos, dataPos );
        dataPos += RecordManager.LONG_SIZE;

        // The B-tree rootPage offset
        long rootPageOffset = recordManager.readLong( btreeHeaderPageIos, dataPos );

        checkOffset( recordManager, rootPageOffset );

        dataPos += RecordManager.LONG_SIZE;

        // The B-tree info offset
        long btreeInfoOffset = recordManager.readLong( btreeHeaderPageIos, dataPos );

        checkOffset( recordManager, btreeInfoOffset );

        BtreeInfo<K, V> btreeInfo = checkBtreeInfo( recordManager, checkedPages, btreeInfoOffset, btreeRevision );

        // Update the checked pages
        updateCheckedPages( checkedPages.get( btreeInfo.btreeName ), recordManager.pageSize, btreeHeaderPageIos );
        updateCheckedPages( checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.pageSize, btreeHeaderPageIos );
        
        // And now, process the rootPage
        checkBtreePage( recordManager, btreeInfo, checkedPages, rootPageOffset );
    }

    
    /**
     * Check the Btree of Btrees rootPage
     */
    private static <K, V> void checkBtreePage( RecordManager recordManager, BtreeInfo<K, V> btreeInfo, Map<String, int[]> checkedPages, long pageOffset ) throws Exception
    {
        PageIO[] pageIos = recordManager.readPageIOs( pageOffset, Long.MAX_VALUE );

        // Update the checkedPages array
        updateCheckedPages( checkedPages.get( btreeInfo.btreeName ), recordManager.pageSize, pageIos );
        updateCheckedPages( checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.pageSize, pageIos );

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
            checkBtreeLeaf( recordManager, btreeInfo, checkedPages, nbElems, revision, byteBuffer, pageIos );
        }
        else
        {
            // It's a node
            long[] children = checkBtreeNode( recordManager, btreeInfo, checkedPages, -nbElems, revision, byteBuffer, pageIos );

            for ( int pos = 0; pos <= -nbElems; pos++ )
            {
                // Recursively check the children
                checkBtreePage( recordManager, btreeInfo, checkedPages, children[pos] );
            }
        }
    }

    
    /**
     * Check the Btree info page
     * @throws ClassNotFoundException 
     */
    private static <K, V> BtreeInfo<K, V> checkBtreeInfo( RecordManager recordManager, Map<String, int[]> checkedPages, long btreeInfoOffset, long btreeRevision ) throws IOException
    {
        BtreeInfo<K, V> btreeInfo = new BtreeInfo<K, V>();
        
        PageIO[] btreeInfoPagesIos = recordManager.readPageIOs( btreeInfoOffset, Long.MAX_VALUE );

        long dataPos = 0L;

        // The B-tree page size
        recordManager.readInt( btreeInfoPagesIos, dataPos );
        dataPos += RecordManager.INT_SIZE;

        // The tree name
        ByteBuffer btreeNameBytes = recordManager.readBytes( btreeInfoPagesIos, dataPos );
        dataPos += RecordManager.INT_SIZE + btreeNameBytes.limit();
        String btreeName = Strings.utf8ToString( btreeNameBytes );

        // The keySerializer FQCN
        ByteBuffer keySerializerBytes = recordManager.readBytes( btreeInfoPagesIos, dataPos );

        if ( keySerializerBytes != null )
        {
            String keySerializerFqcn = Strings.utf8ToString( keySerializerBytes );
            
            btreeInfo.keySerializer = getSerializer(  keySerializerFqcn );
        }

        dataPos += RecordManager.INT_SIZE + keySerializerBytes.limit();

        // The valueSerialier FQCN
        ByteBuffer valueSerializerBytes = recordManager.readBytes( btreeInfoPagesIos, dataPos );

        if ( valueSerializerBytes != null )
        {
            String valueSerializerFqcn = Strings.utf8ToString( valueSerializerBytes );
            
            btreeInfo.valueSerializer = getSerializer( valueSerializerFqcn );
        }

        dataPos += RecordManager.INT_SIZE + valueSerializerBytes.limit();

        // The B-tree allowDuplicates flag
        recordManager.readInt( btreeInfoPagesIos, dataPos );
        dataPos += RecordManager.INT_SIZE;

        // update the checkedPages
        if ( !RecordManager.COPIED_PAGE_BTREE_NAME.equals( btreeName ) && !RecordManager.BTREE_OF_BTREES_NAME.equals( btreeName ) )
        {
            btreeName = btreeName + "<" + btreeRevision + ">";
        }

        btreeInfo.btreeName = btreeName;
        
        // Update the checkedPages
        int[] checkedPagesArray = checkedPages.get( btreeName );
        
        if ( checkedPagesArray == null )
        {
            // Add the new name in the checkedPage name if it's not already there
            checkedPagesArray = createPageArray( recordManager );
            checkedPages.put( btreeName, checkedPagesArray );
        }
        
        updateCheckedPages( checkedPagesArray, recordManager.pageSize, btreeInfoPagesIos );
        updateCheckedPages( checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.pageSize, btreeInfoPagesIos );
        
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
            catch( NoSuchFieldException e )
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
    private static <K, V> void checkBtreeOfBtreesPage( RecordManager recordManager, Map<String, int[]> checkedPages, long pageOffset ) throws Exception
    {
        PageIO[] pageIos = recordManager.readPageIOs( pageOffset, Long.MAX_VALUE );

        // Update the checkedPages array
        updateCheckedPages( checkedPages.get( RecordManager.BTREE_OF_BTREES_NAME), recordManager.pageSize, pageIos );
        updateCheckedPages( checkedPages.get( GLOBAL_PAGES_NAME ), recordManager.pageSize, pageIos );

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
    private static <K, V> void checkBtreeOfBtreesLeaf( RecordManager recordManager, Map<String, int[]> checkedPages, int nbElems, long revision, ByteBuffer byteBuffer, PageIO[] pageIos ) throws Exception
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
                String btreeName = Strings.utf8ToString( bytes );
                
                // Add the new name in the checkedPage name if it's not already there
                int[] btreePagesArray = createPageArray( recordManager );
                checkedPages.put( btreeName + "<" + btreeRevision + ">", btreePagesArray );
                
                // Now, we can check the Btree we just found
                checkBtree( recordManager, btreeOffset, checkedPages );
                
                //System.out.println( "read <" + btreeName + "," + btreeRevision + "> : 0x" + Long.toHexString( btreeOffset ) );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The BOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }
    }
    
    
    /**
     * Check a Btree leaf.
     */
    private static <K, V> void checkBtreeLeaf( RecordManager recordManager, BtreeInfo<K, V> btreeInfo, Map<String, int[]> checkedPages, int nbElems, long revision, ByteBuffer byteBuffer, PageIO[] pageIos ) throws Exception
    {
        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // Read the number of values
                int nbValues = byteBuffer.getInt();
    
                if ( nbValues < 0 )
                {
                    // This is a sub-btree. Read the offset
                    long subBtreeOffset = byteBuffer.getLong();
                    
                    // And process the sub-btree
                    checkBtree( recordManager, subBtreeOffset, checkedPages );
                    
                    // Now, process the key
                    // The key length
                    byteBuffer.getInt();
                    
                    // The key itself
                    btreeInfo.keySerializer.deserialize( byteBuffer );
                }
                else
                {
                    // just deserialize the keys and values
                    // The value
                    byteBuffer.getInt();
                    btreeInfo.valueSerializer.deserialize( byteBuffer );
                    
                    // the key
                    byteBuffer.getInt();
                    
                    btreeInfo.keySerializer.deserialize( byteBuffer );
                }
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
    private static <K, V> long[] checkBtreeOfBtreesNode( RecordManager recordManager, Map<String, int[]> checkedPages, int nbElems, long revision,
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
     * Check a Btree node.
     */
    private static <K, V> long[] checkBtreeNode( RecordManager recordManager, BtreeInfo<K, V> btreeInfo, Map<String, int[]> checkedPages, int nbElems, long revision, ByteBuffer byteBuffer, PageIO[] pageIos ) throws Exception
    {
        long[] children = new long[nbElems + 1];

        // Read each key and value
        for ( int i = 0; i < nbElems; i++ )
        {
            try
            {
                // The offsets of the child
                long firstOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );
                
                checkOffset( recordManager, firstOffset );
                
                long lastOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );

                checkOffset( recordManager, lastOffset );
                
                children[i] = firstOffset;
                
                // Now, read the key
                // The key lenth
                byteBuffer.getInt();
                
                // The key itself
                btreeInfo.keySerializer.deserialize( byteBuffer );
            }
            catch ( BufferUnderflowException bue )
            {
                throw new InvalidBTreeException( "The BOB leaf byte buffer is too short : " + bue.getMessage() );
            }
        }
        
        // The last child
        // The offsets of the child
        long firstOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );
        
        checkOffset( recordManager, firstOffset );
        
        long lastOffset = LongSerializer.INSTANCE.deserialize( byteBuffer );

        checkOffset( recordManager, lastOffset );
        
        children[nbElems] = firstOffset;
        
        return children;
    }
    
    
    /**
     * Create an array of bits for pages 
     */
    private static int[] createPageArray( RecordManager recordManager ) throws IOException
    {
        long fileSize = recordManager.fileChannel.size();
        int pageSize = recordManager.pageSize;
        long nbPages = ( fileSize - RecordManager.RECORD_MANAGER_HEADER_SIZE ) / pageSize;
        int nbPageBits = ( int ) ( nbPages / 32 );
        
        return new int[nbPageBits + 1];
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
                //throw new InvalidBTreeException( "The page " + offset + " has already been referenced" );
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
    private static void checkFreePages( RecordManager recordManager, Map<String, int[]> checkedPages )
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

                setCheckedPage( recordManager, checkedPages.get( GLOBAL_PAGES_NAME ), currentOffset );
                setCheckedPage( recordManager, checkedPages.get( FREE_PAGES_NAME ), currentOffset );

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
            //throw new InvalidBTreeException( "The page " + offset + " has already been referenced" );
        }

        pageMask |= mask;
        
        checkedPages[ pageNumber / nbBitsPage ] |= pageMask;
    }


    /**
     * Output the pages that has been seen ('1') and those which has not been seen ('0'). The '.' represent non-pages
     * at the end of the file.
     */
    private static void dumpCheckedPages( RecordManager recordManager, Map<String, int[]> checkedPages ) throws IOException
    {
        // First dump the global array
        int[] globalArray = checkedPages.get( GLOBAL_PAGES_NAME );
        String result = dumpPageArray( recordManager, globalArray );
        
        String dump = String.format( "%1$-40s : %2$s", GLOBAL_PAGES_NAME, result );
        System.out.println( dump );
        
        // The free pages array
        int[] freePagesArray = checkedPages.get( FREE_PAGES_NAME );
        result = dumpPageArray( recordManager, freePagesArray );
        
        dump = String.format( "%1$-40s : %2$s", FREE_PAGES_NAME, result );
        System.out.println( dump );
        
        // The B-tree of B-trees pages array
        int[] btreeOfBtreesArray = checkedPages.get( RecordManager.BTREE_OF_BTREES_NAME );
        result = dumpPageArray( recordManager, btreeOfBtreesArray );
        
        dump = String.format( "%1$-40s : %2$s", RecordManager.BTREE_OF_BTREES_NAME, result );
        System.out.println( dump );
        
        // The Copied page B-tree pages array
        int[] copiedPagesArray = checkedPages.get( RecordManager.COPIED_PAGE_BTREE_NAME );
        result = dumpPageArray( recordManager, copiedPagesArray );
        
        dump = String.format( "%1$-40s : %2$s", RecordManager.COPIED_PAGE_BTREE_NAME, result );
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
            result = dumpPageArray( recordManager, btreePagesArray );
            
            dump = String.format( "%1$-40s : %2$s", btreeName, result );
            System.out.println( dump );
        }
    }

    
    /**
     * Process a page array
     */
    private static String dumpPageArray( RecordManager recordManager, int[] checkedPages ) throws IOException
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

                    Map<String, int[]> checkedPages = new HashMap<String, int[]>(2);

                    // The global page array
                    checkedPages.put( GLOBAL_PAGES_NAME, new int[nbPageBits + 1] );

                    // The freePages array
                    checkedPages.put( FREE_PAGES_NAME, new int[nbPageBits + 1] );

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
        String name1 = "corpus";
        String name2 = "multiValues";
        /*
        if ( !rm.getManagedTrees().contains( name1 ) )
        {
            rm.addBTree( name1, StringSerializer.INSTANCE, StringSerializer.INSTANCE, true );
        }
        */
        if ( !rm.getManagedTrees().contains( name2 ) )
        {
            rm.addBTree( name2, StringSerializer.INSTANCE, StringSerializer.INSTANCE, true );
        }

        /*
        // Load some elements in the single value btree
        BTree<String, String> btree1 = rm.getManagedTree( name1 );
        
        for ( int i = 0; i < 10; i++ )
        {
            btree1.insert( Integer.toString( i ), Integer.toString( i ) );
        }
        */

        // Load some elements in the multi value btree
        BTree<String, String> btree2 = rm.getManagedTree( name2 );
        
        for ( int i = 0; i < 1; i++ )
        {
            for ( int j = 0; j < 10; j++)
            {
                btree2.insert( Integer.toString( i ), Integer.toString( j ) );
            }
        }

        rm.close();
        
        MavibotInspector mi = new MavibotInspector( f );
        mi.start();
    }
}


/**
 * A class used to store some information about the Btree 
 */
final class BtreeInfo<K, V>
{
    // The btree name
    /* no qualifier */ String btreeName;
    
    // The key serializer
    /* no qualifier */ElementSerializer<K> keySerializer;
    
    // The value serializer
    /* no qualifier */ElementSerializer<V> valueSerializer;
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "B-tree Info :" );
        sb.append( "\n    name              : " ).append( btreeName );
        sb.append( "\n    key serializer    : " ).append( keySerializer.getClass().getName() );
        sb.append( "\n    value serializer  : " ).append( valueSerializer.getClass().getName() );
        
        return sb.toString();
    }
}
