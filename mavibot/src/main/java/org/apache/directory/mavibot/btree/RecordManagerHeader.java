package org.apache.directory.mavibot.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class store the recordManager header, ie the data associated with the current revision.
 * Here is a description of its content : 
 * <pre>
 * +---------------------+
 * | PageSize            | 4 bytes : The size of a physical page (default to 4096)
 * +---------------------+
 * | NbTree              | 4 bytes : The number of managed B-trees (at least 1)
 * +---------------------+
 * | idCounter           | 8 bytes : The page ID counter (an incremental counter)
 * +---------------------+
 * | Revision            | 8 bytes : The current revision
 * +---------------------+
 * | FirstFree           | 8 bytes : The offset of the first free page
 * +---------------------+
 * | current BoB offset  | 8 bytes : The offset of the current B-tree of B-trees
 * +---------------------+
 * | current CP offset   | 8 bytes : The offset of the current CopiedPages B-tree
 * +---------------------+
 * </pre>
 * 
 * We additionally keep a track of the number of read transaction using an instance.
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RecordManagerHeader
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( RecordManagerHeader.class );

    /** The LoggerFactory used for Pages operations logging */
    protected static final Logger LOG_PAGES = LoggerFactory.getLogger( "org.apache.directory.mavibot.LOG_PAGES" );

    /** The current revision *
    /* no qualifier */long revision;
    
    /** The RecordManager underlying page size. */
    /* no qualifier */int pageSize = BTreeConstants.DEFAULT_PAGE_SIZE;
    
    /** The number of managed B-trees */
    /* no qualifier */int nbBtree;

    /** The first and last free page */
    /* no qualifier */long firstFreePage;
    
    /** A map of the current managed B-trees */
    /* no qualifier */Map<String, BTree> btreeMap = new HashMap<>(); 

    /** The b-tree of b-trees, where we store user's b-trees. */
    /* no qualifier */BTree<NameRevision, Long> btreeOfBtrees;

    /** The b-tree of copied pages, where we store the page that have been modified. */
    /* no qualifier */BTree<RevisionName, long[]> copiedPagesBtree;

    /** The current B-tree of B-trees header offset */
    /* no qualifier */long currentBtreeOfBtreesOffset;

    /** The offset on the current copied pages B-tree */
    /* no qualifier */long currentCopiedPagesBtreeOffset = BTreeConstants.NO_PAGE;
    
    /** The page ID incremental counter */
    /* no qualifier */long idCounter = 0;
    
    /** The offset of the end of the file */
    /* no qualifier */long lastOffset = BTreeConstants.NO_PAGE;
    
    /** The transaction counter */
    /* no qualifier */ AtomicInteger txnCounter = new AtomicInteger( 0 );
    
    
    /** The lock used to protect the recordManagerHeader while accessing it */
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    
    
    /**
     * Copy the current data structure and returns it.
     * 
     * @return A copy of the current data structure
     */
    public RecordManagerHeader copy()
    {
        lock.readLock().lock();
        
        try
        {
            RecordManagerHeader copy = new RecordManagerHeader();
            copy.revision = revision;
            copy.nbBtree = nbBtree;
            copy.currentBtreeOfBtreesOffset = currentBtreeOfBtreesOffset;
            copy.currentCopiedPagesBtreeOffset = currentCopiedPagesBtreeOffset;
            copy.firstFreePage = firstFreePage;
            copy.btreeOfBtrees = btreeOfBtrees;
            copy.copiedPagesBtree = copiedPagesBtree;
            copy.lastOffset = lastOffset;
            copy.idCounter = idCounter;
            copy.pageSize = pageSize;
            
            // Copy the map
            Map<String, BTree> newBTreeMap = new HashMap<>( btreeMap.size() );
            
            for ( Map.Entry<String, BTree> entry : btreeMap.entrySet() )
            {
                BTree btree = entry.getValue();
                newBTreeMap.put( entry.getKey(), btree.copy() );
            }
            
            copy.btreeMap = newBTreeMap;
            
            return copy;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    
    /**
     * Update the current revision
     * 
     * @param update The new revision
     */
    public void update( RecordManagerHeader update )
    {
        lock.writeLock().lock();
        
        try
        {
            revision = update.revision;
            nbBtree = update.nbBtree;
            currentBtreeOfBtreesOffset = update.currentBtreeOfBtreesOffset;
            currentCopiedPagesBtreeOffset = update.currentCopiedPagesBtreeOffset;
            firstFreePage = update.firstFreePage;
            lastOffset = update.lastOffset;
            idCounter = update.idCounter;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    
    
    /**
     * {@inheritDoc}
     * @return
     */
    /* No Qualifier */ long getRevision()
    {
        return revision;
    }
    
    
    /* No qualifier */ BTree<NameRevision, Long> getBtreeOfBtrees()
    {
        return btreeOfBtrees;
    }
    
    
    /* No qualifier */ BTree<RevisionName, long[]> getCopiedPagesBtree()
    {
        return copiedPagesBtree;
    }
    
    
    /* No qualifier */ <K, V> BTree<K, V> getBTree( String name )
    {
        return btreeMap.get( name );
    }
    
    
    /* No qualifier */ <K, V> void addBTree( BTree<K, V> btree )
    {
        btreeMap.put( btree.getName(), btree );
    }
    
    
    /* No qualifier */ boolean containsBTree( String name )
    {
        return btreeMap.get( name ) != null;
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
     * | current CP offset   | 8 bytes : The offset of the current CopiedPages B-tree
     * +---------------------+
     * | ID                  | 8 bytes : The page ID
     * +---------------------+
     * </pre>
     */
    /* No qualifier */ ByteBuffer serialize( RecordManager recordManager )
    {
        byte[] recordManagerHeaderBytes = recordManager.getRecordManagerHeaderBytes();
        
        // The page size
        int position = recordManager.writeData( recordManagerHeaderBytes, 0, pageSize );

        // The number of managed b-tree
        position = recordManager.writeData( recordManagerHeaderBytes, position, nbBtree );

        // The current revision
        position = recordManager.writeData( recordManagerHeaderBytes, position, revision );

        // The first free page
        position = recordManager.writeData( recordManagerHeaderBytes, position, firstFreePage );

        // The offset of the current B-tree of B-trees
        position = recordManager.writeData( recordManagerHeaderBytes, position, currentBtreeOfBtreesOffset );

        // The offset of the current B-tree of B-trees
        position = recordManager.writeData( recordManagerHeaderBytes, position, currentCopiedPagesBtreeOffset );

        // The ID counter
        recordManager.writeData( recordManagerHeaderBytes, position, idCounter );

        // Write the RecordManager header on disk
        recordManager.getRecordManagerHeaderBuffer().put( recordManagerHeaderBytes );
        recordManager.getRecordManagerHeaderBuffer().flip();

        LOG.debug( "Update RM header" );

        if ( LOG_PAGES.isDebugEnabled() )
        {
            StringBuilder sb = new StringBuilder();

            sb.append( "revision            : " );
            sb.append( revision );
            sb.append( "First free page     : 0x" );
            sb.append( String.format( "%16x", firstFreePage ) );
            sb.append( "\n" );
            sb.append( "Current BOB header  : 0x" );
            sb.append( String.format( "%16x", currentBtreeOfBtreesOffset ) );
            sb.append( "\n" );
            sb.append( "Current CPB header  : 0x" );
            sb.append( String.format( "%16x", currentCopiedPagesBtreeOffset ) );
            sb.append( "\n" );

            if ( firstFreePage != BTreeConstants.NO_PAGE )
            {
                long freePage = firstFreePage;
                sb.append( "free pages list : " );

                boolean isFirst = true;

                while ( freePage != BTreeConstants.NO_PAGE )
                {
                    if ( isFirst )
                    {
                        isFirst = false;
                    }
                    else
                    {
                        sb.append( " -> " );
                    }

                    sb.append( "0x" ).append( String.format( "%16x", freePage ) );

                    try
                    {
                        PageIO[] freePageIO = recordManager.readPageIOs( this.pageSize, freePage, 8 );

                        freePage = freePageIO[0].getNextPage();
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                }
            }
            
            sb.append( "Nb transactions :" ).append( txnCounter.get() );
            sb.append( '\n' );

            LOG_PAGES.debug( "Update RM Header : \n{}", sb.toString() );
        }

        return recordManager.getRecordManagerHeaderBuffer();
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "RecordManagerHeader :\n" );
        sb.append( "    ID counter :           " ).append( idCounter ).append( '\n' );
        sb.append( "    revision :            " ).append( revision ).append( '\n' );
        sb.append( "    nbTrees :             " ).append( nbBtree ).append( '\n' );
        sb.append( "    pageSize :            " ).append( pageSize ).append( '\n' );
        sb.append( "    BOB current offset :  " ).append( String.format( "%16x", currentBtreeOfBtreesOffset ) ).append( '\n' );
        sb.append( "    CPB current offset :  " ).append( String.format( "%16x", currentCopiedPagesBtreeOffset ) ).append( '\n' );
        sb.append( "    last offset :         " ).append( String.format( "%16x", lastOffset ) ).append( '\n' );
        sb.append( "    Nb transactions :     " ).append( txnCounter.get() ).append( '\n' );

        if ( btreeMap.isEmpty() )
        {
            sb.append( "    No managed B-trees\n" );
        }
        else
        {
            sb.append( "    Managed B-trees :\n" );
            sb.append( "        {" );
            boolean isFirst = true;
            
            for ( String name : btreeMap.keySet() )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append(  ", " );
                }
                
                sb.append( name );
            }

            sb.append( "}\n" );
        }
        
        return sb.toString();
    }
}
