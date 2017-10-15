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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A data structure used to manage a write transaction. We keep a track of all the modified 
 * {@link WALObject}s, and all the copied {@link WALObject}s, that will be flushed at
 * the end of the transaction, if it is successful.
 * 
 *  We also keep a track of the last position in the file, as we may reclaim some
 *  new pages when the FreePage list is empty.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class WriteTransaction extends AbstractTransaction
{
    /** The List containing all the user modified pages, in the order they have been created */
    private Map<Long, WALObject<?, ?>> newPages = new LinkedHashMap<>();
    
    /** The map containing all the copied pages, using their offset as a key */
    private Map<Long, WALObject<?, ?>> copiedPageMap = new HashMap<>();
    
    /**
     * Creates a new WriteTransaction instance
     * 
     * @param recordManager The associated {@link RecordManager}
     */
    WriteTransaction( RecordManager recordManager )
    {
        super( recordManager );
        
        
        // Get a copy of the RMH
        recordManagerHeader = recordManager.getRecordManagerHeaderCopy();
        
        //System.out.println( "---> Write transaction started, " + this );
        
        // We have to increment the revision
        recordManagerHeader.revision++;
    }


    /**
     * {@inheritDoc}
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
        if ( aborted || isClosed() )
        {
            // We have had an exception, or the txn has been closed : rollback the transaction
            newPages.clear();
            copiedPageMap.clear();
            super.close();
        }
        else
        {
            commit();
        }
    }
    
    
    private <K, V> void updateRefs( Node<K, V> node )
    {
        for ( int i = 0; i < node.pageNbElems + 1; i++ )
        {
            if ( node.children[i] < 0L )
            {
                // This is a Page ID, replace it with the page offset
                WALObject child = newPages.get( -node.children[i] );
                node.children[i] = child.getOffset();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() throws IOException
    {
        if ( !isClosed() )
        {
            // First, find the modified users B-trees, and flush the user's pages 
            Set<BTreeInfo<?, ?>> btreeInfos = new HashSet<>();
            
            //System.out.println( "-----User BTree----" );
            for ( WALObject<?, ?> walObject : newPages.values() )
            {
                //System.out.println( "WALObject" + walObject );
                BTreeInfo<?, ?> btreeInfo = walObject.getBtreeInfo();
                
                // Flush the page
                if ( walObject instanceof Node )
                {
                    updateRefs( ( Node ) walObject );
                }
                
                walObject.serialize( this );
                recordManager.flushPages( recordManagerHeader, walObject.getPageIOs() );
                
                // and update the B-tree list if needed
                if ( walObject.isBTreeUser() )
                {
                    btreeInfos.add( btreeInfo );

                    // Also update the recordManagerHeader B-tree map
                    if ( walObject instanceof BTreeHeader )
                    {
                        BTree btree = recordManagerHeader.btreeMap.get( btreeInfo.getName() );
                        BTree newBtree = btree.copy();
                        newBtree.setBtreeHeader( ( BTreeHeader ) walObject );
                        recordManagerHeader.btreeMap.put( btreeInfo.getName(), newBtree );
                    }
                }
            }
            
            // We can clean the user's list
            newPages.clear();

            // Update the BOB, if we aren't already processing the BOB
            for ( BTreeInfo<?, ?> btreeInfo : btreeInfos )
            {
                recordManager.insertInBtreeOfBtrees( this, recordManagerHeader.btreeMap.get( btreeInfo.getName() ) );
            }
            
            // Flush the newly updated pages 
            //System.out.println( "-----BOB----" );
            flushNewPages();

            // BOB done, clear the list
            newPages.clear();
            
            // Add the copied pages in the CPB
            recordManager.insertInCopiedPagesBtree( this );
            
            // Last not least, Flush the CPB pages
            //System.out.println( "-----CPB BTree----" );
            flushNewPages();
            
            long newBtreeOfBtreesOffet = ((BTree<NameRevision, Long>)recordManagerHeader.btreeOfBtrees).getBtreeHeader().offset;
            long newCopiedPagesBtreeOffset = ((BTree<RevisionName, long[]>)recordManagerHeader.copiedPagesBtree).getBtreeHeader().offset;

            // And update the RecordManagerHeader
            recordManager.updateRecordManagerHeader( recordManagerHeader, newBtreeOfBtreesOffet, newCopiedPagesBtreeOffset );
            recordManager.writeRecordManagerHeader( recordManagerHeader );
            
            // Finally, close the transaction
            //newAdminPages.clear();
            newPages.clear();
            copiedPageMap.clear();
            super.close();
        }
    }
    
    
    private void flushNewPages() throws IOException
    {
        for ( WALObject<?, ?> walObject : newPages.values() )
        {
            //System.out.println( "WALObject" + walObject );
            if ( walObject instanceof Node )
            {
                updateRefs( ( Node ) walObject );
            }

            walObject.serialize( this );
            recordManager.flushPages( recordManagerHeader, walObject.getPageIOs() );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void abort() throws IOException
    {
        // We just have to empty the maps
        newPages.clear();
        copiedPageMap.clear();
        super.close();
    }


    /**
     * Updates the map of new BTreeHeaders
     * 
     * @param <K> The b-tree key type
     * @param <V> The b-tree value type
     * @param btreeHeader The new BtreeHeader
     */
    <K, V> void updateNewBTreeHeaders( BTreeHeader<K, V> btreeHeader )
    {
    }
    
    
    /**
     * @return The offset of the first free page
     */
    /* No qualifier */ long getFirstFreePage()
    {
        return recordManagerHeader.firstFreePage;
    }
    
    
    /**
     * @return The offset of the end of the file
     */
    /* No qualifier */ long getLastOffset()
    {
        return recordManagerHeader.lastOffset;
    }
    
    
    /**
     * Change the last offset in the file
     * 
     * @param The new offset of the end of the file
     */
    /* No qualifier */ void setLastOffset( long lastOffset )
    {
        recordManagerHeader.lastOffset = lastOffset;
    }
    
    
    /**
     * Add a page in the list of modified {@link WALObject}s. If there is an existing 
     * {@link WALObject}, it will be replaced with the new content. Note that a {@link WALObject} 
     * is associated to one to many {@link PageIO}s.
     *  
     * @param walObject The {@link WALObject} to store
     */
    /* No qualifier */void addWALObject( WALObject walObject )
    {
        // Only add the page if it's not already there
        if ( walObject != null )
        {
            WALObject oldPage = newPages.put( walObject.getId(), walObject );
            recordManager.putPage( walObject );
        }
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> Page<K, V> getPage( BTreeInfo<K, V> btreeInfo, long offset ) throws IOException
    {
        if ( offset >= 0 )
        {
            return ( Page<K, V> ) recordManager.getPage( btreeInfo, recordManagerHeader.pageSize, offset );
        }
        else
        {
            return ( Page<K, V> )newPages.get( -offset );
        }
    }

    
    /**
     * Add a page in the list of modified {@link WALObject}s. If there is an existing 
     * {@link WALObject}, it will be replaced with the new content. Note that a {@link WALObject} 
     * is associated to one to many {@link PageIO}s.
     *  
     * @param walObject The {@link WALObject} to store
     */
    /* No qualifier */WALObject removeWALObject( long id )
    {
        return newPages.remove( id );
    }
    
    
    /**
     * Add a WALObject in the list of copied {@link WALObject}s. If there is an existing 
     * {@link WALObject}, it will be replaced with the new content. Note that a {@link WALObject} 
     * is associated to one to many {@link PageIO}s.
     *  
     * @param walObject The {@link WALObject} to store
     */
    /* No qualifier */<K, V> void addCopiedWALObject( WALObject<K, V> walObject )
    {
        if ( ( walObject != null ) && ( walObject.getOffset() != BTreeConstants.NO_PAGE ) && !copiedPageMap.containsKey( walObject.getId() ) )
        {
            copiedPageMap.put( walObject.getId(), walObject );
        }
    }
    
    
    /**
     * Search for a {@link WALObject}. It may be present in the cache, or if it's not, we will
     * have to fetch it from the disk.
     * 
     * @param offset The position of this {@link WALObject} on disk
     * @return The found {@link WALObject}
     */
    public WALObject<?, ?> getWALObject( long id )
    {
        return newPages.get( id );
    }
    
    
    /* No qualifier */ Map<Long, WALObject<?, ?>> getCopiedPageMap()
    {
        return copiedPageMap;
    }
    
    
    /**
     * Update the WAL
     * 
     * @param revision The previous revision
     * @param oldPage The previous page
     * @param newPage The new page
     * @throws IOException If we had an error storing the 
     **/
    /* No qualifier */ <K, V> void updateWAL( long previousRevision, WALObject<K, V> oldPage, WALObject<K, V> newPage )
    {
        addWALObject( newPage );

        if ( ( previousRevision != getRevision() ) && newPage.isBTreeUser() )
        {
            // Now, move the new child into the copied pages if needed
            addCopiedWALObject( oldPage );
        }
    }
    
    
    /**
     * Creates a new {@link Leaf}, with a valid offset.
     * 
     * @param btreeInfo The {@link BTreeInfo} reference
     * @return A new {@link Leaf} instance
     */
    /* No qualifier */ <K, V> Leaf<K, V> newLeaf( BTreeInfo<K, V> btreeInfo, int nbElems )
    {
        Leaf<K, V> leaf = new Leaf<>( btreeInfo, getRevision(), nbElems );
        leaf.initId( recordManagerHeader );
        
        return leaf;
    }
    
    
    /**
     * Creates a new {@link Leaf}, with a valid offset.
     * 
     * @param btreeInfo The {@link BTreeInfo} reference
     * @return A new {@link Leaf} instance
     */
    /* No qualifier */ <K, V> Leaf<K, V> newLeaf( BTreeInfo<K, V> btreeInfo, long id, int nbElems )
    {
        Leaf<K, V> leaf = new Leaf<>( btreeInfo, getRevision(), nbElems );
        leaf.setId( id );
        
        return leaf;
    }
    
    
    /**
     * Creates a new {@link Node}, with a valid offset.
     * 
     * @param btreeInfo The {@link BTreeInfo} reference
     * @return A new {@link Leaf} instance
     */
    /* No qualifier */ <K, V> Node<K, V> newNode( BTreeInfo<K, V> btreeInfo, int nbElems )
    {
        Node<K, V> node = new Node<>( btreeInfo, getRevision(), nbElems );
        node.initId( recordManagerHeader );

        return node;
    }
    
    
    /**
     * Creates a new {@link Node}, with a valid offset.
     * 
     * @param btreeInfo The {@link BTreeInfo} reference
     * @return A new {@link Leaf} instance
     */
    /* No qualifier */ <K, V> Node<K, V> newNode( BTreeInfo<K, V> btreeInfo, long id, int nbElems )
    {
        Node<K, V> node = new Node<>( btreeInfo, getRevision(), nbElems );
        node.setId( id );
        
        return node;
    }
    
    
    /**
     * Creates a new Node which will contain only one key, with references to
     * a left and right page. This is a specific constructor used by the btree
     * when the root was full when we added a new value.
     *
     * @param btreeInfo the {@link BTreeInfo} reference
     * @param key The new key
     * @param leftPage The left page offset
     * @param rightPage The right page offset
     */
    /* No qualifier */ <K, V> Node<K, V> newNode( BTreeInfo<K, V> btreeInfo, K key, long leftPage, long rightPage )
    {
        Node<K, V> node = new Node<>( btreeInfo, getRevision(), key, leftPage, rightPage );
        node.initId( recordManagerHeader );
        
        return node;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "W:" );
        sb.append( super.toString() );
        sb.append( ", last offset" );
        sb.append( String.format( " %x", getLastOffset() ) ).append( '\n' );
        
        if ( newPages.size() > 0 )
        {
            sb.append( "    pageList :[\n        " );
            boolean isFirst = true;
            
            for ( WALObject walObject : newPages.values() )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ",\n        " );
                }

                sb.append( walObject.prettyPrint() );
            }
            
            sb.append( '\n' );
            
        }
        else
        {
            sb.append( "    UserPageList empty\n" );
        }
        
        if ( copiedPageMap.size() > 0 )
        {
            sb.append( "    CopiedPagesMap :[\n        " );
            boolean isFirst = true;
            
            for ( Entry<Long, WALObject<?, ?>> entry : copiedPageMap.entrySet() )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ",\n        " );
                }
                
                sb.append( String.format( "%x", entry.getKey() ) );
                sb.append(  ':' );
                sb.append( '<' ).append( entry.getValue().getName() ).append( '>' );
                
                sb.append( entry.getValue().getClass().getSimpleName() );
            }
            
            sb.append( "]\n" );
        }
        else
        {
            sb.append( "    CopiedPagesMap empty\n" );
        }

        return sb.toString();
    }
}
