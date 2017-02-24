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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    //private List<WALObject<?, ?>> newUserPages = new ArrayList<WALObject<?, ?>>();
    private List<WALObject<?, ?>> newPages = new ArrayList<WALObject<?, ?>>();
    
    /** The List containing all the BOB/CPB modified pages, in the order they have been created */
    //private List<WALObject<?, ?>> newAdminPages = new ArrayList<WALObject<?, ?>>();

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
        if ( !isClosed() )
        {
            commit();
        }
        else
        {
            //newUserPages.clear();
            //newAdminPages.clear();
            newPages.clear();
            copiedPageMap.clear();
            super.close();
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
            Set<BTree<?, ?>> btrees = new HashSet<>();
            
            //for ( WALObject<?, ?> walObject : newUserPages )
            for ( WALObject<?, ?> walObject : newPages )
            {
                BTree<?, ?> btree = walObject.getBtree();
                
                // Flush the page
                walObject.serialize( this );
                recordManager.flushPages( recordManagerHeader, walObject.getPageIOs() );
                
                // and update the B-tree list if needed
                btrees.add( btree );
            }
            
            // We can clean the user's list
            //newUserPages.clear();
            newPages.clear();

            // Update the BOB, if we aren't already processing the BOB
            for ( BTree<?, ?> btree : btrees )
            {
                recordManager.insertInBtreeOfBtrees( this, btree );
            }
            
            //for ( WALObject<?, ?> walObject : newAdminPages )
            for ( WALObject<?, ?> walObject : newPages )
            {
                // Flush the page
                walObject.serialize( this );
                recordManager.flushPages( recordManagerHeader, walObject.getPageIOs() );
            }

            // BOB done, clear the list
            //newAdminPages.clear();
            newPages.clear();
            
            // Add the copied pages in the CPB
            recordManager.insertInCopiedPagesBtree( this );
            
            // Last not least, Flush the CPB pages
            //for ( WALObject<?, ?> walObject : newAdminPages )
            for ( WALObject<?, ?> walObject : newPages )
            {
                walObject.serialize( this );
                recordManager.flushPages( recordManagerHeader, walObject.getPageIOs() );
            }
            
            long newBtreeOfBtreesOffet = ((BTreeImpl<NameRevision, Long>)recordManagerHeader.btreeOfBtrees).getBtreeHeader().offset;
            long newCopiedPagesBtreeOffset = ((BTreeImpl<RevisionName, long[]>)recordManagerHeader.copiedPagesBtree).getBtreeHeader().offset;

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


    /**
     * {@inheritDoc}
     */
    @Override
    public void abort() throws IOException
    {
        // We just have to empty the maps
        //newUserPages.clear();
        //newAdminPages.clear();
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
    /* No qualifier */void addWALObject( WALObject walObject ) throws IOException
    {
        if ( walObject != null )
        {
            newPages.add( walObject );
            /*
            if ( walObject.getBtree().getType() == BTreeTypeEnum.PERSISTED )
            {
                newUserPages.add( walObject );
            }
            else
            {
                newAdminPages.add( walObject );
            }
            */
        }
    }
    
    
    /**
     * Add a page in the list of modified {@link WALObject}s. If there is an existing 
     * {@link WALObject}, it will be replaced with the new content. Note that a {@link WALObject} 
     * is associated to one to many {@link PageIO}s.
     *  
     * @param walObject The {@link WALObject} to store
     */
    /* No qualifier */WALObject removeWALObject( long id ) throws IOException
    {
        for ( int i = 0; i < newPages.size(); i++ )
        {
            if ( newPages.get( i ).getId() == id )
            {
                return newPages.remove( i );
            }
        }
        /*
        for ( int i = 0; i < newUserPages.size(); i++ )
        {
            if ( newUserPages.get( i ).getId() == id )
            {
                return newUserPages.remove( i );
            }
        }

        for ( int i = 0; i < newAdminPages.size(); i++ )
        {
            if ( newAdminPages.get( i ).getId() == id )
            {
                return newAdminPages.remove( i );
            }
        }
        */
        
        return null;
    }
    
    
    /**
     * Add a WALObject in the list of copied {@link WALObject}s. If there is an existing 
     * {@link WALObject}, it will be replaced with the new content. Note that a {@link WALObject} 
     * is associated to one to many {@link PageIO}s.
     *  
     * @param walObject The {@link WALObject} to store
     */
    /* No qualifier */void addCopiedWALObject( WALObject walObject )
    {
        if ( walObject != null )
        {
            copiedPageMap.put( Long.valueOf( walObject.getOffset() ), walObject );
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
        for ( WALObject walObject : newPages )
        {
            if ( walObject.getId() == id )
            {
                return walObject;
            }
        }
        /*
        for ( WALObject walObject : newUserPages )
        {
            if ( walObject.getId() == id )
            {
                return walObject;
            }
        }

        for ( WALObject walObject : newAdminPages )
        {
            if ( walObject.getId() == id )
            {
                return walObject;
            }
        }
        */
        
        return null;
    }
    
    
    /* No qualifier */ Map<Long, WALObject<?, ?>> getCopiedPageMap()
    {
        return copiedPageMap;
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
            
            for ( WALObject walObject : newPages )
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
            
            /*
            for ( Entry<Long, WALObject<?, ?>> entry : pageMap.entrySet() )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ", " );
                }
                
                sb.append( String.format( "%x", entry.getKey() ) );
                sb.append(  ':' );
                sb.append( '<' );
                sb.append( entry.getValue().getName() );
                sb.append( ':' );
                
                if ( entry.getValue().getRevision() == RecordManager.NO_PAGE )
                {
                    sb.append( "info" );
                }
                else
                {
                    sb.append( entry.getValue().getRevision() );
                }
                sb.append( '>' );
                sb.append( entry.getValue().getClass().getSimpleName() );
            }
            
            sb.append( "]\n" );
            */
        }
        else
        {
            sb.append( "    UserPageList empty\n" );
        }
        
        /*
        if ( newAdminPages.size() > 0 )
        {
            sb.append( "    AdminPageList :[\n        " );
            boolean isFirst = true;
            
            for ( WALObject walObject : newAdminPages )
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
            sb.append( "    AdminPageList empty\n" );
        }
        */

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
