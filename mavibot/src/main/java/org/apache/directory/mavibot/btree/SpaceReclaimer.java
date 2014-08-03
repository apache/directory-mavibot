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


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class used for reclaiming the copied pages.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class SpaceReclaimer
{
    /** the record manager */
    private RecordManager rm;

    private static String COPIED_PAGE_MAP_DATA_FILE = "cpm.db";

    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( SpaceReclaimer.class );

    /**
     * Creates a new instance of SpaceReclaimer.
     *
     * @param rm the record manager
     */
    public SpaceReclaimer( RecordManager rm )
    {
        this.rm = rm;
    }

    
    /**
     * stores the copied page map, if not empty, in a file under the given directory
     * 
     * @param dir the directory where mavibot database file is present
     */
    /* no qualifier */ void storeCopiedPageMap( File dir )
    {
        if ( rm.copiedPageMap.isEmpty() )
        {
            LOG.debug( "Copied page map is empty, nothing to store on disk." );
            return;
        }
        
        File file = new File( dir, COPIED_PAGE_MAP_DATA_FILE );

        try
        {
            LOG.debug( "Storing {} RevisionNames of Copied page map", rm.copiedPageMap.size() );
            
            OutputStream fileOut = new FileOutputStream( file );
            
            ObjectOutputStream objOut = new ObjectOutputStream( fileOut );
            
            objOut.writeObject( rm.copiedPageMap );
            
            objOut.close();
            
            LOG.debug( "Successfully stored copied page map in {}", file.getAbsolutePath() );
        }
        catch( Exception e )
        {
            LOG.warn( "Failed to store the copied page map in {}", file.getAbsolutePath() );
            LOG.warn( "", e );
        }
    }


    /**
     * reads the copied page map from the file named {@link #COPIED_PAGE_MAP_DATA_FILE} if it
     * is present under the given directory
     * 
     * @param dir the directory where mavibot database file is present
     * 
     * @return
     */
    /* no qualifier */ ConcurrentHashMap<RevisionName, long[]> readCopiedPageMap( File dir )
    {
        
        ConcurrentHashMap<RevisionName, long[]> map = new ConcurrentHashMap<RevisionName, long[]>();
        
        File file = new File( dir, COPIED_PAGE_MAP_DATA_FILE );
        
        if ( !file.exists() )
        {
            LOG.debug( "Copied page map store {} doesn't exist, returning empty map", file.getAbsolutePath() );
            return map;
        }

        try
        {
            LOG.debug( "Reading Copied page map data stored in {}", file.getAbsolutePath() );
            
            InputStream fileIn = new FileInputStream( file );
            
            ObjectInputStream objIn = new ObjectInputStream( fileIn );
            
            map = ( ConcurrentHashMap<RevisionName, long[]> ) objIn.readObject();
            
            objIn.close();
            
            LOG.debug( "Successfully read copied page map containing {} RevisionNames", map.size() );
        }
        catch( Exception e )
        {
            LOG.warn( "Failed to read the copied page map from {}", file.getAbsolutePath() );
            LOG.warn( "", e );
        }
        finally
        {
            boolean deleted = file.delete();
            
            // this is dangerous, cause during a subsequent restart the pages
            // will be freed again, but this time they might have been in use
            if( !deleted )
            {
                String warn = "Failed to delete the copied page map store " + file.getAbsolutePath() +
                    " Make sure the approapriate permissions are given to delete this file by mavibot process." ;
                LOG.warn( warn );
                
                throw new RuntimeException( warn );
            }
        }
        
        return map;
    }

    
    /**
     * relcaims the copied pages
     */
    /* no qualifier */ void reclaim()
    {
        //System.out.println( "reclaiming pages" );
        try
        {
            Set<String> managed = rm.getManagedTrees();

            for ( String name : managed )
            {
                PersistedBTree tree = ( PersistedBTree ) rm.getManagedTree( name );

                Set<Long> inUseRevisions = new TreeSet<Long>();

                // the tree might have been removed
                if ( tree != null )
                {
                    Iterator<ReadTransaction> txnItr = tree.getReadTransactions().iterator();
                    while ( txnItr.hasNext() )
                    {
                        inUseRevisions.add( txnItr.next().getRevision() );
                    }
                }

                List<RevisionOffset> copiedRevisions = getRevisions( name );

                for ( RevisionOffset ro : copiedRevisions )
                {
                    long rv = ro.getRevision();
                    if ( inUseRevisions.contains( rv ) )
                    {
                        //System.out.println( "Revision " + rv + " of BTree " + name + " is in use, not reclaiming pages" );
                        break;
                    }

                    long[] offsets = ro.getOffsets();

                    //System.out.println( "Reclaiming " + Arrays.toString( offsets ) + "( " + offsets.length + " ) pages of the revision " + rv + " of BTree " + name );

                    rm.free( offsets );

                    RevisionName key = new RevisionName( rv, name );
                    rm.copiedPageMap.remove( key );
                }
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }


    /**
     * gets a list of all the copied pages of a given B-Tree.
     * 
     * @param name the name of the B-Tree
     * @return list of RevisionOffset
     * @throws Exception
     */
    private List<RevisionOffset> getRevisions( String name ) throws Exception
    {
        long nbElems = rm.copiedPageMap.size();
        //System.out.println( "Total number of entries in CPB " + nbElems );

        if ( nbElems == 0 )
        {
            return Collections.EMPTY_LIST;
        }

        Iterator<Map.Entry<RevisionName, long[]>> cursor = rm.copiedPageMap.entrySet().iterator();

        List<RevisionOffset> lst = new ArrayList<RevisionOffset>();

        while ( cursor.hasNext() )
        {
            Map.Entry<RevisionName, long[]> t = cursor.next();
            RevisionName rn = t.getKey();
            if ( name.equals( rn.getName() ) )
            {
                //System.out.println( t.getValue() );
                lst.add( new RevisionOffset( rn.getRevision(), t.getValue() ) );
            }
        }

        return lst;
    }
}
