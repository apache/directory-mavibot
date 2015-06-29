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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class used for reclaiming the copied pages.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PageReclaimer
{
    /** the record manager */
    private RecordManager rm;

    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( PageReclaimer.class );

    /** a flag to detect the running state */
    private boolean running = false;
    
    /**
     * Creates a new instance of PageReclaimer.
     *
     * @param rm the record manager
     */
    public PageReclaimer( RecordManager rm )
    {
        this.rm = rm;
    }    

    
    /**
     * relcaims the copied pages
     */
    /* no qualifier */ void reclaim()
    {
        //System.out.println( "reclaiming pages" );
        try
        {
            if ( running )
            {
                return;
            }
            
            running = true;
            
            Set<String> managed = rm.getManagedTrees();

            for ( String name : managed )
            {
                PersistedBTree tree = ( PersistedBTree ) rm.getManagedTree( name );

                long latestRev = tree.getRevision();
                
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

                // the revision last removed from copiedPage BTree
                long lastRemovedRev = -1;

                List<Long> freeList = new ArrayList<Long>();
                
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

                    for( long l : offsets )
                    {
                        freeList.add( l );
                    }

                    RevisionName key = new RevisionName( rv, name );
                    
                    //System.out.println( "delete cpb key " + key );
                    rm.copiedPageBtree.delete( key );
                    lastRemovedRev = rv;
                }

                // no new txn is needed for the operations on BoB
                // and also no need to traverse BoB if the tree is a sub-btree
                if ( ( lastRemovedRev != -1 ) && !tree.isAllowDuplicates() )
                {
                    // we SHOULD NOT delete the latest revision from BoB
                    NameRevision nr = new NameRevision( name, latestRev );
                    TupleCursor<NameRevision, Long> cursor = rm.btreeOfBtrees.browseFrom( nr );
                    
                    List<Long> btreeHeaderOffsets = new ArrayList<Long>();
                    
                    while ( cursor.hasPrev() )
                    {
                        Tuple<NameRevision, Long> t = cursor.prev();
                        //System.out.println( "deleting BoB rev " + t.getKey()  + " latest rev " + latestRev );
                        rm.btreeOfBtrees.delete( t.getKey() );
                        btreeHeaderOffsets.add( t.value );
                    }

                    cursor.close();
                    
                    for( Long l : btreeHeaderOffsets )
                    {
                        // the offset may have already been present while
                        // clearing CPB so skip it here, otherwise it will result in OOM
                        // due to the attempt to free and already freed page
                        if(freeList.contains( l ))
                        {
                            //System.out.println( "bob duplicate offset " + l );
                            continue;
                        }

                        freeList.add( l );
                    }
                }
                
                for( Long offset : freeList )
                {
                    PageIO[] pageIos = rm.readPageIOs( offset, -1L );
                    
                    for ( PageIO pageIo : pageIos )
                    {
                        rm.free( pageIo );
                    }
                }

            }

            running = false;
        }
        catch ( Exception e )
        {
        	running = false;
        	rm.rollback();
        	LOG.warn( "Errors while reclaiming", e );
        	throw new RuntimeException( e );
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
        TupleCursor<RevisionName, long[]> cursor = rm.copiedPageBtree.browse();

        List<RevisionOffset> lst = new ArrayList<RevisionOffset>();

        while ( cursor.hasNext() )
        {
            Tuple<RevisionName, long[]> t = cursor.next();
            RevisionName rn = t.getKey();
            if ( name.equals( rn.getName() ) )
            {
                //System.out.println( t.getValue() );
                lst.add( new RevisionOffset( rn.getRevision(), t.getValue() ) );
            }
        }

        cursor.close();
        
        return lst;
    }
}
