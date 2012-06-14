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
package org.apache.mavibot.btree;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The B+Tree MVCC data structure.
 * 
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 *
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class BTree<K, V>
{
    /** Default page size (number of entries per node) */
    public static final int DEFAULT_PAGE_SIZE = 16;
    
    /** A field used to generate new revisions in a thread safe way */
    private AtomicLong revision = new AtomicLong(0);

    /** A field used to generate new recordId in a thread safe way */
    private transient AtomicLong pageRecordIdGenerator;

    /** Comparator used to index entries. */
    private Comparator<K> comparator;

    /** The current rootPage */
    protected Page<K, V> rootPage;
    
    /** A map containing all the existing revisions */
    private Map<Long, Page<K, V>> roots = new ConcurrentHashMap<Long, Page<K, V>>();

    /** Number of entries in each Page. */
    protected int pageSize;


    /**
     * Creates a new BTree with a default page size and a comparator.
     * 
     * @param comparator The comparator to use
     */
    public BTree( Comparator<K> comparator ) throws IOException
    {
        this( comparator, DEFAULT_PAGE_SIZE );
    }
    
    
    /**
     * Creates a new BTree with a specific page size and a comparator.
     * 
     * @param comparator The comparator to use
     * @param pageSize The number of elements we can store in a page
     */
    public BTree( Comparator<K> comparator, int pageSize ) throws IOException
    {
        if ( comparator == null )
        {
            throw new IllegalArgumentException( "Comparator should not be null" );
        }

        this.comparator = comparator;
        setPageSize( pageSize );
        
        // Create the map contaning all the revisions
        roots = new ConcurrentHashMap<Long, Page<K,V>>();
        
        // Initialize the PageId counter
        pageRecordIdGenerator = new AtomicLong(0);
        
        // Initialize the revision counter
        revision = new AtomicLong(0);
        
        // Create the first root page, with revision 0L. It wil be empty
        // and increment the revision at the same time
        rootPage = new Leaf<K, V>();
        roots.put( revision.getAndIncrement(), rootPage );
    }
    
    
    /**
     * Gets the number which is a power of 2 immediately above the given positive number.
     */
    private int getPowerOf2( int size )
    {
        int newSize = size--;
        newSize |= newSize >> 1;
        newSize |= newSize >> 2;
        newSize |= newSize >> 4;
        newSize |= newSize >> 8;
        newSize |= newSize >> 16;
        newSize++;
        
        return newSize;
    }
    
    
    /**
     * Set the maximum number of elements we can store in a page. This must be a
     * number greater than 1, and a power of 2. The default page size is 16.
     * <br/>
     * If the provided size is below 2, we will default to DEFAULT_PAGE_SIZE.<br/>
     * If the provided size is not a power of 2, we will select the closest power of 2
     * higher than the given number<br/>
     * 
     * @param pageSize The requested page size
     */
    public void setPageSize( int pageSize )
    {
        this.pageSize = pageSize;

        if ( pageSize <= 2 )
        {
            this.pageSize = DEFAULT_PAGE_SIZE;
        }
        
        this.pageSize = getPowerOf2( pageSize );
    }


    /**
     * @return the pageSize
     */
    public int getPageSize()
    {
        return pageSize;
    }

    
    /**
     * Generates a new RecordId. It's only used by the Page instances.
     * 
     * @return a new incremental recordId
     */
    /** No qualifier */ long generateRecordId()
    {
        return pageRecordIdGenerator.getAndIncrement();
    }
    
    
    /**
     * Generates a new revision number. It's only used by the Page instances.
     * 
     * @return a new incremental revision number
     */
    /** No qualifier */ long generateRevision()
    {
        return revision.getAndIncrement();
    }


    /**
     * Insert an entry in the BTree.
     * <p>
     * We will replace the value if the provided key already exists in the
     * btree.
     *
     * @param key Inserted key
     * @param value Inserted value
     * @return Existing value, if any.
     */
    public V insert( K key, V value ) throws IOException
    {
        long revision = generateRevision();
        
        return insert( key, value, revision );
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
     * @return Existing value, if any.
     */
    private V insert( K key, V value, long revision ) throws IOException
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Key must not be null" );
        }
        
        if ( value == null )
        {
            throw new IllegalArgumentException( "Value must not be null" );
        }
        
        // Commented atm, we will have to play around the idea of transactions later
        //acquireWriteLock();

        try
        {
            // If the key exists, the existing value will be replaced. We store it
            // to return it to the caller.
            V modifiedValue = null;
            
            // Try to insert the new value in the tree at the right place,
            // starting from the root page
            InsertResult<K, V> result = rootPage.insert( revision, key, value );
            
            if ( result instanceof ModifyResult )
            {
                ModifyResult<K, V> modifyResult = ((ModifyResult)result);
                
                // The root has just been modified, we haven't split it
                // Get it and make it the current root page
                rootPage = modifyResult.getModifiedPage();
                
                modifiedValue = modifyResult.getModifiedValue();
                
                // Save it into the roots.
                roots.put( revision, rootPage );
            }
            else
            {
                // We have split the old root, create a new one containing
                // only the pivotal we got back
                SplitResult<K, V> splitResult = ((SplitResult)result);

                K pivot = splitResult.getPivot();
                Page<K, V> leftPage = splitResult.getLeftPage();
                Page<K, V> rightPage = splitResult.getRightPage();
                
                // Create the new rootPage
                rootPage = new Node<K, V>( this, revision, pivot, leftPage, rightPage );
            }

            
            // Save the rootPage into the roots with the new revision.
            roots.put( revision, rootPage );
            
            // Return the value we have found if it was modified
            return modifiedValue;
        }
        finally
        {
            // See above
            //releaseWriteLock()
        }
    }


    /**
     * @return the comparator
     */
    public Comparator<K> getComparator()
    {
        return comparator;
    }


    /**
     * @param comparator the comparator to set
     */
    public void setComparator( Comparator<K> comparator )
    {
        this.comparator = comparator;
    }

    
    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "BTree" );
        sb.append( ", pageSize:" ).append( pageSize );
        
        if ( rootPage != null )
        {
            sb.append( ", nbEntries:" ).append( rootPage.getNbElems() );
        }
        else
        {
            sb.append( ", nbEntries:" ).append( 0 );
        }
        
        sb.append( ", comparator:" );
        
        if ( comparator == null )
        {
            sb.append( "null" );
        }
        else
        {
            sb.append( comparator.getClass().getSimpleName() );
        }
        
        sb.append( ") : " );
        sb.append( rootPage );

        return sb.toString();
    }
}
