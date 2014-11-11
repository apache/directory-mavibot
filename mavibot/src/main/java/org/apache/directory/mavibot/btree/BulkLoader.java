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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.directory.mavibot.btree.comparator.IntComparator;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;


/**
 * A class used to bulk load a BTree. It will allow the load of N elements in 
 * a given BTree without to have to inject one by one, saving a lot of time.
 * The second advantage is that the btree will be dense (the leaves will be
 * complete, except the last one).
 * This class can also be used to compact a BTree.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BulkLoader<K, V>
{
    static enum LevelEnum
    {
        LEAF,
        NODE
    }

    /**
     * A private class to store informations on a level. We have to keep :
     * <ul>
     * <li>The number of elements to store in this level</li>
     * <li>A flag that tells if it's a leaf or a node level</li>
     * <li>The number of pages necessary to store all the elements in a level</li>
     * <li>The number of elements we can store in a complete page (we may have one or two 
     * incomplete pages at the end)</li>
     * <li>A flag that tells if we have some incomplete page at the end</li>
     * </ul>
     * TODO LevelInfo.
     *
     * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
     */
    /*private*/class LevelInfo
    {
        /** The level number */
        private int levelNumber;

        /** Nb of elements for this level */
        /*private*/int nbElems;

        /** The number of pages in this level */
        /*private*/int nbPages;

        /** Nb of elements before we reach an incomplete page */
        /*private*/int nbElemsLimit;

        /** A flag that tells if the level contains nodes or leaves */
        private boolean isNode;

        /** The current page which contains the data until we move it to the resulting BTree */
        /*private*/Page<K, V> currentPage;

        /** The current position in the currentPage */
        private int currentPos;

        /** The number of already added elements for this level */
        private int nbAddedElems;


        /** @see Object#toString() */
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if ( isNode )
            {
                sb.append( "NodeLevel[" );
                sb.append( levelNumber );
                sb.append( "] :" );
            }
            else
            {
                sb.append( "LeafLevel:" );
            }

            sb.append( "\n    nbElems           = " ).append( nbElems );
            sb.append( "\n    nbPages           = " ).append( nbPages );
            sb.append( "\n    nbElemsLimit      = " ).append( nbElemsLimit );
            sb.append( "\n    nbAddedElems      = " ).append( nbAddedElems );
            sb.append( "\n    currentPos        = " ).append( currentPos );
            sb.append( "\n    currentPage" );
            sb.append( "\n        nbKeys : " ).append( currentPage.getNbElems() );

            return sb.toString();
        }
    }


    /**
     * Process the data, and creates files to store them sorted if necessary, or store them
     * TODO readElements.
     *
     * @param btree
     * @param iterator
     * @param sortedFiles
     * @param tuples
     * @param chunkSize
     * @return
     * @throws IOException
     */
    private int readElements( BTree<K, V> btree, Iterator<Tuple<K, V>> iterator, List<File> sortedFiles,
        List<Tuple<K, V>> tuples, int chunkSize ) throws IOException
    {
        int nbRead = 0;
        int nbIteration = 0;
        int nbElems = 0;
        boolean inMemory = true;

        while ( true )
        {
            nbIteration++;
            tuples.clear();

            // Read up to chukSize elements
            while ( iterator.hasNext() && ( nbRead < chunkSize ) )
            {
                Tuple<K, V> tuple = iterator.next();
                tuples.add( tuple );
                nbRead++;
            }

            if ( nbRead < chunkSize )
            {
                if ( nbIteration == 1 )
                {
                    // We have read all the data in one round trip, let's get out, no need
                    // to store the data on disk
                    ;
                }
                else
                {
                    // Flush the sorted data on disk and exit
                    inMemory = false;

                    sortedFiles.add( flushToDisk( nbIteration, tuples, btree ) );
                }

                // Update the number of read elements
                nbElems += nbRead;

                break;
            }
            else
            {
                if ( !iterator.hasNext() )
                {
                    // special case : we have exactly chunkSize elements in the incoming data
                    if ( nbIteration > 1 )
                    {
                        // Flush the sorted data on disk and exit
                        inMemory = false;
                        sortedFiles.add( flushToDisk( nbIteration, tuples, btree ) );
                    }

                    // We have read all the data in one round trip, let's get out, no need
                    // to store the data on disk

                    // Update the number of read elements
                    nbElems += nbRead;

                    break;
                }

                // We have read chunkSize elements, we have to sort them on disk
                nbElems += nbRead;
                nbRead = 0;
                sortedFiles.add( flushToDisk( nbIteration, tuples, btree ) );
            }
        }

        if ( !inMemory )
        {
            tuples.clear();
        }

        return nbElems;
    }


    /**
     * Bulk Load data into a persisted BTree
     *
     * @param btree The persisted BTree in which we want to load the data
     * @param iterator The iterator over the data to bulkload
     * @param chunkSize The number of elements we may store in memory at each iteration
     * @throws IOException If there is a problem while processing the data
     */
    public BTree<K, V> load( PersistedBTree<K, V> btree, Iterator<Tuple<K, V>> iterator, int chunkSize )
        throws IOException
    {
        if ( btree == null )
        {
            throw new RuntimeException( "Invalid BTree : it's null" );
        }

        if ( iterator == null )
        {
            // Nothing to do...
            return null;
        }

        // Iterate through the elements by chunk
        boolean inMemory = true;

        // The list of files we will use to store the sorted chunks
        List<File> sortedFiles = new ArrayList<File>();

        // An array of chukSize tuple max
        List<Tuple<K, V>> tuples = new ArrayList<Tuple<K, V>>( chunkSize );

        // Now, start to read all the tuples to sort them. We may use intermediate files
        // for that purpose if we hit the threshold.
        int nbElems = readElements( btree, iterator, sortedFiles, tuples, chunkSize );

        // If the tuple list is empty, we have to process the load based on files, not in memory
        if ( nbElems > 0 )
        {
            inMemory = tuples.size() > 0;
        }

        // Now that we have processed all the data, we can start storing them in the btree
        Iterator<Tuple<K, Set<V>>> dataIterator = null;
        FileInputStream[] streams = null;

        if ( inMemory )
        {
            // Here, we have all the data in memory, no need to merge files
            // We will build a simple iterator over the data
            dataIterator = createTupleIterator( btree, tuples );
        }
        else
        {
            // We first have to build an iterator over the files
            int nbFiles = sortedFiles.size();
            streams = new FileInputStream[nbFiles];

            for ( int i = 0; i < nbFiles; i++ )
            {
                streams[i] = new FileInputStream( sortedFiles.get( i ) );
            }

            dataIterator = createIterator( btree, streams );
        }

        // Ok, we have an iterator over sorted elements, we can now load them in the 
        // target btree.
        BTree<K, V> resultBTree = bulkLoad( btree, dataIterator, nbElems );

        // Now, close the FileInputStream, and delete them if we have some
        if ( !inMemory )
        {
            int nbFiles = sortedFiles.size();

            for ( int i = 0; i < nbFiles; i++ )
            {
                streams[i].close();
                sortedFiles.get( i ).delete();
            }
        }

        return resultBTree;
    }


    /**
     * Creates a node leaf LevelInfo based on the number of elements in the lower level. We can store
     * up to PageSize + 1 references to pages in a node.
     */
    /* no qualifier*/LevelInfo computeLevel( BTree<K, V> btree, int nbElems, LevelEnum levelType )
    {
        int pageSize = btree.getPageSize();
        int incrementNode = 0;

        if ( levelType == LevelEnum.NODE )
        {
            incrementNode = 1;
        }

        LevelInfo level = new LevelInfo();
        level.isNode = ( levelType == LevelEnum.NODE );
        level.nbElems = nbElems;
        level.nbPages = nbElems / ( pageSize + incrementNode );
        level.levelNumber = 0;
        level.nbAddedElems = 0;
        level.currentPos = 0;

        // Create the first level page
        if ( nbElems <= pageSize + incrementNode )
        {
            if ( nbElems % ( pageSize + incrementNode ) != 0 )
            {
                level.nbPages = 1;
            }

            level.nbElemsLimit = nbElems;

            if ( level.isNode )
            {
                level.currentPage = BTreeFactory.createNode( btree, 0L, nbElems - 1 );
            }
            else
            {
                level.currentPage = BTreeFactory.createLeaf( btree, 0L, nbElems );
            }
        }
        else
        {
            int remaining = nbElems % ( pageSize + incrementNode );

            if ( remaining == 0 )
            {
                level.nbElemsLimit = nbElems;

                if ( level.isNode )
                {
                    level.currentPage = BTreeFactory.createNode( btree, 0L, pageSize );
                }
                else
                {
                    level.currentPage = BTreeFactory.createLeaf( btree, 0L, pageSize );
                }
            }
            else
            {
                level.nbPages++;

                if ( remaining < ( pageSize / 2 ) + incrementNode )
                {
                    level.nbElemsLimit = nbElems - remaining - ( pageSize + incrementNode );

                    if ( level.nbElemsLimit > 0 )
                    {
                        if ( level.isNode )
                        {
                            level.currentPage = BTreeFactory.createNode( btree, 0L, pageSize );
                        }
                        else
                        {
                            level.currentPage = BTreeFactory.createLeaf( btree, 0L, pageSize );
                        }
                    }
                    else
                    {
                        if ( level.isNode )
                        {
                            level.currentPage = BTreeFactory.createNode( btree, 0L, ( pageSize / 2 ) + remaining - 1 );
                        }
                        else
                        {
                            level.currentPage = BTreeFactory.createLeaf( btree, 0L, ( pageSize / 2 ) + remaining );
                        }
                    }
                }
                else
                {
                    level.nbElemsLimit = nbElems - remaining;

                    if ( level.isNode )
                    {
                        level.currentPage = BTreeFactory.createNode( btree, 0L, pageSize );
                    }
                    else
                    {
                        level.currentPage = BTreeFactory.createLeaf( btree, 0L, pageSize );
                    }
                }
            }
        }

        return level;
    }


    /**
     * Compute the number of pages necessary to store all the elements per level. The resulting list is
     * reversed ( ie the leaves are on the left, the root page on the right.
     */
    /* No Qualifier */List<LevelInfo> computeLevels( BTree<K, V> btree, int nbElems )
    {
        List<LevelInfo> levelList = new ArrayList<LevelInfo>();

        // Compute the leaves info
        LevelInfo leafLevel = computeLevel( btree, nbElems, LevelEnum.LEAF );

        levelList.add( leafLevel );
        int nbPages = leafLevel.nbPages;
        int levelNumber = 1;

        while ( nbPages > 1 )
        {
            // Compute the Nodes info
            LevelInfo nodeLevel = computeLevel( btree, nbPages, LevelEnum.NODE );
            nodeLevel.levelNumber = levelNumber++;
            levelList.add( nodeLevel );
            nbPages = nodeLevel.nbPages;
        }

        return levelList;
    }


    /**
     * Inject a tuple into a leaf
     */
    private void injectInLeaf( BTree<K, V> btree, Tuple<K, Set<V>> tuple, LevelInfo leafLevel )
    {
        PersistedLeaf<K, V> leaf = ( PersistedLeaf<K, V> ) leafLevel.currentPage;

        KeyHolder<K> keyHolder = new PersistedKeyHolder<K>( btree.getKeySerializer(), tuple.getKey() );
        ValueHolder<V> valueHolder = new PersistedValueHolder<V>( btree, ( V[] ) tuple.getValue().toArray() );
        leaf.setKey( leafLevel.currentPos, keyHolder );
        leaf.setValue( leafLevel.currentPos, valueHolder );

        leafLevel.currentPos++;
    }


    private int computeNbElemsLeaf( BTree<K, V> btree, LevelInfo levelInfo )
    {
        int pageSize = btree.getPageSize();
        int remaining = levelInfo.nbElems - levelInfo.nbAddedElems;

        if ( remaining < pageSize )
        {
            return remaining;
        }
        else if ( remaining == pageSize )
        {
            return pageSize;
        }
        else if ( remaining > levelInfo.nbElems - levelInfo.nbElemsLimit )
        {
            return pageSize;
        }
        else
        {
            return remaining - pageSize / 2;
        }
    }


    /**
     * Compute the number of nodes necessary to store all the elements.
     */
    /* No qualifier */int computeNbElemsNode( BTree<K, V> btree, LevelInfo levelInfo )
    {
        int pageSize = btree.getPageSize();
        int remaining = levelInfo.nbElems - levelInfo.nbAddedElems;

        if ( remaining < pageSize + 1 )
        {
            return remaining;
        }
        else if ( remaining == pageSize + 1 )
        {
            return pageSize + 1;
        }
        else if ( remaining > levelInfo.nbElems - levelInfo.nbElemsLimit )
        {
            return pageSize + 1;
        }
        else
        {
            return remaining - pageSize / 2;
        }
    }


    /**
     * Inject a page reference into the root page.
     */
    private void injectInRoot( BTree<K, V> btree, Page<K, V> page, PageHolder<K, V> pageHolder, LevelInfo level )
        throws IOException
    {
        PersistedNode<K, V> node = ( PersistedNode<K, V> ) level.currentPage;
        if ( ( level.currentPos == 0 ) && ( node.getPage( 0 ) == null ) )

        {
            node.setPageHolder( 0, pageHolder );
            level.nbAddedElems++;
        }
        else
        {
            // Inject the pageHolder and the page leftmost key
            node.setPageHolder( level.currentPos + 1, pageHolder );
            KeyHolder<K> keyHolder = new PersistedKeyHolder<K>( btree.getKeySerializer(), page.getLeftMostKey() );
            node.setKey( level.currentPos, keyHolder );
            level.currentPos++;
            level.nbAddedElems++;

            // Check that we haven't added the last element. If so,
            // we have to write the page on disk and update the btree
            if ( level.nbAddedElems == level.nbElems )
            {
                PageHolder<K, V> rootHolder = ( ( PersistedBTree<K, V> ) btree ).getRecordManager().writePage(
                    btree, node, 0L );
                ( ( PersistedBTree<K, V> ) btree ).setRootPage( rootHolder.getValue() );
            }
        }

        return;
    }


    /**
     * Inject a page reference into a Node. This method will recurse if needed.
     */
    private void injectInNode( BTree<K, V> btree, Page<K, V> page, List<LevelInfo> levels, int levelIndex )
        throws IOException
    {
        int pageSize = btree.getPageSize();
        LevelInfo level = levels.get( levelIndex );
        PersistedNode<K, V> node = ( PersistedNode<K, V> ) level.currentPage;

        // We first have to write the page on disk
        PageHolder<K, V> pageHolder = ( ( PersistedBTree<K, V> ) btree ).getRecordManager().writePage( btree, page, 0L );

        // First deal with a node that has less than PageSize elements at this level.
        // It will become the root node.
        if ( level.nbElems <= pageSize + 1 )
        {
            injectInRoot( btree, page, pageHolder, level );

            return;
        }

        // Now, we have some parent node. We process the 3 different use case :
        // o Full node before the limit
        // o Node over the limit but with at least N/2 elements
        // o Node over the limit but with elements spread into 2 nodes
        if ( level.nbAddedElems < level.nbElemsLimit )
        {
            // Ok, we haven't yet reached the incomplete pages (if any).
            // Let's add the page reference into the node
            // There is one special case : when we are adding the very first page 
            // reference into a node. In this case, we don't store the key
            if ( ( level.currentPos == 0 ) && ( node.getKey( 0 ) == null ) )
            {
                node.setPageHolder( 0, pageHolder );
            }
            else
            {
                // Inject the pageHolder and the page leftmost key
                node.setPageHolder( level.currentPos, pageHolder );
                KeyHolder<K> keyHolder = new PersistedKeyHolder<K>( btree.getKeySerializer(), page.getLeftMostKey() );
                node.setKey( level.currentPos - 1, keyHolder );
            }

            // Now, increment this level nb of added elements
            level.currentPos++;
            level.nbAddedElems++;

            // Check that we haven't added the last element. If so,
            // we have to write the page on disk and update the parent's node
            if ( level.nbAddedElems == level.nbElems )
            {
                //PageHolder<K, V> rootHolder = ( ( PersistedBTree<K, V> ) btree ).getRecordManager().writePage(
                //    btree, node, 0L );
                //( ( PersistedBTree<K, V> ) btree ).setRootPage( rootHolder.getValue() );
                injectInNode( btree, node, levels, levelIndex + 1 );

                return;
            }
            else
            {
                // Check that we haven't completed the current node, and that this is not the root node.
                if ( ( level.currentPos == pageSize + 1 ) && ( level.levelNumber < levels.size() - 1 ) )
                {
                    // yes. We have to write the node on disk, update its parent
                    // and create a new current node
                    injectInNode( btree, node, levels, levelIndex + 1 );

                    // The page is full, we have to create a new one, with a size depending on the remaining elements
                    if ( level.nbAddedElems < level.nbElemsLimit )
                    {
                        // We haven't reached the limit, create a new full node
                        level.currentPage = BTreeFactory.createNode( btree, 0L, pageSize );
                    }
                    else if ( level.nbElems - level.nbAddedElems <= pageSize )
                    {
                        level.currentPage = BTreeFactory.createNode( btree, 0L, level.nbElems - level.nbAddedElems - 1 );
                    }
                    else
                    {
                        level.currentPage = BTreeFactory.createNode( btree, 0L, ( level.nbElems - 1 )
                            - ( level.nbAddedElems + 1 ) - pageSize / 2 );
                    }

                    level.currentPos = 0;
                }
            }

            return;
        }
        else
        {
            // We are dealing with the last page or the last two pages 
            // We can have either one single pages which can contain up to pageSize-1 elements
            // or with two pages, the first one containing ( nbElems - limit ) - pageSize/2 elements
            // and the second one will contain pageSize/2 elements. 
            if ( level.nbElems - level.nbElemsLimit > pageSize )
            {
                // As the remaining elements are above a page size, they will be spread across
                // two pages. We have two cases here, depending on the page we are filling
                if ( level.nbElems - level.nbAddedElems <= pageSize / 2 + 1 )
                {
                    // As we have less than PageSize/2 elements to write, we are on the second page
                    if ( ( level.currentPos == 0 ) && ( node.getKey( 0 ) == null ) )
                    {
                        node.setPageHolder( 0, pageHolder );
                    }
                    else
                    {
                        // Inject the pageHolder and the page leftmost key
                        node.setPageHolder( level.currentPos, pageHolder );
                        KeyHolder<K> keyHolder = new PersistedKeyHolder<K>( btree.getKeySerializer(),
                            page.getLeftMostKey() );
                        node.setKey( level.currentPos - 1, keyHolder );
                    }

                    // Now, increment this level nb of added elements
                    level.currentPos++;
                    level.nbAddedElems++;

                    // Check if we are done with the page
                    if ( level.nbAddedElems == level.nbElems )
                    {
                        // Yes, we have to update the parent
                        injectInNode( btree, node, levels, levelIndex + 1 );
                    }
                }
                else
                {
                    // This is the first page 
                    if ( ( level.currentPos == 0 ) && ( node.getKey( 0 ) == null ) )
                    {
                        // First element of the page
                        node.setPageHolder( 0, pageHolder );
                    }
                    else
                    {
                        // Any other following elements
                        // Inject the pageHolder and the page leftmost key
                        node.setPageHolder( level.currentPos, pageHolder );
                        KeyHolder<K> keyHolder = new PersistedKeyHolder<K>( btree.getKeySerializer(),
                            page.getLeftMostKey() );
                        node.setKey( level.currentPos - 1, keyHolder );
                    }

                    // Now, increment this level nb of added elements
                    level.currentPos++;
                    level.nbAddedElems++;

                    // Check if we are done with the page
                    if ( level.currentPos == node.getNbElems() + 1 )
                    {
                        // Yes, we have to update the parent
                        injectInNode( btree, node, levels, levelIndex + 1 );

                        // An create a new one
                        level.currentPage = BTreeFactory.createNode( btree, 0L, pageSize / 2 );
                        level.currentPos = 0;
                    }
                }
            }
            else
            {
                // Two cases : we don't have anything else to write, or this is a single page
                if ( level.nbAddedElems == level.nbElems )
                {
                    // We are done with the page
                    injectInNode( btree, node, levels, levelIndex + 1 );
                }
                else
                {
                    // We have some more elements to add in  the page
                    // This is the first page 
                    if ( ( level.currentPos == 0 ) && ( node.getKey( 0 ) == null ) )
                    {
                        // First element of the page
                        node.setPageHolder( 0, pageHolder );
                    }
                    else
                    {
                        // Any other following elements
                        // Inject the pageHolder and the page leftmost key
                        node.setPageHolder( level.currentPos, pageHolder );
                        KeyHolder<K> keyHolder = new PersistedKeyHolder<K>( btree.getKeySerializer(),
                            page.getLeftMostKey() );
                        node.setKey( level.currentPos - 1, keyHolder );
                    }

                    // Now, increment this level nb of added elements
                    level.currentPos++;
                    level.nbAddedElems++;

                    // Check if we are done with the page
                    if ( level.currentPos == node.getNbElems() + 1 )
                    {
                        // Yes, we have to update the parent
                        injectInNode( btree, node, levels, levelIndex + 1 );

                        // An create a new one
                        level.currentPage = BTreeFactory.createNode( btree, 0L, pageSize / 2 );
                        level.currentPos = 0;
                    }
                }

                return;
            }
        }
    }


    private BTree<K, V> bulkLoadSinglePage( BTree<K, V> btree, Iterator<Tuple<K, Set<V>>> dataIterator, int nbElems )
        throws IOException
    {
        // Create a new page
        PersistedLeaf<K, V> rootPage = ( PersistedLeaf<K, V> ) BTreeFactory.createLeaf( btree, 0L, nbElems );

        // We first have to inject data into the page
        int pos = 0;

        while ( dataIterator.hasNext() )
        {
            Tuple<K, Set<V>> tuple = dataIterator.next();

            // Store the current element in the rootPage
            KeyHolder<K> keyHolder = new PersistedKeyHolder<K>( btree.getKeySerializer(), tuple.getKey() );
            ValueHolder<V> valueHolder = new PersistedValueHolder<V>( btree, ( V[] ) tuple.getValue().toArray() );
            rootPage.setKey( pos, keyHolder );
            rootPage.setValue( pos, valueHolder );
            pos++;
        }

        // Now write the page on disk
        ( ( PersistedBTree<K, V> ) btree ).getRecordManager().writePage( btree, rootPage, 0L );

        // Update the btree with the rootPage and the nb of added elements
        ( ( PersistedBTree<K, V> ) btree ).getBtreeHeader().setRootPage( rootPage );
        ( ( PersistedBTree<K, V> ) btree ).getBtreeHeader().setNbElems( nbElems );

        return btree;
    }


    /**
     * Construct the target BTree from the sorted data. We will use the nb of elements
     * to determinate the structure of the BTree, as it must be balanced
     */
    private BTree<K, V> bulkLoad( BTree<K, V> btree, Iterator<Tuple<K, Set<V>>> dataIterator, int nbElems )
        throws IOException
    {
        int pageSize = btree.getPageSize();

        // Special case : we can store all the element sin a single page
        if ( nbElems <= pageSize )
        {
            return bulkLoadSinglePage( btree, dataIterator, nbElems );
        }

        // Ok, we will need more than one page to store the elements, which
        // means we also will need more than one level.
        // First, compute the needed number of levels.
        List<LevelInfo> levels = computeLevels( btree, nbElems );

        // Now, let's fill the levels
        LevelInfo leafLevel = levels.get( 0 );

        while ( dataIterator.hasNext() )
        {
            // let's fill page up to the point all the complete pages have been filled
            if ( leafLevel.nbAddedElems < leafLevel.nbElemsLimit )
            {
                // grab a tuple
                Tuple<K, Set<V>> tuple = dataIterator.next();

                injectInLeaf( btree, tuple, leafLevel );
                leafLevel.nbAddedElems++;

                // The page is completed, update the parent's node and create a new current page
                if ( leafLevel.currentPos == pageSize )
                {
                    injectInNode( btree, leafLevel.currentPage, levels, 1 );

                    // The page is full, we have to create a new one
                    leafLevel.currentPage = BTreeFactory.createLeaf( btree, 0L, computeNbElemsLeaf( btree, leafLevel ) );
                    leafLevel.currentPos = 0;
                }
            }
            else
            {
                // We have to deal with uncompleted pages now (if we have any)
                if ( leafLevel.nbAddedElems == nbElems )
                {
                    // First use case : we have injected all the elements in the btree : get out
                    break;
                }

                if ( nbElems - leafLevel.nbElemsLimit > pageSize )
                {
                    // Second use case : the number of elements after the limit does not
                    // fit in a page, that means we have to split it into
                    // two pages

                    // First page will contain nbElems - leafLevel.nbElemsLimit - PageSize/2 elements
                    int nbToAdd = nbElems - leafLevel.nbElemsLimit - pageSize / 2;

                    while ( nbToAdd > 0 )
                    {
                        // grab a tuple
                        Tuple<K, Set<V>> tuple = dataIterator.next();

                        injectInLeaf( btree, tuple, leafLevel );
                        leafLevel.nbAddedElems++;
                        nbToAdd--;
                    }

                    // Now inject the page into the node
                    injectInNode( btree, leafLevel.currentPage, levels, 1 );

                    // Create a new page for the remaining elements
                    nbToAdd = pageSize / 2;
                    leafLevel.currentPage = BTreeFactory.createLeaf( btree, 0L, nbToAdd );
                    leafLevel.currentPos = 0;

                    while ( nbToAdd > 0 )
                    {
                        // grab a tuple
                        Tuple<K, Set<V>> tuple = dataIterator.next();

                        injectInLeaf( btree, tuple, leafLevel );
                        leafLevel.nbAddedElems++;
                        nbToAdd--;
                    }

                    // And update the parent node
                    injectInNode( btree, leafLevel.currentPage, levels, 1 );

                    // We are done
                    break;
                }
                else
                {
                    // Third use case : we can push all the elements in the last page.
                    // Let's do it
                    int nbToAdd = nbElems - leafLevel.nbElemsLimit;

                    while ( nbToAdd > 0 )
                    {
                        // grab a tuple
                        Tuple<K, Set<V>> tuple = dataIterator.next();

                        injectInLeaf( btree, tuple, leafLevel );
                        leafLevel.nbAddedElems++;
                        nbToAdd--;
                    }

                    // Now inject the page into the node
                    injectInNode( btree, leafLevel.currentPage, levels, 1 );

                    // and we are done
                    break;
                }
            }
        }

        return btree;
    }


    /**
     * Flush a list of tuples to disk after having sorted them. In the process, we may have to gather the values
     * for the tuples having the same keys.
     * @throws IOException 
     */
    private File flushToDisk( int fileNb, List<Tuple<K, V>> tuples, BTree<K, V> btree ) throws IOException
    {
        // Sort the tuples. 
        Tuple<K, Set<V>>[] sortedTuples = sort( btree, tuples );

        File file = File.createTempFile( "sorted", Integer.toString( fileNb ) );
        file.deleteOnExit();
        FileOutputStream fos = new FileOutputStream( file );

        // Flush the tuples on disk
        for ( Tuple<K, Set<V>> tuple : sortedTuples )
        {
            // Serialize the key
            byte[] bytesKey = btree.getKeySerializer().serialize( tuple.key );
            fos.write( IntSerializer.serialize( bytesKey.length ) );
            fos.write( bytesKey );

            // Serialize the number of values
            int nbValues = tuple.getValue().size();
            fos.write( IntSerializer.serialize( nbValues ) );

            // Serialize the values
            for ( V value : tuple.getValue() )
            {
                byte[] bytesValue = btree.getValueSerializer().serialize( value );

                // Serialize the value
                fos.write( IntSerializer.serialize( bytesValue.length ) );
                fos.write( bytesValue );
            }
        }

        fos.flush();
        fos.close();

        return file;
    }


    /**
     * Sort a list of tuples, eliminating the duplicate keys and storing the values in a set when we 
     * have a duplicate key
     */
    private Tuple<K, Set<V>>[] sort( BTree<K, V> btree, List<Tuple<K, V>> tuples )
    {
        Comparator<Tuple<K, Set<V>>> tupleComparator = new TupleComparator( btree.getKeyComparator(), btree
            .getValueComparator() );

        // Sort the list
        Tuple<K, V>[] tuplesArray = ( Tuple<K, V>[] ) tuples.toArray( new Tuple[]
            {} );

        // First, eliminate the equals keys. We use a map for that
        Map<K, Set<V>> mapTuples = new HashMap<K, Set<V>>();

        for ( Tuple<K, V> tuple : tuplesArray )
        {
            // Is the key present in the map ?
            Set<V> foundSet = mapTuples.get( tuple.key );

            if ( foundSet != null )
            {
                // We already have had such a key, add the value to the existing key
                foundSet.add( tuple.value );
            }
            else
            {
                // No such key present in the map : create a new set to store the values,
                // and add it in the map associated with the new key
                Set<V> set = new TreeSet<V>();
                set.add( tuple.value );
                mapTuples.put( tuple.key, set );
            }
        }

        // Now, sort the map, by extracting all the key/values from the map
        int size = mapTuples.size();
        Tuple<K, Set<V>>[] sortedTuples = new Tuple[size];
        int pos = 0;

        // We create an array containing all the elements
        for ( Map.Entry<K, Set<V>> entry : mapTuples.entrySet() )
        {
            sortedTuples[pos] = new Tuple<K, Set<V>>();
            sortedTuples[pos].key = entry.getKey();
            sortedTuples[pos].value = entry.getValue();
            pos++;
        }

        // And we sort the array
        Arrays.sort( sortedTuples, tupleComparator );

        return sortedTuples;
    }


    /**
     * Build an iterator over an array of sorted tuples, in memory
     */
    private Iterator<Tuple<K, Set<V>>> createTupleIterator( BTree<K, V> btree, List<Tuple<K, V>> tuples )
    {
        final Tuple<K, Set<V>>[] sortedTuples = sort( btree, tuples );

        Iterator<Tuple<K, Set<V>>> tupleIterator = new Iterator<Tuple<K, Set<V>>>()
        {
            private int pos = 0;


            @Override
            public Tuple<K, Set<V>> next()
            {
                // Return the current tuple, if any
                if ( pos < sortedTuples.length )
                {
                    Tuple<K, Set<V>> tuple = sortedTuples[pos];
                    pos++;

                    return tuple;
                }

                return null;
            }


            @Override
            public boolean hasNext()
            {
                return pos < sortedTuples.length;
            }


            @Override
            public void remove()
            {
            }
        };

        return tupleIterator;
    }


    private Tuple<K, Set<V>> fetchTuple( BTree<K, V> btree, FileInputStream fis )
    {
        try
        {
            if ( fis.available() == 0 )
            {
                return null;
            }

            Tuple<K, Set<V>> tuple = new Tuple<K, Set<V>>();
            tuple.value = new TreeSet<V>();

            byte[] intBytes = new byte[4];

            // Read the key length
            fis.read( intBytes );
            int keyLength = IntSerializer.deserialize( intBytes );

            // Read the key
            byte[] keyBytes = new byte[keyLength];
            fis.read( keyBytes );
            K key = btree.getKeySerializer().fromBytes( keyBytes );
            tuple.key = key;

            // get the number of values
            fis.read( intBytes );
            int nbValues = IntSerializer.deserialize( intBytes );

            // Serialize the values
            for ( int i = 0; i < nbValues; i++ )
            {
                // Read the value length
                fis.read( intBytes );
                int valueLength = IntSerializer.deserialize( intBytes );

                // Read the value
                byte[] valueBytes = new byte[valueLength];
                fis.read( valueBytes );
                V value = btree.getValueSerializer().fromBytes( valueBytes );
                tuple.value.add( value );
            }

            return tuple;
        }
        catch ( IOException ioe )
        {
            return null;
        }
    }


    /**
     * Build an iterator over an array of sorted tuples, from files on the disk
     * @throws FileNotFoundException 
     */
    private Iterator<Tuple<K, Set<V>>> createIterator( final BTree<K, V> btree, final FileInputStream[] streams )
        throws FileNotFoundException
    {
        // The number of files we have to read from
        final int nbFiles = streams.length;

        // We will read only one element at a time from each file
        final Tuple<K, Set<V>>[] readTuples = new Tuple[nbFiles];
        final TreeSet<Tuple<K, Integer>> candidateSet =
            new TreeSet<Tuple<K, Integer>>(
                new TupleComparator<K, Integer>( btree.getKeyComparator(), IntComparator.INSTANCE ) );

        // Read the tuple from each files
        for ( int i = 0; i < nbFiles; i++ )
        {
            while ( true )
            {
                readTuples[i] = fetchTuple( btree, streams[i] );

                if ( readTuples[i] != null )
                {
                    Tuple<K, Integer> candidate = new Tuple<K, Integer>( readTuples[i].key, i, btree.getKeySerializer()
                        .getComparator() );

                    if ( !candidateSet.contains( candidate ) )
                    {
                        candidateSet.add( candidate );
                        break;
                    }
                }
                else
                {
                    break;
                }
            }
        }

        Iterator<Tuple<K, Set<V>>> tupleIterator = new Iterator<Tuple<K, Set<V>>>()
        {
            @Override
            public Tuple<K, Set<V>> next()
            {
                // Get the first candidate
                Tuple<K, Integer> tupleCandidate = candidateSet.first();

                // Remove it from the set
                candidateSet.remove( tupleCandidate );

                // Get the the next tuple from the stream we just got the tuple from
                Tuple<K, Set<V>> tuple = readTuples[tupleCandidate.value];

                // fetch it from the disk and store it into its reader
                readTuples[tupleCandidate.value] = fetchTuple( btree, streams[tupleCandidate.value] );

                if ( readTuples[tupleCandidate.value] != null )
                {
                    // And store it into the candidate set
                    Tuple<K, Integer> newTuple = new Tuple<K, Integer>( readTuples[tupleCandidate.value].key,
                        tupleCandidate.value );
                    candidateSet.add( newTuple );
                }

                // We can now return the found value
                return tuple;
            }


            @Override
            public boolean hasNext()
            {
                // Check that we have at least one element to read
                return !candidateSet.isEmpty();
            }


            @Override
            public void remove()
            {
            }

        };

        return tupleIterator;
    }


    /**
     * Compact a given persisted BTree, making it dense. All the values will be stored 
     * in newly created pages, each one of them containing as much elements
     * as it's size.
     * </br>
     * The RecordManager will be stopped and restarted, do not use this method
     * on a running BTree.
     *
     * @param recordManager The associated recordManager
     * @param btree The BTree to compact
     */
    public static void compact( RecordManager recordManager, BTree<?, ?> btree )
    {

    }


    /**
     * Compact a given in-memory BTree, making it dense. All the values will be stored 
     * in newly created pages, each one of them containing as much elements
     * as it's size.
     * </br>
     *
     * @param btree The BTree to compact
     * @throws KeyNotFoundException 
     * @throws IOException 
     */
    public static BTree<?, ?> compact( BTree<?, ?> btree ) throws IOException, KeyNotFoundException
    {
        // First, create a new BTree which will contain all the elements
        InMemoryBTreeConfiguration configuration = new InMemoryBTreeConfiguration();
        configuration.setName( btree.getName() );
        configuration.setPageSize( btree.getPageSize() );
        configuration.setKeySerializer( btree.getKeySerializer() );
        configuration.setValueSerializer( btree.getValueSerializer() );
        configuration.setAllowDuplicates( btree.isAllowDuplicates() );
        configuration.setReadTimeOut( btree.getReadTimeOut() );
        configuration.setWriteBufferSize( btree.getWriteBufferSize() );

        File file = ( ( InMemoryBTree ) btree ).getFile();

        if ( file != null )
        {
            configuration.setFilePath( file.getPath() );
        }

        // Create a new Btree Builder
        InMemoryBTreeBuilder btreeBuilder = new InMemoryBTreeBuilder( configuration );

        // Create a cursor over the existing btree
        final TupleCursor cursor = btree.browse();

        // Create an iterator that will iterate the existing btree
        Iterator<Tuple> tupleItr = new Iterator<Tuple>()
        {
            @Override
            public Tuple next()
            {
                try
                {
                    return cursor.next();
                }
                catch ( EndOfFileExceededException e )
                {
                    return null;
                }
                catch ( IOException e )
                {
                    return null;
                }
            }


            @Override
            public boolean hasNext()
            {
                try
                {
                    return cursor.hasNext();
                }
                catch ( EndOfFileExceededException e )
                {
                    return false;
                }
                catch ( IOException e )
                {
                    return false;
                }
            }


            @Override
            public void remove()
            {
            }
        };

        // And finally, compact the btree
        return btreeBuilder.build( tupleItr );
    }
}
