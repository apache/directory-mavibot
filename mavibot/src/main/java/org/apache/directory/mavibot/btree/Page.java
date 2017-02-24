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

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;


/**
 * A MVCC Page interface. A Page can be either a Leaf (containing keys and values) or a Node
 * (containing keys and references to child pages).<br/>
 * A Page can be stored on disk. If so, we store the serialized value of this Page into
 * one or more {@link PageIO} (they will be linked)
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/interface Page<K, V> extends WALObject<K, V>
{
    /**
     * @return The number of elements present in this page
     */
    int getNbPageElems();


    /**
     * Inserts the given key and value into this page. We first find the place were to
     * inject the <K,V> into the tree, by recursively browsing the pages :<br/>
     * <ul>
     * <li>If the index is below zero, the key is present in the Page : we modify the
     * value and return</li>
     * <li>If the page is a node, we have to go down to the right child page</li>
     * <li>If the page is a leaf, we insert the new <K,V> element into the page, and if
     * the Page is full, we split it and propagate the new pivot up into the tree</li>
     * </ul>
     * <p>
     *
     * @param transaction The on going transaction
     * @param key Inserted key
     * @param value Inserted value
     * @return An instance of a {@InsertResult}
     * @throws IOException If we have an error while trying to access the page
     */
    InsertResult<K, V> insert( WriteTransaction transaction, K key, V value ) throws IOException;


    /**
     * Deletes the key and its associated value from this page. We first find
     * the place were to remove the <K,V> into the tree, by recursively browsing the pages.
     * If the key is present, it will be deleted and we will remove the key from the tree.
     *
     * @param revision The new revision for the modified pages
     * @param key The key to delete
     * @param parent The parent page
     * @param parentPos The position of the current page in it's parent
     * @return Either a modified Page if the key has been removed from the page, or a NotPresentResult.
     * @throws IOException If we have an error while trying to access the page
     */
    DeleteResult<K, V> delete( WriteTransaction transaction, K key ) throws IOException;


    /**
     * Gets the value associated with the given key, if any. If we don't have
     * one, this method will throw a KeyNotFoundException.<br/>
     * Note that we may get back null if a null value has been associated
     * with the key.
     *
     * @param key The key we are looking for
     * @return The associated value, which can be null
     * @throws KeyNotFoundException If no entry with the given key can be found
     * @throws IOException If we have an error while trying to access the page
     */
    V get( K key ) throws KeyNotFoundException, IOException;


    /**
     * Checks if the page contains the given key with the given value.
     *
     * @param key The key we are looking for
     * @param value The value associated with the given key
     * @return true if the key and value are associated with each other, false otherwise
     */
    boolean contains( K key, V value ) throws IOException;

    
    /**
     * Copies the current page and all its keys, with a new revision.
     *
     * @param revision The new revision
     * @return The copied page
     */
    Page<K, V> copy( WriteTransaction transaction );


    /**
     * Browses the tree, looking for the given key, and creates a Cursor on top
     * of the found result.
     *
     * @param key The key we are looking for.
     * @param transaction The started transaction for this operation
     * @param stack The stack of parents we go through to get to this page
     * @return A Cursor to browse the next elements
     * @throws IOException If we have an error while trying to access the page
     */
    TupleCursor<K, V> browse( Transaction transaction, K key, ParentPos<K, V>[] stack, int depth )
        throws IOException;


    /**
     * Browses the whole tree, and creates a Cursor on top of it.
     *
     * @param transaction The started transaction for this operation
     * @param stack The stack of parents we go through to get to this page
     * @return A Cursor to browse the next elements
     * @throws IOException If we have an error while trying to access the page
     */
    TupleCursor<K, V> browse( Transaction transaction, ParentPos<K, V>[] stack, int depth )
        throws IOException;


    /**
     * Browses the keys of whole tree, and creates a Cursor on top of it.
     *
     * @param transaction The started transaction for this operation
     * @param stack The stack of parents we go through to get to this page
     * @return A Cursor to browse the keys
     * @throws IOException If we have an error while trying to access the page
     */
    KeyCursor<K> browseKeys( Transaction transaction, ParentPos<K, K>[] stack, int depth )
        throws IOException;

    
    /**
     * @return the revision
     */
    @Override
    long getRevision();


    /**
     * Returns the key at a given position
     *
     * @param pos The position of the key we want to retrieve
     * @return The key found at the given position
     */
    K getKey( int pos );


    /**
     * Finds the leftmost key in this page. If the page is a node, it will go
     * down in the leftmost children to recursively find the leftmost key.
     *
     * @return The leftmost key in the tree
     */
    K getLeftMostKey();


    /**
     * Finds the rightmost key in this page. If the page is a node, it will go
     * down in the rightmost children to recursively find the rightmost key.
     *
     * @return The rightmost key in the tree
     */
    K getRightMostKey();


    /**
     * Finds the leftmost element in this page. If the page is a node, it will go
     * down in the leftmost children to recursively find the leftmost element.
     *
     * @return The leftmost element in the tree
     * @throws IOException If we have an error while trying to access the page
     */
    Tuple<K, V> findLeftMost() throws IOException;


    /**
     * Finds the rightmost element in this page. If the page is a node, it will go
     * down in the rightmost children to recursively find the rightmost element.
     *
     * @return The rightmost element in the tree
     * @throws IOException If we have an error while trying to access the page
     */
    Tuple<K, V> findRightMost() throws IOException;


    /**
     * Pretty-prints the tree with tabs
     * @param tabs The tabs to add in front of each node
     * @return A pretty-print dump of the tree
     */
    String dumpPage( String tabs );


    /**
     * Find the position of the given key in the page. If we have found the key,
     * we will return its position as a negative value.
     * <p/>
     * Assuming that the array is zero-indexed, the returned value will be : <br/>
     *   position = - ( position + 1)
     * <br/>
     * So for the following table of keys : <br/>
     * <pre>
     * +---+---+---+---+
     * | b | d | f | h |
     * +---+---+---+---+
     *   0   1   2   3
     * </pre>
     * looking for 'b' will return -1 (-(0+1)) and looking for 'f' will return -3 (-(2+1)).<br/>
     * Computing the real position is just a matter to get -(position++).
     * <p/>
     * If we don't find the key in the table, we will return the position of the key
     * immediately above the key we are looking for. <br/>
     * For instance, looking for :
     * <ul>
     * <li>'a' will return 0</li>
     * <li>'b' will return -1</li>
     * <li>'c' will return 1</li>
     * <li>'d' will return -2</li>
     * <li>'e' will return 2</li>
     * <li>'f' will return -3</li>
     * <li>'g' will return 3</li>
     * <li>'h' will return -4</li>
     * <li>'i' will return 4</li>
     * </ul>
     *
     * @param key The key to find
     * @return The position in the page.
     */
    int findPos( K key );


    /**
     * Checks if the given key exists.
     *
     * @param key The key we are looking at
     * @return true if the key is present, false otherwise
     * @throws IOException If we have an error while trying to access the page
     */
    boolean hasKey( K key ) throws IOException;


    /**
     * Tells if the page is a leaf or not
     * @return true if the page is a leaf
     */
    boolean isLeaf();


    /**
     * Tells if the page is a node or not
     * @return true if the page is a node
     */
    boolean isNode();
}
