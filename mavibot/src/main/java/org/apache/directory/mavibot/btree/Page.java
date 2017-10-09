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
import java.io.Serializable;

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
/* No qualifier*/interface Page<K, V> extends WALObject<K, V>, Serializable
{
    /**
     * @return The number of elements present in this page
     */
    int getPageNbElems();


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
     * @param transaction The started transaction for this operation
     * @param key The key to delete
     * @param parent The parent page
     * @param parentPos The position of the current page in it's parent
     * @return Either a modified Page if the key has been removed from the page, or a {@link NotPresentResult}.
     * @throws IOException If we have an error while trying to access the page
     */
    DeleteResult<K, V> delete( WriteTransaction transaction, K key, Node<K, V> parent, int parentPos ) throws IOException;


    /**
     * Gets the value associated with the given key, if any. If we don't have
     * one, this method will throw a KeyNotFoundException.<br/>
     * Note that we may get back null if a null value has been associated
     * with the key.
     *
     * @param transaction The started transaction for this operation
     * @param key The key we are looking for
     * @return The associated value, which can be null
     * @throws KeyNotFoundException If no entry with the given key can be found
     * @throws IOException If we have an error while trying to access the page
     */
    V get( Transaction transaction, K key ) throws KeyNotFoundException, IOException;


    /**
     * Checks if the page contains the given key with the given value.
     *
     * @param transaction The started transaction for this operation
     * @param key The key we are looking for
     * @param value The value associated with the given key
     * @return true if the key and value are associated with each other, false otherwise
     * @throws IOException if we can't access the underlying database
     */
    boolean contains( Transaction transaction, K key, V value ) throws IOException;

    
    /**
     * Copies the current page and all its keys, with the transaction's revision and a new ID.
     *
     * @param transaction The started transaction for this operation
     * @return The copied page
     */
    Page<K, V> copy( WriteTransaction transaction );


    /**
     * Browses the tree, looking for the given key, and creates a Cursor on top
     * of the found result.
     *
     * @param transaction The started transaction for this operation
     * @param key The key we are looking for.
     * @param stack The stack of parents we go through to get to this page
     * @param depth The depth in the stack we are in. This will be incremented for each child we go down to.
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
     * @param depth The depth in the stack we are in. This will be incremented for each child we go down to.
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
     * @param depth The depth in the stack we are in. This will be incremented for each child we go down to.
     * @return A Cursor to browse the keys
     * @throws IOException If we have an error while trying to access the page
     */
    KeyCursor<K> browseKeys( Transaction transaction, ParentPos<K, K>[] stack, int depth )
        throws IOException;


    /**
     * Finds the leftmost key in this page. If the page is a node, it will go
     * down in the leftmost children to recursively find the leftmost key.
     *
     * @param transaction The started transaction for this operation
     * @return The leftmost key in the tree
     * @throws IOException If we have an error while trying to access the page
     */
    K getLeftMostKey( Transaction transaction ) throws IOException;


    /**
     * Finds the rightmost key in this page. If the page is a node, it will go
     * down in the rightmost children to recursively find the rightmost key.
     *
     * @param transaction The started transaction for this operation
     * @return The rightmost key in the tree
     * @throws IOException If we have an error while trying to access the page
     */
    K getRightMostKey( Transaction transaction ) throws IOException;


    /**
     * Finds the leftmost element in this page. If the page is a node, it will go
     * down in the leftmost children to recursively find the leftmost element.
     *
     * @param transaction The started transaction for this operation
     * @return The leftmost element in the tree
     * @throws IOException If we have an error while trying to access the page
     */
    Tuple<K, V> findLeftMost( Transaction transaction ) throws IOException;


    /**
     * Finds the rightmost element in this page. If the page is a node, it will go
     * down in the rightmost children to recursively find the rightmost element.
     *
     * @param transaction The started transaction for this operation
     * @return The rightmost element in the tree
     * @throws IOException If we have an error while trying to access the page
     */
    Tuple<K, V> findRightMost( Transaction transaction ) throws IOException;


    /**
     * Pretty-prints the tree with tabs
     * @param tabs The tabs to add in front of each node
     * @return A pretty-print dump of the tree
     */
    String dumpPage( String tabs );


    /**
     * Checks if the given key exists.
     *
     * @param transaction The started transaction for this operation
     * @param key The key we are looking at
     * @return true if the key is present, false otherwise
     * @throws IOException If we have an error while trying to access the page
     */
    boolean hasKey( Transaction transaction, K key ) throws IOException;


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
