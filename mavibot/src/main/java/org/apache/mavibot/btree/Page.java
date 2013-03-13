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
import java.util.LinkedList;

import org.apache.mavibot.btree.exception.KeyNotFoundException;


/**
 * A MVCC Page interface.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public interface Page<K, V>
{
    /**
     * @return The number of keys present in this page
     */
    int getNbElems();


    /**
     * Insert the given key and value into this page. We first find the place were to
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
     * @param revision The new revision for the modified pages
     * @param key Inserted key
     * @param value Inserted value
     * @return Either a modified Page or an Overflow element if the Page was full
     */
    InsertResult<K, V> insert( long revision, K key, V value ) throws IOException;


    /**
     * delete an entry with the given key from this page. We first find the place were to
     * remove the <K,V> into the tree, by recursively browsing the pages :<br/>
     * <p>
     * 
     * @param revision The new revision for the modified pages
     * @param key The key to delete
     * @param parent The parent page
     * @param parentPos he position of the current page in it's parent
     * @return
     */
    DeleteResult<K, V> delete( long revision, K key, Page<K, V> parent, int parentPos );


    /**
     * Check if there is an element associated with the given key.
     * 
     * @param key The key we are looking at
     * @return true if the Key exists in the BTree 
     * @throws IOException If we have an error while trying to access the page
     */
    boolean exist( K key ) throws IOException;


    /**
     * Get the value associated with the given key, if any. If we don't have 
     * one, this method will throw a KeyNotFoundException.<br/>
     * Note that we may get back null if a null value has been associated 
     * with the key.
     * 
     * @param key The key we are looking for
     * @throws KeyNotFoundException If no entry with the given key can be found
     * @return The associated value, or null if there is none
     */
    V get( K key ) throws KeyNotFoundException;


    /**
     * browse the tree, looking for the given key, and create a Cursor on top
     * of the found result.
     * 
     * @param key The key we are looking for.
     * @param transaction The started transaction for this operation
     * @param stack The stack of parents we go through to get to this page
     * @return A Cursor to browse the next elements
     */
    Cursor<K, V> browse( K key, Transaction<K, V> transaction, LinkedList<ParentPos<K, V>> stack );


    /**
     * browse the whole tree, and create a Cursor on top of it.
     * 
     * @param transaction The started transaction for this operation
     * @param stack The stack of parents we go through to get to this page
     * @return A Cursor to browse the next elements
     */
    Cursor<K, V> browse( Transaction<K, V> transaction, LinkedList<ParentPos<K, V>> stack );


    /**
     * @return the revision
     */
    long getRevision();


    /**
     * Return the key at a given position
     * @param pos The position of the key we want to retrieve
     * @return The key found at the given position
     */
    K getKey( int pos );


    /**
     * Find the leftmost key in this page. If the page is a node, it will go
     * down in the leftmost children to recursively find the leftmost key.
     * 
     * @return The leftmost key in the tree
     */
    K getLeftMostKey();


    /**
     * Find the leftmost element in this page. If the page is a node, it will go
     * down in the leftmost children to recursively find the leftmost element.
     * 
     * @return The leftmost element in the tree
     */
    Tuple<K, V> findLeftMost();


    /**
     * Find the rightmost element in this page. If the page is a node, it will go
     * down in the rightmost children to recursively find the rightmost element.
     * 
     * @return The rightmost element in the tree
     */
    Tuple<K, V> findRightMost();


    /**
     * Pretty-print the tree with tabs
     * @param tabs The tabs to add in front of each node
     * @return A pretty-print dump of the tree
     */
    String dumpPage( String tabs );
}
