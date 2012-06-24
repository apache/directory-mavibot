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

/**
 * A MVCC Page interface.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
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
    InsertResult<K, V> insert( long revision, K key, V value );


    /**
     * delete an entry with the given key from this page. We first find the place were to
     * remove the <K,V> into the tree, by recursively browsing the pages :<br/>
     * <p>
     * 
     * @param revision The new revision for the modified pages
     * @param key The key to delete
     * @param parent The parent page
     * @return
     */
    DeleteResult<K, V> delete( long revision, K key, Page<K, V> parent );
    
    
    /**
     * Find the value associated with the given key, if any.
     * 
     * @param key The key we are looking for
     * @return The associated value, or null if there is none
     */
    V find( K key );
    
    
    /**
     * browse the tree, looking for the given key, and create a Cursor on top
     * of the found result.
     * 
     * @param key The key we are looking for.
     * @param transaction The started transaction for this operation
     * @return A Cursor to browse the next elements
     */
    Cursor<K, V> browse( K key, Transaction<K, V> transaction );
    
    
    /**
     * browse the whole tree, and create a Cursor on top of it.
     * 
     * @param transaction The started transaction for this operation
     * @return A Cursor to browse the next elements
     */
    Cursor<K, V> browse( Transaction<K, V> transaction );
    
    
    /**
     * @return the revision
     */
    long getRevision();


    /**
     * @return the page ID
     */
    long getId();
    
    
    /**
     * Pretty-print the tree with tabs
     * @param tabs The tabs to add in front of each node
     * @return A pretty-print dump of the tree
     */
    String dumpPage( String tabs );
}
