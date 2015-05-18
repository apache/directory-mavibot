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
import java.util.Comparator;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * A B-tree interface, to be implemented by the PersistedBTree or the InMemoryBTree
 *
 * @param <K> The Key type
 * @param <V> The Value type
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface BTree<K, V>
{
    /** Default page size (number of entries per node) */
    int DEFAULT_PAGE_SIZE = 16;

    /** Default size of the buffer used to write data on disk. Around 1Mb */
    int DEFAULT_WRITE_BUFFER_SIZE = 4096 * 250;

    /** Define a default delay for a read transaction. This is 10 seconds */
    long DEFAULT_READ_TIMEOUT = 10 * 1000L;

    /** The B-tree allows duplicate values */
    boolean ALLOW_DUPLICATES = true;

    /** The B-tree forbids duplicate values */
    boolean FORBID_DUPLICATES = false;


    /**
     * Close the B-tree, cleaning up all the data structure
     */
    void close() throws IOException;


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
    void setPageSize( int pageSize );


    /**
     * @return the number of elements per page
     */
    int getPageSize();


    /**
     * Insert an entry in the B-tree.
     * <p>
     * We will replace the value if the provided key already exists in the
     * B-tree.
     *
     * @param key Inserted key
     * @param value Inserted value
     * @return Existing value, if any.
     * @throws IOException TODO
     */
    V insert( K key, V value ) throws IOException;


    /**
     * Delete the entry which key is given as a parameter. If the entry exists, it will
     * be removed from the tree, the old tuple will be returned. Otherwise, null is returned.
     *
     * @param key The key for the entry we try to remove
     * @return A Tuple<K, V> containing the removed entry, or null if it's not found.
     */
    Tuple<K, V> delete( K key ) throws IOException;


    /**
     * Delete the value from an entry associated with the given key. If the value
     * If the value is present, it will be deleted first, later if there are no other
     * values associated with this key(which can happen when duplicates are enabled),
     * we will remove the key from the tree.
     *
     * @param key The key for the entry we try to remove
     * @param value The value to delete (can be null)
     * @return A Tuple<K, V> containing the removed entry, or null if it's not found.
     */
    Tuple<K, V> delete( K key, V value ) throws IOException;


    /**
     * Find a value in the tree, given its key. If the key is not found,
     * it will throw a KeyNotFoundException. <br/>
     * Note that we can get a null value stored, or many values.
     *
     * @param key The key we are looking at
     * @return The found value, or null if the key is not present in the tree
     * @throws KeyNotFoundException If the key is not found in the B-tree
     * @throws IOException TODO
     */
    V get( K key ) throws IOException, KeyNotFoundException;


    /**
     * Get the rootPage associated to a given revision.
     *
     * @param revision The revision we are looking for
     * @return The rootPage associated to this revision
     * @throws IOException If we had an issue while accessing the underlying file
     * @throws KeyNotFoundException If the revision does not exist for this B-tree
     */
    Page<K, V> getRootPage( long revision ) throws IOException, KeyNotFoundException;


    /**
     * Get the current rootPage
     *
     * @return The current rootPage
     */
    Page<K, V> getRootPage();


    /**
     * @see Page#getValues(Object)
     */
    ValueCursor<V> getValues( K key ) throws IOException, KeyNotFoundException;


    /**
     * Find a value in the tree, given its key, at a specific revision. If the key is not found,
     * it will throw a KeyNotFoundException. <br/>
     * Note that we can get a null value stored, or many values.
     *
     * @param revision The revision for which we want to find a key
     * @param key The key we are looking at
     * @return The found value, or null if the key is not present in the tree
     * @throws KeyNotFoundException If the key is not found in the B-tree
     * @throws IOException If there was an issue while fetching data from the disk
     */
    V get( long revision, K key ) throws IOException, KeyNotFoundException;


    /**
     * Checks if the given key exists.
     *
     * @param key The key we are looking at
     * @return true if the key is present, false otherwise
     * @throws IOException If we have an error while trying to access the page
     * @throws KeyNotFoundException If the key is not found in the B-tree
     */
    boolean hasKey( K key ) throws IOException, KeyNotFoundException;


    /**
     * Checks if the given key exists for a given revision.
     *
     * @param revision The revision for which we want to find a key
     * @param key The key we are looking at
     * @return true if the key is present, false otherwise
     * @throws IOException If we have an error while trying to access the page
     * @throws KeyNotFoundException If the key is not found in the B-tree
     */
    boolean hasKey( long revision, K key ) throws IOException, KeyNotFoundException;


    /**
     * Checks if the B-tree contains the given key with the given value.
     *
     * @param key The key we are looking for
     * @param value The value associated with the given key
     * @return true if the key and value are associated with each other, false otherwise
     */
    boolean contains( K key, V value ) throws IOException;


    /**
     * Checks if the B-tree contains the given key with the given value for a given revision
     *
     * @param revision The revision we would like to browse
     * @param key The key we are looking for
     * @param value The value associated with the given key
     * @return true if the key and value are associated with each other, false otherwise
     * @throws KeyNotFoundException If the key is not found in the B-tree
     */
    boolean contains( long revision, K key, V value ) throws IOException, KeyNotFoundException;


    /**
     * Creates a cursor starting at the beginning of the tree
     *
     * @return A cursor on the B-tree
     * @throws IOException
     */
    TupleCursor<K, V> browse() throws IOException, KeyNotFoundException;


    /**
     * Creates a cursor starting at the beginning of the tree, for a given revision
     *
     * @param revision The revision we would like to browse
     * @return A cursor on the B-tree
     * @throws IOException If we had an issue while fetching data from the disk
     * @throws KeyNotFoundException If the key is not found in the B-tree
     */
    TupleCursor<K, V> browse( long revision ) throws IOException, KeyNotFoundException;


    /**
     * Creates a cursor starting on the given key
     *
     * @param key The key which is the starting point. If the key is not found,
     * then the cursor will always return null.
     * @return A cursor on the B-tree
     * @throws IOException
     */
    TupleCursor<K, V> browseFrom( K key ) throws IOException;


    /**
     * Creates a cursor starting on the given key at the given revision
     *
     * @param The revision we are looking for
     * @param key The key which is the starting point. If the key is not found,
     * then the cursor will always return null.
     * @return A cursor on the B-tree
     * @throws IOException If wxe had an issue reading the B-tree from disk
     * @throws KeyNotFoundException  If we can't find a rootPage for this revision
     */
    TupleCursor<K, V> browseFrom( long revision, K key ) throws IOException, KeyNotFoundException;


    /**
     * Creates a cursor starting at the beginning of the tree
     *
     * @return A cursor on the B-tree keys
     * @throws IOException
     */
    KeyCursor<K> browseKeys() throws IOException, KeyNotFoundException;


    /**
     * @return the key comparator
     */
    Comparator<K> getKeyComparator();


    /**
     * @return the value comparator
     */
    Comparator<V> getValueComparator();


    /**
     * @param keySerializer the Key serializer to set
     */
    void setKeySerializer( ElementSerializer<K> keySerializer );


    /**
     * @param valueSerializer the Value serializer to set
     */
    void setValueSerializer( ElementSerializer<V> valueSerializer );


    /**
     * Flush the latest revision to disk. We will replace the current file by the new one, as
     * we flush in a temporary file.
     */
    void flush() throws IOException;


    /**
     * @return the readTimeOut
     */
    long getReadTimeOut();


    /**
     * @param readTimeOut the readTimeOut to set
     */
    void setReadTimeOut( long readTimeOut );


    /**
     * @return the name
     */
    String getName();


    /**
     * @param name the name to set
     */
    void setName( String name );


    /**
     * @return the writeBufferSize
     */
    int getWriteBufferSize();


    /**
     * @param writeBufferSize the writeBufferSize to set
     */
    void setWriteBufferSize( int writeBufferSize );


    /**
     * @return the keySerializer
     */
    ElementSerializer<K> getKeySerializer();


    /**
     * @return the keySerializer FQCN
     */
    String getKeySerializerFQCN();


    /**
     * @return the valueSerializer
     */
    ElementSerializer<V> getValueSerializer();


    /**
     * @return the valueSerializer FQCN
     */
    String getValueSerializerFQCN();


    /**
     * @return The current B-tree revision
     */
    long getRevision();


    /**
     * @return The current number of elements in the B-tree
     */
    long getNbElems();


    /**
     * @return true if this B-tree allow duplicate values
     */
    boolean isAllowDuplicates();


    /**
     * @param allowDuplicates True if the B-tree will allow duplicate values
     */
    void setAllowDuplicates( boolean allowDuplicates );


    /**
     * @return the type
     */
    BTreeTypeEnum getType();
}
