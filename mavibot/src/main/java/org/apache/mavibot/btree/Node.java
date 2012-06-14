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
 * A MVCC Node. It stores the keys and references to its children page. It does not
 * contain any value.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
public class Node<K, V> extends AbstractPage<K, V>
{
    /** Children pages associated with keys. */
    protected Page<K, V>[] children;
    
    
    /**
     * Create a new Node which will contain only one key, with references to
     * a left and right page. This is a specific constructor used by the btree
     * when the root was full when we added a new value.
     * 
     * @param btree the parent BTree
     * @param revision the Node revision
     * @param key The new key
     * @param leftPage The left page
     * @param rightPage The right page
     */
    /* No qualifier */ Node( BTree<K, V> btree, long revision, K key, Page<K, V> leftPage, Page<K, V> rightPage )
    {
        // Store the common values
        this.btree = btree;
        this.revision = revision;
        nbElems = 1;
        
        // Create the children array, and store the left and right children
        children = (Page<K, V>[])new Object[btree.getPageSize()];
        children[0] = leftPage;
        children[1] = rightPage;
        
        // Create the keys array and store the pivot into it
        keys = (K[])new Object[btree.getPageSize()];
        keys[0] = key;
    }
}
