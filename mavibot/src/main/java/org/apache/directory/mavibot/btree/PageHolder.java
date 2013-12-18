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


/**
 * A Page holder. It stores the page and provide a way to access it.
 * 
 * @param <K> The type of the BTree key
 * @param <V> The type of the BTree value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class PageHolder<K, V>
{
    /** The BTree */
    protected BTree<K, V> btree;

    /** The stored page */
    private Page<K, V> page;


    /**
     * Create a new holder storing an offset and a SoftReference containing the element.
     * 
     * @param btree The associated BTree
     * @param page The element to store into a SoftReference
     **/
    /* no qualifier */PageHolder( BTree<K, V> btree, Page<K, V> page )
    {
        this.btree = btree;
        this.page = page;
    }


    /**
     * @return the stored page
     */
    /* no qualifier */Page<K, V> getValue()
    {
        return page;
    }
}
