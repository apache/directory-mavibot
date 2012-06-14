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
    Comparator<K> comparator;

    /** The current rootPage */
    protected Page<K, V> rootPage;
    
    /** A map containing all the existing revisions */
    private Map<Long, Page<K, V>> roots = new ConcurrentHashMap<Long, Page<K, V>>();

}
