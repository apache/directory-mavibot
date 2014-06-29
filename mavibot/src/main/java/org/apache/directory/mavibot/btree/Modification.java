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
 * An abstract class used to store a modification done on a BTree.
 *  
 * @param <K> The key type
 * @param <V> The value type
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/abstract class Modification<K, V> extends Tuple<K, V>
{
    /** The byte used to define an Addition in the serialized journal */
    public static final byte ADDITION = 0;

    /** The byte used to define a Deletion in the serialized journal */
    public static final byte DELETION = 1;


    /**
     * Create a new Modification instance.
     * 
     * @param key The key being modified
     * @param value The value being modified
     */
    protected Modification( K key, V value )
    {
        super( key, value );
    }
}
