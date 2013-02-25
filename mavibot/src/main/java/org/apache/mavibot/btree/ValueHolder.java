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
 * A Value holder. As we may not store all the values in memory (except for an in-memory
 * BTree), we will use a SoftReference to keep a reference to a Value, and if it's null,
 * then we will load the Value from the underlying physical support, using the offset. 
 * 
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
/* No qualifier */interface ValueHolder<K, V>
{
    /**
     * @param btree The Btree storing the value
     * @return The stored value
     */
    V getValue( BTree<K, V> btree );
}
