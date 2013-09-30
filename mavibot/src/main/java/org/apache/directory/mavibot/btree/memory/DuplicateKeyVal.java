/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */
package org.apache.directory.mavibot.btree.memory;


/**
 * A class to hold a single value or multiple values (in a sub-tree) of a key.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class DuplicateKeyVal<V>
{
    private V singleValue;

    private BTree<V, V> subTree;


    /** No Qualifier */
    DuplicateKeyVal( V singleValue )
    {
        this.singleValue = singleValue;
    }


    /** No Qualifier */
    DuplicateKeyVal( BTree<V, V> subTree )
    {
        this.subTree = subTree;
    }


    public boolean isSingleValue()
    {
        return ( singleValue != null );
    }


    /**
     * @return the singleValue
     */
    public V getSingleValue()
    {
        return singleValue;
    }


    /**
     * @return the subTree
     */
    public BTree<V, V> getSubTree()
    {
        return subTree;
    }

}
