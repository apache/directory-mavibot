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
 * A holder to store the Values
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * @param <V> The value type
 */
/* no qualifier */interface ValueHolder<V> extends Cloneable
{
    /**
     * Tells if a value is contained in this ValueHolder
     * 
     * @param checkedValue The added to check
     */
    boolean contains( V checkedValue );


    /**
     * @return the number of stored values
     */
    int size();


    /**
     * @return a cursor on top of the values
     */
    ValueCursor<V> getCursor();


    /**
     * @return true if we store the values in a sub btree
     */
    boolean isSubBtree();


    /**
     * Add a new value in the ValueHolder
     * 
     * @param newValue The added value
     */
    void add( V newValue );


    /**
     * Remove a value from the ValueHolder
     * 
     * @param removedValue The value to remove
     */
    V remove( V removedValue );

    
    /**
     * Replaces the single value present in the array.
     * 
     * This is only applicable for B-Trees that don't
     * support duplicate values.
     *
     * @param newValue the new value
     * @return the value that was replaced
     */
    V replaceValueArray( V newValue );
    

    /**
     * Create a clone of this instance
     * 
     * @return a new instance of a ValueHolder
     * @throws CloneNotSupportedException If we can't clone this instance
     */
    ValueHolder<V> clone() throws CloneNotSupportedException;
}
