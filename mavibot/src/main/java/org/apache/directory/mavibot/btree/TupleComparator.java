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

package org.apache.directory.mavibot.btree;


import java.util.Comparator;


/**
 * An comparator that encapsulate a Comparator to compare two tuples
 * using their key.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class TupleComparator<K, V> implements Comparator<Tuple<K, V>>
{
    /** the embedded Comparator to use for the key comparison */
    Comparator<K> keyComparator;

    /** the embedded Comparator to use for the value comparison */
    Comparator<V> valueComparator;


    /**
     * Creates a new instance of TupleComparator.
     *
     * @param keyComparator The inner key comparator
     * @param valueComparator The inner value comparator
     */
    public TupleComparator( Comparator<K> keyComparator, Comparator<V> valueComparator )
    {
        this.keyComparator = keyComparator;
    }


    /**
     * Compare two tuples. We compare the keys only. 
     * 
     * @param t1 The first tuple
     * @param t2 The second tuple
     * @return There are 5 possible results :
     * <ul>
     * <li>-1 : the first key is below the second key </li>
     * <li>0 : the two keys are equals, keys and values</li>
     * <li>1 : the first key is above the second key</li>
     * </ul>
     */
    @Override
    public int compare( Tuple<K, V> t1, Tuple<K, V> t2 )
    {
        return keyComparator.compare( t1.key, t2.key );
    }
}
