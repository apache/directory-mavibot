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


import java.util.Comparator;

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * A holder to store the Value
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * @param <V> The value type
 */
/* No qualifier*/abstract class AbstractValueHolder<V> implements ValueHolder<V>
{
    /** The array storing from 1 to N values */
    protected V value;

    /** The Value serializer */
    protected ElementSerializer<V> valueSerializer;


    /**
     * Create a clone of this instance
     */
    public ValueHolder<V> clone() throws CloneNotSupportedException
    {
        ValueHolder<V> copy = ( ValueHolder<V> ) super.clone();

        return copy;
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( V checkedValue )
    {
        Comparator<V> comparator = valueSerializer.getComparator();

        int result = comparator.compare( value,  checkedValue );
        
        return result == 0;
    }


    /**
     * {@inheritDoc}
     */
    public void set( V value )
    {
        this.value = value;
    }
}
