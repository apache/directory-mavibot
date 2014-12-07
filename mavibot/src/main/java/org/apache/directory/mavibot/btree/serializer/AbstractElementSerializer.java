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
package org.apache.directory.mavibot.btree.serializer;


import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Comparator;


/**
 * An abstract ElementSerializer that implements comon methods
 *
 * @param <T> The type for the element to serialize and compare
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public abstract class AbstractElementSerializer<T> implements ElementSerializer<T>
{
    /** The associated comparator */
    private final Comparator<T> comparator;

    /** The type which is being serialized */
    private Class<?> type;


    /**
     * Create a new instance of Serializer
     */
    public AbstractElementSerializer( Comparator<T> comparator )
    {
        this.comparator = comparator;

        // We will extract the Type to use for values, using the comparator for that
        Class<?> comparatorClass = comparator.getClass();
        Type[] types = comparatorClass.getGenericInterfaces();

        if ( types[0] instanceof Class )
        {
            type = ( Class<?> ) types[0];
        }
        else
        {
            Type[] argumentTypes = ( ( ParameterizedType ) types[0] ).getActualTypeArguments();

            if ( ( argumentTypes != null ) && ( argumentTypes.length > 0 ) )
            {
                if ( argumentTypes[0] instanceof Class<?> )
                {
                    type = ( Class<?> ) argumentTypes[0];
                }
                else if ( argumentTypes[0] instanceof GenericArrayType )
                {
                    Class<?> clazz = ( Class<?> ) ( ( GenericArrayType ) argumentTypes[0] ).getGenericComponentType();

                    type = Array.newInstance( clazz, 0 ).getClass();
                }
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    public int compare( T type1, T type2 )
    {
        return comparator.compare( type1, type2 );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<T> getComparator()
    {
        return comparator;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?> getType()
    {
        return type;
    }
}
