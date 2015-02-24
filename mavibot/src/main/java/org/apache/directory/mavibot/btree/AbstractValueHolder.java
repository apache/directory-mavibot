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


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * A holder to store the Values
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * @param <V> The value type
 */
/* No qualifier*/abstract class AbstractValueHolder<V> implements ValueHolder<V>
{
    /** The BTree storing multiple value, if we have more than one value */
    protected BTree<V, V> valueBtree;

    /** The array storing from 1 to N values */
    protected V[] valueArray;

    /** The Value serializer */
    protected ElementSerializer<V> valueSerializer;

    /** The configuration for the array <-> BTree switch. Default to 1 */
    protected int valueThresholdUp = 1;
    protected int valueThresholdLow = 1;

    protected int nbArrayElems;


    /**
     * {@inheritDoc}
     */
    public boolean isSubBtree()
    {
        return valueBtree != null;
    }


    /**
     * Create a clone of this instance
     */
    public ValueHolder<V> clone() throws CloneNotSupportedException
    {
        ValueHolder<V> copy = ( ValueHolder<V> ) super.clone();

        return copy;
    }


    /**
     * @return a cursor on top of the values
     */
    public ValueCursor<V> getCursor()
    {
        if ( valueBtree != null )
        {
            return new ValueBTreeCursor<V>( valueBtree );
        }
        else
        {
            return new ValueArrayCursor<V>( valueArray );
        }
    }


    /**
     * Find the position of a given value in the array, or the position where we
     * would insert the element (in this case, the position will be negative).
     * As we use a 0-based array, the negative position for 0 is -1.
     * -1 means the element can be added in position 0
     * -2 means the element can be added in position 1
     * ... 
     */
    private int findPos( V value )
    {
        if ( valueArray.length == 0 )
        {
            return -1;
        }

        // Do a search using dichotomy
        int pivot = valueArray.length / 2;
        int low = 0;
        int high = valueArray.length - 1;
        Comparator<V> comparator = valueSerializer.getComparator();

        while ( high > low )
        {
            switch ( high - low )
            {
                case 1:
                    // We have 2 elements
                    int result = comparator.compare( value, valueArray[pivot] );

                    if ( result == 0 )
                    {
                        return pivot;
                    }

                    if ( result < 0 )
                    {
                        if ( pivot == low )
                        {
                            return -( low + 1 );
                        }
                        else
                        {
                            result = comparator.compare( value, valueArray[low] );

                            if ( result == 0 )
                            {
                                return low;
                            }

                            if ( result < 0 )
                            {
                                return -( low + 1 );
                            }
                            else
                            {
                                return -( low + 2 );
                            }
                        }
                    }
                    else
                    {
                        if ( pivot == high )
                        {
                            return -( high + 2 );
                        }
                        else
                        {
                            result = comparator.compare( value, valueArray[high] );

                            if ( result == 0 )
                            {
                                return high;
                            }

                            if ( result < 0 )
                            {
                                return -( high + 1 );
                            }
                            else
                            {
                                return -( high + 2 );
                            }
                        }
                    }

                default:
                    // We have 3 elements
                    result = comparator.compare( value, valueArray[pivot] );

                    if ( result == 0 )
                    {
                        return pivot;
                    }

                    if ( result < 0 )
                    {
                        high = pivot - 1;
                    }
                    else
                    {
                        low = pivot + 1;
                    }

                    pivot = ( high + low ) / 2;

                    continue;
            }
        }

        int result = comparator.compare( value, valueArray[pivot] );

        if ( result == 0 )
        {
            return pivot;
        }

        if ( result < 0 )
        {
            return -( pivot + 1 );
        }
        else
        {
            return -( pivot + 2 );
        }
    }


    /**
     * Check if the array of values contains a given value
     */
    private boolean arrayContains( V value )
    {
        if ( valueArray.length == 0 )
        {
            return false;
        }

        // Do a search using dichotomy
        return findPos( value ) >= 0;
    }


    /**
     * Check if the subBtree contains a given value
     */
    protected boolean btreeContains( V value )
    {
        try
        {
            return valueBtree.hasKey( value );
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
        catch ( KeyNotFoundException knfe )
        {
            knfe.printStackTrace();
            return false;
        }
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( V checkedValue )
    {
        if ( valueArray == null )
        {
            return btreeContains( checkedValue );
        }
        else
        {
            return arrayContains( checkedValue );
        }
    }


    /**
     * Create a new Sub-BTree to store the values.
     */
    protected abstract void createSubTree();


    /**
     * Manage a new Sub-BTree .
     */
    protected abstract void manageSubTree();


    /**
     * Add the value in an array
     */
    private void addInArray( final V value )
    {
        // We have to check that we have reached the threshold or not
        if ( size() >= valueThresholdUp )
        {
            // Ok, transform the array into a btree
            createSubTree();

            Iterator<Tuple<V, V>> valueIterator = new Iterator<Tuple<V, V>>()
            {
                int pos = 0;


                @Override
                public Tuple<V, V> next()
                {
                    // We can now return the found value
                    if ( pos == valueArray.length )
                    {
                        // Special case : deal with the added value
                        pos++;

                        return new Tuple<V, V>( value, value );
                    }
                    else
                    {
                        V oldValue = valueArray[pos];
                        pos++;

                        return new Tuple<V, V>( oldValue, oldValue );
                    }
                }


                @Override
                public boolean hasNext()
                {
                    // Check that we have at least one element to read
                    return pos < valueArray.length + 1;
                }


                @Override
                public void remove()
                {
                }

            };

            try
            {
                BulkLoader.load( valueBtree, valueIterator, valueArray.length );
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            manageSubTree();

            // And make the valueArray to be null now
            valueArray = null;
        }
        else
        {
            // Create the array if it's null
            if ( valueArray == null )
            {
                valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), 1 );
                nbArrayElems = 1;
                valueArray[0] = value;
            }
            else
            {
                // check that the value is not already present in the ValueHolder
                int pos = findPos( value );

                if ( pos >= 0 )
                {
                    // The value exists : nothing to do
                    return;
                }

                // Ok, we just have to insert the new element at the right position
                // We transform the position to a positive value 
                pos = -( pos + 1 );
                // First, copy the array
                V[] newValueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), valueArray.length + 1 );

                System.arraycopy( valueArray, 0, newValueArray, 0, pos );
                newValueArray[pos] = value;
                System.arraycopy( valueArray, pos, newValueArray, pos + 1, valueArray.length - pos );

                // And switch the arrays
                valueArray = newValueArray;
            }
        }
    }


    /**
     * Add the value in the subBTree
     */
    private void addInBtree( V value )
    {
        try
        {
            valueBtree.insert( value, null );
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    /**
     * {@inheritDoc}
     */
    public void add( V value )
    {
        if ( valueBtree == null )
        {
            addInArray( value );
        }
        else
        {
            addInBtree( value );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V replaceValueArray( V newValue )
    {
        if ( isSubBtree() )
        {
            throw new IllegalStateException( "method is not applicable for the duplicate B-Trees" );
        }

        V tmp = valueArray[0];

        nbArrayElems = 1;
        valueArray[0] = newValue;

        return tmp;
    }

}
