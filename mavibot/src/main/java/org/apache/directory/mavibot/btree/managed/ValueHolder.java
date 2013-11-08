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
package org.apache.directory.mavibot.btree.managed;


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.UUID;

import org.apache.directory.mavibot.btree.TupleCursor;
import org.apache.directory.mavibot.btree.ValueCursor;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;


/**
 * A holder to store the Values
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * @param <V> The value type
 */
public class ValueHolder<V> implements Cloneable
{
    /** The deserialized value */
    private V[] valueArray;

    /** The BTree storing multiple value, if we have moe than a threashold values */
    private BTree<V, V> valueBtree;

    /** The serialized value */
    private byte[] raw;

    /** A flag set to true if the values are stored in a BTree */
    private boolean isSubBtree = false;

    /** The RecordManager */
    private BTree<?, V> btree;

    /** The Value serializer */
    private ElementSerializer<V> valueSerializer;

    /** An internal flag used when the values are not yet deserialized */
    private boolean isRaw = true;


    /**
     * Creates a new instance of a ValueHolder, containing the serialized values
     * 
     * @param valueSerializer The Value's serializer
     * @param raw The raw data containing the values
     */
    /* No qualifier */ValueHolder( BTree<?, V> btree, ElementSerializer<V> valueSerializer,
        boolean isSubBtree, int nbValues,
        byte[] raw )
    {
        this.valueSerializer = valueSerializer;
        this.raw = raw;
        this.isSubBtree = isSubBtree;
        this.btree = btree;

        if ( nbValues < BTree.valueThresholdUp )
        {
            // Keep an array
            valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), nbValues );
        }
        else
        {
            // Use a sub btree

            //raw = ByteBuffer.wrap( valueSerializer.serialize( key ) );
        }
    }


    /**
     * Creates a new instance of a ValueHolder, containing the serialized values
     * 
     * @param valueSerializer The Value's serializer
     * @param raw The raw data containing the values
     */
    /* No qualifier */ValueHolder( BTree<?, V> btree, ElementSerializer<V> valueSerializer,
        BTree<V, V> subBtree )
    {
        this.valueSerializer = valueSerializer;
        this.btree = btree;
        raw = null;
        isRaw = false;
        isSubBtree = true;
        valueBtree = subBtree;
    }


    /**
     * Creates a new instance of a ValueHolder, containing Values
     * 
     * @param valueSerializer The Value's serializer
     * @param values The Values stored in the ValueHolder
     */
    /* No qualifier */ValueHolder( BTree<?, V> btree, ElementSerializer<V> valueSerializer, V... values )
    {
        this.valueSerializer = valueSerializer;
        this.btree = btree;

        if ( values != null )
        {
            int nbValues = values.length;

            if ( nbValues < BTree.valueThresholdUp )
            {
                // Keep an array
                valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), nbValues );

                try
                {
                    System.arraycopy( values, 0, valueArray, 0, values.length );
                }
                catch ( ArrayStoreException ase )
                {
                    ase.printStackTrace();
                    throw ase;
                }

                // Serialize the values
                byte[][] data = new byte[nbValues][];
                int pos = 0;
                int length = 0;

                for ( V value : values )
                {
                    byte[] serializedValue = valueSerializer.serialize( value );

                    data[pos++] = serializedValue;
                    length += serializedValue.length;
                }

                raw = new byte[length];
                pos = 0;

                for ( byte[] bytes : data )
                {
                    System.arraycopy( bytes, 0, raw, pos, bytes.length );
                    pos += bytes.length;
                }
            }
            else
            {
                // Use a sub btree, now that we have reached the threshold
                createSubTree();

                // Now inject all the values into it
                for ( V value : values )
                {
                    try
                    {
                        valueBtree.insert( value, value );
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
        else
        {
            // No value, we create an empty array
            valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), 0 );

            //raw = ByteBuffer.wrap( valueSerializer.serialize( key ) );
        }

        isRaw = false;
    }


    /**
     * @return a cursor on top of the values
     */
    public ValueCursor<V> getCursor()
    {
        checkRaw();

        ValueCursor<V> cursor;

        if ( isSubBtree )
        {
            cursor = new ValueBtreeCursor();
        }
        else
        {
            cursor = new ValueArrayCursor();
        }

        return cursor;
    }

    /**
     * A class that encapsulate the values into an array
     */
    private class ValueArrayCursor implements ValueCursor<V>
    {
        /** Store the current position in the array or in the BTree */
        private int currentPos;


        /**
         * Create an instance
         */
        private ValueArrayCursor()
        {
            // Start at -1 to be positioned before the first element
            currentPos = -1;
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext()
        {
            if ( valueArray == null )
            {
                // Load the array from the raw data
                return false;
            }
            else
            {
                return currentPos < valueArray.length - 1;
            }
        }


        /**
         * {@inheritDoc}
         */
        public V next()
        {
            if ( valueArray == null )
            {
                // Deserialize the array
                return null;
            }
            else
            {
                currentPos++;

                if ( currentPos == valueArray.length )
                {
                    // We have reached the end of the array
                    return null;
                }
                else
                {
                    return valueArray[currentPos];
                }
            }
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasPrev() throws EndOfFileExceededException, IOException
        {
            if ( valueArray == null )
            {
                // Load the array from the raw data
                return false;
            }
            else
            {
                return currentPos > 0;
            }
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public void close()
        {
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public void beforeFirst() throws IOException
        {
            currentPos = -1;
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public void afterLast() throws IOException
        {
            currentPos = valueArray.length;
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public V prev() throws EndOfFileExceededException, IOException
        {
            if ( valueArray == null )
            {
                // Deserialize the array
                return null;
            }
            else
            {
                currentPos--;

                if ( currentPos == -1 )
                {
                    // We have reached the end of the array
                    return null;
                }
                else
                {
                    return valueArray[currentPos];
                }
            }
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public int size()
        {
            if ( valueArray != null )
            {
                return valueArray.length;
            }
            else
            {
                return 0;
            }
        }
    }

    /**
     * A class that encapsulate the values into an sub-btree
     */
    private class ValueBtreeCursor implements ValueCursor<V>
    {
        /** Store the current position in the array or in the BTree */
        private TupleCursor<V, V> cursor;


        /**
         * Create an instance
         */
        private ValueBtreeCursor()
        {
            // Start at -1 to be positionned before the first element
            try
            {
                if ( valueBtree != null )
                {
                    cursor = valueBtree.browse();
                }
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }


        /**
         * {@inheritDoc}}
         */
        @Override
        public boolean hasNext()
        {
            if ( cursor == null )
            {
                return false;
            }
            else
            {
                try
                {
                    return cursor.hasNext();
                }
                catch ( EndOfFileExceededException e )
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return false;
                }
                catch ( IOException e )
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return false;
                }
            }
        }


        /**
         * {@inheritDoc}}
         */
        public V next()
        {
            try
            {
                return cursor.next().getKey();
            }
            catch ( EndOfFileExceededException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        }


        /**
         * {@inheritDoc}}
         */
        @Override
        public boolean hasPrev() throws EndOfFileExceededException, IOException
        {
            if ( cursor == null )
            {
                return false;
            }
            else
            {
                try
                {
                    return cursor.hasPrev();
                }
                catch ( EndOfFileExceededException e )
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return false;
                }
                catch ( IOException e )
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return false;
                }
            }
        }


        /**
         * {@inheritDoc}}
         */
        @Override
        public void close()
        {
            if ( cursor != null )
            {
                cursor.close();
            }
        }


        /**
         * {@inheritDoc}}
         */
        @Override
        public void beforeFirst() throws IOException
        {
            if ( cursor != null )
            {
                cursor.beforeFirst();
            }
        }


        /**
         * {@inheritDoc}}
         */
        @Override
        public void afterLast() throws IOException
        {
            if ( cursor != null )
            {
                cursor.afterLast();
            }
        }


        /**
         * {@inheritDoc}}
         */
        @Override
        public V prev() throws EndOfFileExceededException, IOException
        {
            try
            {
                return cursor.prev().getKey();
            }
            catch ( EndOfFileExceededException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public int size()
        {
            if ( valueBtree != null )
            {
                return ( int ) valueBtree.getNbElems();
            }
            else
            {
                return 0;
            }
        }
    }


    /**
     * @return the raw representation of the value holder. 
     */
    public byte[] getRaw()
    {
        if ( isRaw )
        {
            // We don't have to serialize the ValueHolder, it has not been changed 
            return raw;
        }
        else
        {
            // Ok, some values have been added/modified/removed, we have to serialize the ValueHolder
            byte[][] valueBytes = new byte[valueArray.length * 2][];
            int length = 0;
            int pos = 0;

            for ( V value : valueArray )
            {
                // Serialize the value
                byte[] bytes = valueSerializer.serialize( value );
                length += bytes.length;

                // Serialize the value's length
                byte[] sizeBytes = IntSerializer.serialize( bytes.length );
                length += sizeBytes.length;

                // And store the two byte[]
                valueBytes[pos++] = sizeBytes;
                valueBytes[pos++] = bytes;
            }

            raw = new byte[length];
            pos = 0;

            for ( byte[] bytes : valueBytes )
            {
                System.arraycopy( bytes, 0, raw, pos, bytes.length );
                pos += bytes.length;
            }

            return raw;
        }
    }


    /**
     * @return the isSubBtree
     */
    public boolean isSubBtree()
    {
        return isSubBtree;
    }


    /**
     * @return the number of stored values
     */
    public int size()
    {
        if ( isSubBtree )
        {
            return ( int ) valueBtree.getNbElems();
        }
        else
        {
            return valueArray.length;
        }
    }


    /**
     * Create a new Sub-BTree to store the values.
     */
    private void createSubTree()
    {
        try
        {
            BTreeConfiguration<V, V> configuration = new BTreeConfiguration<V, V>();
            configuration.setAllowDuplicates( false );
            configuration.setKeySerializer( valueSerializer );
            configuration.setName( UUID.randomUUID().toString() );
            configuration.setValueSerializer( valueSerializer );
            configuration.setParentBTree( btree );
            configuration.setSubBtree( true );
            
            valueBtree = new BTree<V, V>( configuration );

            try
            {
                btree.getRecordManager().manage( valueBtree, true );
                isSubBtree = true;
                isRaw = false;
                raw = null;
            }
            catch ( BTreeAlreadyManagedException e )
            {
                // should never happen
                throw new RuntimeException( e );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }


    /**
     * Set the subBtree in the ValueHolder
     */
    /* No qualifier*/void setSubBtree( BTree<V, V> subBtree )
    {
        valueBtree = subBtree;
        raw = null;
        isRaw = false;
        isSubBtree = true;
        valueArray = null;
    }


    /**
     * Add a new value in the ValueHolder
     * 
     * @param value The added value
     */
    public void add( V value )
    {
        checkRaw();

        if ( !isSubBtree )
        {
            // We have to check that we have reached the threshold or not
            if ( valueArray.length + 1 > BTree.valueThresholdUp )
            {
                // Ok, transform the array into a btree
                createSubTree();

                try
                {
                    for ( V val : valueArray )
                    {
                        // Here, we should insert all the values in one shot then 
                        // write the btree on disk only once.
                        valueBtree.insert( val, null );
                    }

                    // We can delete the array now
                    valueArray = null;

                    // And inject the new value
                    valueBtree.insert( value, null );
                }
                catch ( IOException e )
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            else
            {
                // First check that the value is not already present in the ValueHolder
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
        else
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
    }


    /**
     * Add a new value in the ValueHolder
     * 
     * @param value The added value
     */
    public void remove( V value )
    {
        checkRaw();

        if ( !isSubBtree )
        {
            // First check that the value is not already present in the ValueHolder
            int pos = findPos( value );

            if ( pos < 0 )
            {
                // The value does not exists : nothing to do
                return;
            }

            // Ok, we just have to delete the new element at the right position
            // First, copy the array
            V[] newValueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), valueArray.length - 1 );

            System.arraycopy( valueArray, 0, newValueArray, 0, pos );
            System.arraycopy( valueArray, pos + 1, newValueArray, pos, valueArray.length - pos - 1 );

            // And switch the arrays
            valueArray = newValueArray;
        }
        else
        {
            try
            {
                valueBtree.delete( value );
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


    /**
     * Add a new value in the ValueHolder
     * 
     * @param value The added value
     */
    public boolean contains( V value )
    {
        checkRaw();

        if ( isSubBtree )
        {
            try
            {
                return valueBtree.hasKey( value );
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else
        {
            if ( valueArray.length == 0 )
            {
                return false;
            }

            // Do a search using dichotomy
            return findPos( value ) >= 0;
        }

        return true;
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
     * Create a clone of this instance
     */
    public ValueHolder<V> clone() throws CloneNotSupportedException
    {
        ValueHolder<V> copy = ( ValueHolder<V> ) super.clone();

        //copy the valueArray if it's not null
        // We don't clone the BTree, as we will create new revisions when 
        //modifying it
        if ( ( !isSubBtree ) && ( valueArray != null ) )
        {
            copy.valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), valueArray.length );
            System.arraycopy( valueArray, 0, copy.valueArray, 0, valueArray.length );
        }

        return copy;
    }


    /**
     * Check if we haven't yet deserialized the values, and if so, do it
     */
    private void checkRaw()
    {
        if ( isRaw )
        {
            // We haven't yet deserialized the values. Let's do it now
            if ( isSubBtree )
            {
                // This is a sub BTree, we have to read the tree from the offsets

            }
            else
            {
                // We have to deserialize the array of values
                int index = 0;
                int pos = 0;

                while ( pos < raw.length )
                {
                    try
                    {
                        int size = IntSerializer.deserialize( raw, pos );
                        pos += 4;

                        V value = valueSerializer.fromBytes( raw, pos );
                        pos += size;
                        valueArray[index++] = value;
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                }
            }

            isRaw = false;
        }
    }


    /**
     * @return The sub-btree offset
     */
    /* No qualifier */long getOffset()
    {
        if ( isSubBtree )
        {
            return valueBtree.getBtreeOffset();
        }
        else
        {
            return -1L;
        }
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "ValueHolder[" ).append( valueSerializer.getClass().getSimpleName() );

        if ( isRaw )
        {
            sb.append( ", isRaw[" ).append( raw.length ).append( "]" );
        }
        else
        {
            if ( isSubBtree )
            {
                sb.append( ", SubBTree" );
            }
            else
            {
                sb.append( ", array{" );

                if ( valueArray == null )
                {
                    sb.append( "}" );
                }
                else
                {
                    boolean isFirst = true;

                    for ( V value : valueArray )
                    {
                        if ( isFirst )
                        {
                            isFirst = false;
                        }
                        else
                        {
                            sb.append( "/" );
                        }

                        sb.append( value );
                    }

                    sb.append( "}" );
                }
            }
        }

        sb.append( "]" );

        return sb.toString();
    }
}
