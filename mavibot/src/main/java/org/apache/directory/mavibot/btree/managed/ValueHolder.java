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

import org.apache.directory.mavibot.btree.BTree;
import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.TupleCursor;
import org.apache.directory.mavibot.btree.ValueBTreeCursor;
import org.apache.directory.mavibot.btree.ValueCursor;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;


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

    /** The BTree storing multiple value, if we have more than a threshold values */
    private BTree<V, V> valueBtree;

    /** The serialized value */
    private byte[] raw;
    
    /** A flag set to true when the raw value has been deserialized */
    private boolean isDeserialized = false;
    
    /** A flag to signal that the raw value represent the serialized values in their last state */
    private boolean isRawUpToDate = false;

    /** The parent BTree */
    private BTree<?, V> btree;

    /** The Value serializer */
    private ElementSerializer<V> valueSerializer;


    /**
     * Creates a new instance of a ValueHolder, containing the serialized values.
     * 
     * @param btree the container BTree
     * @param raw The raw data containing the values
     * @param nbValues the number of stored values
     * @param raw the byte[] containing either the serialized array of values or the sub-btree offset
     */
    /* No qualifier */ValueHolder( BTree<?, V> btree, int nbValues, byte[] raw )
    {
        this.btree = btree;
        this.valueSerializer = btree.getValueSerializer();
        this.raw = raw;
        isRawUpToDate = true;

        // We create the array of values if they fit in an array. If they are stored in a 
        // BTree, we do nothing atm.
        if ( nbValues <= PersistedBTree.valueThresholdUp )
        {
            // The values are contained into an array
            valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), nbValues );
        }
    }


    /**
     * Creates a new instance of a ValueHolder, containing Values. This constructor is called
     * whe we need to create a new ValueHolder with deserialized values.
     * 
     * @param valueSerializer The Value's serializer
     * @param values The Values stored in the ValueHolder
     */
    /* No qualifier */ValueHolder( BTree<?, V> btree, V... values )
    {
        this.btree = btree;
        this.valueSerializer = btree.getValueSerializer();

        if ( values != null )
        {
            int nbValues = values.length;

            if ( nbValues < PersistedBTree.valueThresholdUp )
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
        }
        
        isDeserialized = true;
    }


    /**
     * @return a cursor on top of the values
     */
    public ValueCursor<V> getCursor()
    {
        ValueCursor<V> cursor;

        // Check that the values are deserialized before doing anything
        checkAndDeserialize();

        if ( valueArray == null )
        {
            cursor = new ValueBTreeCursor<V>( valueBtree );
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
            currentPos = BEFORE_FIRST;
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext()
        {
            return ( currentPos < valueArray.length - 1 ) && ( currentPos != AFTER_LAST );
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
                    currentPos = AFTER_LAST;
                    
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
            return currentPos > 0 || currentPos == AFTER_LAST;
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
            currentPos = BEFORE_FIRST;
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public void afterLast() throws IOException
        {
            currentPos = AFTER_LAST;
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
                if ( currentPos == AFTER_LAST )
                {
                    currentPos = valueArray.length - 1;
                }
                else
                {
                    currentPos--;
                }

                if ( currentPos == BEFORE_FIRST )
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
            return valueArray.length;
        }
    }

    /**
     * @return the raw representation of the value holder. The serialized value will not be the same
     * if the values are stored in an array or in a btree. <br/>
     * If they are stored in a BTree, the raw value will contain the offset of the btree, otherwise
     * it will contain a byte[] which will contain each serialized value, prefixed by their length. 
     * 
     */
    /* No qualifier*/ byte[] getRaw()
    {
        if ( isRawUpToDate )
        {
            // Just have to return the raw value
            return raw;
        }

        if ( isSubBtree() )
        {
            // The values are stored into a subBtree, return the offset of this subBtree
            long btreeOffset = ((PersistedBTree<V, V>)valueBtree).getBtreeOffset();
            raw = LongSerializer.serialize( btreeOffset );
        }
        else
        {
            // Create as many byte[] as we have length and serialized values to store
            byte[][] valueBytes = new byte[valueArray.length * 2][];
            int length = 0;
            int pos = 0;
    
            // Process each value now
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
    
            // Last, not least, create a buffer large enough to contain all the created byte[],
            // and copy all those byte[] into this buffer
            raw = new byte[length];
            pos = 0;
    
            for ( byte[] bytes : valueBytes )
            {
                System.arraycopy( bytes, 0, raw, pos, bytes.length );
                pos += bytes.length;
            }
        }
        
        // Update the flags
        isRawUpToDate = true;

        return raw;
    }


    /**
     * @return the isSubBtree
     */
    public boolean isSubBtree()
    {
        return valueArray == null;
    }


    /**
     * @return the number of stored values
     */
    public int size()
    {
        checkAndDeserialize();

        if ( valueArray == null )
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
            
            valueBtree = BTreeFactory.createBTree( configuration );

            try
            {
                ((PersistedBTree<V, V>)btree).getRecordManager().manage( valueBtree, RecordManager.INTERNAL_BTREE );
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
        valueArray = null;
        isDeserialized = true;
        isRawUpToDate = false;
    }
    
    
    /**
     * Check that the values are stored as raw value
     */
    private void checkAndDeserialize()
    {
        if ( !isDeserialized )
        {
            if ( valueArray == null )
            {
                // the values are stored into a sub-btree. Read it now if it's not already done
                deserializeSubBtree();
            }
            else
            {
                // The values are stored into an array. Deserialize it now
                deserializeArray();
            }

            // Change the flag
            isDeserialized = true;
        }
    }
    
    /**
     * Add the value in an array
     */
    private void addInArray( V value )
    {
        checkAndDeserialize();

        // We have to check that we have reached the threshold or not
        if ( valueArray.length >= PersistedBTree.valueThresholdUp )
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
    

    /**
     * Add the value in the subBTree
     */
    private void addInBtree( V value )
    {
        // First check that we have a loaded BTree
        checkAndDeserialize();
        
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
     * Add a new value in the ValueHolder
     * 
     * @param value The added value
     */
    public void add( V value )
    {
        if ( valueArray != null )
        {
            addInArray( value );
        }
        else
        {
            addInBtree( value );
        }
        
        // The raw value is not anymore up to date with the content
        isRawUpToDate = false;
        raw = null;
    }
    
    
    /**
     * Remove a value from an array
     */
    private V removeFromArray( V value )
    {
        checkAndDeserialize();

        // First check that the value is not already present in the ValueHolder
        int pos = findPos( value );

        if ( pos < 0 )
        {
            // The value does not exists : nothing to do
            return null;
        }

        // Ok, we just have to delete the new element at the right position
        // First, copy the array
        V[] newValueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), valueArray.length - 1 );

        System.arraycopy( valueArray, 0, newValueArray, 0, pos );
        System.arraycopy( valueArray, pos + 1, newValueArray, pos, valueArray.length - pos - 1 );

        // Get the removed element
        V removedValue = valueArray[pos];
        
        // And switch the arrays
        valueArray = newValueArray;
        
        return removedValue;
    }

    
    /**
     * Remove the value from a sub btree
     */
    private V removeFromBtree( V removedValue )
    {
        // First check that we have a loaded BTree
        checkAndDeserialize();

        if ( btreeContains( removedValue ) )
        {
            try
            {
                if ( valueBtree.getNbElems() - 1 < PersistedBTree.valueThresholdLow )
                {
                    int nbValues = (int)(valueBtree.getNbElems() - 1);
                        
                    // We have to switch to an Array of values
                    valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), nbValues );
    
                    // Now copy all the value but the one we have removed
                    TupleCursor<V,V> cursor = valueBtree.browse();
                    V returnedValue = null;
                    int pos = 0;
                    
                    while ( cursor.hasNext() )
                    {
                        Tuple<V, V> tuple = cursor.next();
                        
                        V value = tuple.getKey();
                        
                        if ( valueSerializer.getComparator().compare( removedValue, value ) == 0 )
                        {
                            // This is the removed value : skip it
                            returnedValue = value;
                        }
                        else
                        {
                            valueArray[pos++] = value;
                        }
                    }
                    
                    return returnedValue;
                }
                else
                {
                    Tuple<V, V> removedTuple = valueBtree.delete( removedValue );
                    
                    if ( removedTuple != null )
                    {
                        return removedTuple.getKey();
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        }
        else
        {
            return null;
        }
    }

    /**
     * Add a new value in the ValueHolder
     * 
     * @param value The added value
     */
    public V remove( V value )
    {
        V removedValue = null;
        
        if ( valueArray != null )
        {
            removedValue = removeFromArray( value );
        }
        else
        {
            removedValue = removeFromBtree( value );
        }

        // The raw value is not anymore up to date wth the content
        isRawUpToDate = false;
        raw = null;
        
        return removedValue;
    }
    
    
    /**
     * Check if the array of values contains a given value
     */
    private boolean arrayContains( V value )
    {
        // First, deserialize the value if it's still a byte[]
        checkAndDeserialize();
        
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
    private boolean btreeContains( V value )
    {
        // First, deserialize the value if it's still a byte[]
        checkAndDeserialize();
        
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
    }


    /**
     * Add a new value in the ValueHolder
     * 
     * @param value The added value
     */
    public boolean contains( V value )
    {
        if ( valueArray == null )
        {
            return btreeContains( value );
        }
        else
        {
            return arrayContains( value );
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
     * Create a clone of this instance
     */
    public ValueHolder<V> clone() throws CloneNotSupportedException
    {
        ValueHolder<V> copy = ( ValueHolder<V> ) super.clone();

        // copy the valueArray if it's not null
        // We don't clone the BTree, as we will create new revisions when 
        // modifying it
        if ( valueArray != null )
        {
            copy.valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), valueArray.length );
            System.arraycopy( valueArray, 0, copy.valueArray, 0, valueArray.length );
        }
        
        // Also clone the raw value if its up to date
        if ( isRawUpToDate )
        {
            copy.raw = new byte[raw.length];
            System.arraycopy( raw,  0,  copy.raw, 0, raw.length );
        }

        return copy;
    }


    /**
     * Deserialize the values stored in an array
     */
    private void deserializeArray()
    {
        // We haven't yet deserialized the values. Let's do it now. The values are
        // necessarily stored in an array at this point
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


    /**
     * Deserialize the values stored in a sub-btree
     */
    private void deserializeSubBtree()
    {
        // Get the sub-btree offset
        long offset = LongSerializer.deserialize( raw );
        
        // and reload the sub btree
        valueBtree = ((PersistedBTree<V, V>)btree).getRecordManager().loadDupsBTree( offset );
    }
    

    /**
     * @return The sub-btree offset
     */
    /* No qualifier */long getOffset()
    {
        if ( valueArray == null )
        {
            return ((PersistedBTree<V, V>)valueBtree).getBtreeOffset();
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

        if ( !isDeserialized )
        {
            sb.append( ", isRaw[" ).append( raw.length ).append( "]" );
        }
        else
        {
            if ( valueArray == null )
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
