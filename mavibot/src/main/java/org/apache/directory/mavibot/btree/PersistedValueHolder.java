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
import java.util.UUID;

import org.apache.directory.mavibot.btree.exception.BTreeAlreadyCreatedException;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.BTreeCreationException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A holder to store the Values
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * @param <V> The value type
 */
/* No qualifier */class PersistedValueHolder<V> extends AbstractValueHolder<V>
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( PersistedValueHolder.class );

    /** The parent BTree */
    protected PersistedBTree<V, V> parentBtree;

    /** The serialized value */
    private byte[] raw;

    /** A flag set to true when the raw value has been deserialized */
    private boolean isDeserialized = false;

    /** A flag to signal that the raw value represent the serialized values in their last state */
    private boolean isRawUpToDate = false;


    /**
     * Creates a new instance of a ValueHolder, containing the serialized values.
     *
     * @param parentBtree the parent BTree
     * @param raw The raw data containing the values
     * @param nbValues the number of stored values
     * @param raw the byte[] containing either the serialized array of values or the sub-btree offset
     */
    PersistedValueHolder( BTree<?, V> parentBtree, int nbValues, byte[] raw )
    {
        this.parentBtree = ( PersistedBTree<V, V> ) parentBtree;
        this.valueSerializer = parentBtree.getValueSerializer();
        this.raw = raw;
        isRawUpToDate = true;
        valueThresholdUp = PersistedBTree.valueThresholdUp;
        valueThresholdLow = PersistedBTree.valueThresholdLow;

        // We create the array of values if they fit in an array. If they are stored in a
        // BTree, we do nothing atm.
        if ( nbValues <= valueThresholdUp )
        {
            // The values are contained into an array
            valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), nbValues );
        }
    }


    /**
     * Creates a new instance of a ValueHolder, containing Values. This constructor is called
     * when we need to create a new ValueHolder with deserialized values.
     *
     * @param parentBtree The parent BTree
     * @param values The Values stored in the ValueHolder
     */
    PersistedValueHolder( BTree<?, V> parentBtree, V... values )
    {
        this.parentBtree = ( PersistedBTree<V, V> ) parentBtree;
        this.valueSerializer = parentBtree.getValueSerializer();
        valueThresholdUp = PersistedBTree.valueThresholdUp;
        valueThresholdLow = PersistedBTree.valueThresholdLow;

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

                try
                {
                    build( ( PersistedBTree<V, V> ) valueBtree, values );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }

                manageSubTree();
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
        // Check that the values are deserialized before doing anything
        checkAndDeserialize();

        return super.getCursor();
    }


    /**
     * @return the raw representation of the value holder. The serialized value will not be the same
     * if the values are stored in an array or in a btree. <br/>
     * If they are stored in a BTree, the raw value will contain the offset of the btree, otherwise
     * it will contain a byte[] which will contain each serialized value, prefixed by their length.
     *
     */
    /* No qualifier*/byte[] getRaw()
    {
        if ( isRawUpToDate )
        {
            // Just have to return the raw value
            return raw;
        }

        if ( isSubBtree() )
        {
            // The values are stored into a subBtree, return the offset of this subBtree
            long btreeOffset = ( ( PersistedBTree<V, V> ) valueBtree ).getBtreeOffset();
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
     * {@inheritDoc}
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
    protected void createSubTree()
    {
        PersistedBTreeConfiguration<V, V> configuration = new PersistedBTreeConfiguration<V, V>();
        configuration.setAllowDuplicates( false );
        configuration.setKeySerializer( valueSerializer );
        configuration.setName( UUID.randomUUID().toString() );
        configuration.setValueSerializer( valueSerializer );
        configuration.setParentBTree( parentBtree );
        configuration.setBtreeType( BTreeTypeEnum.PERSISTED_SUB );

        valueBtree = BTreeFactory.createPersistedBTree( configuration );
        ( ( PersistedBTree<V, V> ) valueBtree ).setRecordManager( parentBtree.getRecordManager() );
    }


    /**
     * Push the sub-BTree into the RecordManager
     */
    protected void manageSubTree()
    {
        try
        {
            parentBtree.getRecordManager().manageSubBtree( valueBtree );
            raw = null;
        }
        catch ( BTreeAlreadyManagedException e )
        {
            // should never happen
            throw new BTreeAlreadyCreatedException( e );
        }
        catch ( IOException e )
        {
            throw new BTreeCreationException( e );
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
     * {@inheritDoc}
     */
    public void add( V value )
    {
        // First check that we have a loaded BTree
        checkAndDeserialize();

        super.add( value );

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
                    int nbValues = ( int ) ( valueBtree.getNbElems() - 1 );

                    // We have to switch to an Array of values
                    valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), nbValues );

                    // Now copy all the value but the one we have removed
                    TupleCursor<V, V> cursor = valueBtree.browse();
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

                    cursor.close();

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
            catch ( KeyNotFoundException knfe )
            {
                // TODO Auto-generated catch block
                knfe.printStackTrace();
                return null;
            }
        }
        else
        {
            return null;
        }
    }


    /**
     * {@inheritDoc}
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
     * {@inheritDoc}
     */
    public boolean contains( V checkedValue )
    {
        // First, deserialize the value if it's still a byte[]
        checkAndDeserialize();

        return super.contains( checkedValue );
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
        PersistedValueHolder<V> copy = ( PersistedValueHolder<V> ) super.clone();

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
            System.arraycopy( raw, 0, copy.raw, 0, raw.length );
        }

        return copy;
    }


    @Override
    public V replaceValueArray( V newValue )
    {
        V val = super.replaceValueArray( newValue );
        // The raw value is not anymore up to date with the content
        isRawUpToDate = false;

        return val;
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
        valueBtree = parentBtree.getRecordManager().loadDupsBtree( offset, parentBtree );
    }


    /**
     * @return The sub-btree offset
     */
    /* No qualifier */long getOffset()
    {
        if ( valueArray == null )
        {
            return ( ( PersistedBTree<V, V> ) valueBtree ).getBtreeOffset();
        }
        else
        {
            return -1L;
        }
    }


    /**
     * Constructs the sub-BTree using bulkload instead of performing sequential inserts.
     * 
     * @param btree the sub-BTtree to be constructed
     * @param dupKeyValues the array of values to be inserted as keys
     * @return The created BTree
     * @throws Exception
     */
    private BTree<V, V> build( PersistedBTree<V, V> btree, final V[] dupKeyValues ) throws Exception
    {
        Iterator<Tuple<V, V>> valueIterator = new Iterator<Tuple<V, V>>()
        {
            int pos = 0;


            @Override
            public Tuple<V, V> next()
            {
                // We can now return the found value
                V value = dupKeyValues[pos];
                pos++;

                return new Tuple<V, V>( value, value );
            }


            @Override
            public boolean hasNext()
            {
                // Check that we have at least one element to read
                return pos < dupKeyValues.length;
            }


            @Override
            public void remove()
            {
            }

        };

        BulkLoader.load( btree, valueIterator, dupKeyValues.length );

        return btree;
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

        sb.append( "]" );

        return sb.toString();
    }
}
