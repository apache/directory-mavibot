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
import java.util.Comparator;

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A holder to store the Value
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * @param <V> The value type
 */
/* No qualifier*/class ValueHolder<V> implements Cloneable
{
    /** The LoggerFactory used by this class */
    protected static final Logger LOG = LoggerFactory.getLogger( ValueHolder.class );

    /** The parent BTree */
    protected BTreeImpl parentBtree;

    /** The serialized value */
    private byte[] raw;

    /** A flag set to true when the raw value has been deserialized */
    private boolean isDeserialized = false;

    /** A flag to signal that the raw value represent the serialized values in their last state */
    private boolean isRawUpToDate = false;

    /** The array storing from 1 to N values */
    private V value;

    /** The Value serializer */
    protected ElementSerializer<V> valueSerializer;

    /**
     * Creates a new instance of a ValueHolder, containing the serialized values.
     *
     * @param parentBtree the parent BTree
     * @param raw The raw data containing the values
     * @param nbValues the number of stored values
     * @param raw the byte[] containing either the serialized array of values or the sub-btree offset
     */
    ValueHolder( BTree<?, V> parentBtree, byte[] raw )
    {
        this.parentBtree = ( BTreeImpl<V, V> ) parentBtree;
        this.valueSerializer = parentBtree.getValueSerializer();
        this.raw = raw;
        isRawUpToDate = true;
    }

    
    /**
     * Creates a new instance of a ValueHolder, containing a Value. This constructor is called
     * when we need to create a new ValueHolder with deserialized values.
     *
     * @param parentBtree The parent BTree
     * @param values The Value stored in the ValueHolder
     */
    public ValueHolder( BTree parentBtree, V value )
    {
        this.parentBtree = ( BTreeImpl ) parentBtree;
        this.valueSerializer = parentBtree.getValueSerializer();
        set( value );

        isDeserialized = true;
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

        // Process the value
        // Serialize the value
        raw = valueSerializer.serialize( value );

        // Update the flags
        isRawUpToDate = true;

        return raw;
    }
    
    
    /**
     * Deserialize the stored raw value
     * @throws IOException 
     */
    private void deserialize() throws IOException
    {
        // We haven't yet deserialized the values. Let's do it now.
        value = valueSerializer.fromBytes( raw, 0 );
    }


    /**
     * Check that the values are stored as raw value
     * @throws IOException 
     */
    private void checkAndDeserialize() throws IOException
    {
        if ( !isDeserialized )
        {
                // The values are stored into an array. Deserialize it now
            deserialize();
        }

        // Change the flag
        isDeserialized = true;
    }

    
    /**
     * Create a clone of this instance
     */
    @Override
    public ValueHolder<V> clone() throws CloneNotSupportedException
    {
        ValueHolder<V> copy = ( ValueHolder<V> ) super.clone();

        // Also clone the raw value if its up to date
        if ( isRawUpToDate )
        {
            copy.raw = new byte[raw.length];
            System.arraycopy( raw, 0, copy.raw, 0, raw.length );
        }

        return copy;
    }


    /**
     * {@inheritDoc}
     */
    public boolean contains( V checkedValue )
    {
        // First, deserialize the value if it's still a byte[]
        try
        {
            checkAndDeserialize();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

        Comparator<V> comparator = valueSerializer.getComparator();

        int result = comparator.compare( value,  checkedValue );
        
        return result == 0;
    }


    /**
     * {@inheritDoc}
     */
    public V get()
    {
        return value;
    }


    /**
     * {@inheritDoc}
     */
    public void set( V value )
    {
        this.value = value;

        // The raw value is not anymore up to date with the content
        isRawUpToDate = false;
        raw = null;
    }


    /**
     * {@inheritDoc}
     */
    public V remove( V value )
    {
        if ( contains( value ) )
        {
            V removedValue = this.value;
            this.value = null;
            
            // The raw value is not anymore up to date with the content
            isRawUpToDate = false;
            raw = null;
    
            return removedValue;
        }
        else 
        { 
            return null;
        }
    }


    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        if ( !isDeserialized )
        {
            sb.append( "isRaw[" ).append( raw.length ).append( "]" );
        }
        else
        {
            sb.append( value );
        }

        return sb.toString();
    }
}
