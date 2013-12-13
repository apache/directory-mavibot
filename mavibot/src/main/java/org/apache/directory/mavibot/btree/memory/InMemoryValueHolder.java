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
package org.apache.directory.mavibot.btree.memory;


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.UUID;

import org.apache.directory.mavibot.btree.AbstractValueHolder;
import org.apache.directory.mavibot.btree.BTree;
import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;


/**
 * A holder to store the Values
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * @param <V> The value type
 */
public class InMemoryValueHolder<V> extends AbstractValueHolder<V>
{
    /**
     * Creates a new instance of a ValueHolder, containing the serialized values.
     * 
     * @param parentBtree the parent BTree
     * @param valueSerializer The Value's serializer
     * @param raw The raw data containing the values
     * @param nbValues the number of stored values
     * @param raw the byte[] containing either the serialized array of values or the sub-btree offset
     */
    InMemoryValueHolder( BTree<?, V> parentBtree, int nbValues )
    {
        valueSerializer = parentBtree.getValueSerializer();
    }


    /**
     * Creates a new instance of a ValueHolder, containing Values. This constructor is called
     * whe we need to create a new ValueHolder with deserialized values.
     * 
     * @param parentBtree The parent BTree
     * @param values The Values stored in the ValueHolder
     */
    InMemoryValueHolder( BTree<?, V> parentBtree, V... values )
    {
        valueSerializer = parentBtree.getValueSerializer();

        if ( ( values != null ) && ( values.length > 0 ) )
        {
            int nbValues = values.length;

            if ( nbValues < 2 )
            {
                // Store the value
                valueArray = values;
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
    }


    /**
     * {@inheritDoc}
     */
    public int size()
    {
        if ( valueBtree != null )
        {
            return ( int ) valueBtree.getNbElems();
        }
        else
        {
            return 1;
        }
    }


    /**
     * Create a new Sub-BTree to store the values.
     */
    protected void createSubTree()
    {
        try
        {
            BTreeConfiguration<V, V> configuration = new BTreeConfiguration<V, V>();
            configuration.setAllowDuplicates( false );
            configuration.setName( UUID.randomUUID().toString() );
            configuration.setKeySerializer( valueSerializer );
            configuration.setValueSerializer( valueSerializer );

            valueBtree = new InMemoryBTree<V, V>( configuration );
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
        valueArray = null;
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

        return removedValue;
    }

    
    /**
     * Remove the value from a sub btree
     */
    private V removeFromBtree( V removedValue )
    {
        V returnedValue = null;
        
        try
        {
            Tuple<V, V> removedTuple = valueBtree.delete( removedValue );
            
            if ( removedTuple != null )
            {
                returnedValue = removedTuple.getKey();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        if ( valueBtree.getNbElems() == 1 )
        {
            try
            {
                valueArray = ( V[] ) Array.newInstance( valueSerializer.getType(), 1 );
                valueArray[0] = valueBtree.browse().next().getKey();
                valueBtree.close();
                valueBtree = null;
            }
            catch ( EndOfFileExceededException e )
            {
                throw new RuntimeException( e );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        
        return returnedValue;
    }
    
    
    /**
     * Remove a value from an array
     */
    private V removeFromArray( V value )
    {
        // First check that the value is not already present in the ValueHolder
        Comparator<V> comparator = valueSerializer.getComparator();

        int result = comparator.compare( valueArray[0], value );

        if ( result != 0 )
        {
            // The value does not exists : nothing to do
            return null;
        }
        else
        {
            V returnedValue = valueArray[0];
            valueArray[0] = null;
          
            return returnedValue;
        }
    }

    
    /**
     * {@inheritDoc}
     */
    public boolean contains( V checkedValue )
    {
        if ( valueBtree != null )
        {
            try
            {
                return valueBtree.hasKey( checkedValue );
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        else
        {
            Comparator<V> comparator = valueSerializer.getComparator();

            int result = comparator.compare( checkedValue, valueArray[0] );
            
            return result == 0;
        }
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "ValueHolder[" ).append( valueSerializer.getClass().getSimpleName() );

        if ( valueBtree != null )
        {
            sb.append( ", SubBTree" );
        }
        else
        {
            sb.append( ", {" );
            sb.append( valueArray[0] );
            sb.append( "}" );
        }
        
        sb.append( "]" );

        return sb.toString();
    }
}
