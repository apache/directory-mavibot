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
import java.util.Comparator;
import java.util.UUID;

import org.apache.directory.mavibot.btree.BTree;
import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.ValueCursor;
import org.apache.directory.mavibot.btree.ValueBTreeCursor;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;


/**
 * A holder to store the Values
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 * @param <V> The value type
 */
public class ValueHolder<V> implements Cloneable
{
    /** The deserialized value */
    private V value;

    /** The BTree storing multiple value, if we have more than one value */
    private BTree<V, V> valueBtree;

    /** The RecordManager */
    private BTree<?, V> btree;


    /**
     * A class that encapsulate one single value
     */
    private class ValueSingletonCursor implements ValueCursor<V>
    {
        /** Store the current position in the array or in the BTree */
        private int currentPos;


        /**
         * Create an instance
         */
        private ValueSingletonCursor()
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
            return currentPos == BEFORE_FIRST;
        }


        /**
         * {@inheritDoc}
         */
        public V next()
        {
            switch ( currentPos )
            {
                case AFTER_LAST :
                    return null;
                    
                case BEFORE_FIRST :
                    currentPos = 0;
                    return value;
                    
                default :
                    currentPos = AFTER_LAST;
                    return null;
            }
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasPrev()
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
            switch ( currentPos )
            {
                case AFTER_LAST :
                    currentPos = 0;
                    return value;
                    
                case BEFORE_FIRST :
                    return null;
                    
                default :
                    currentPos = BEFORE_FIRST;
                    return null;
            }
        }


        /**
         * {@inheritDoc}
         */
        @Override
        public int size()
        {
            return 1;
        }


        /**
         * @see Object#toString()
         */
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            
            sb.append( "SingletonCursor , currentpos =" );
            
            switch ( currentPos ) 
            {
                case BEFORE_FIRST :
                    sb.append(  "BEFORE_FIRST" );
                    break;
                    
                case AFTER_LAST :
                    sb.append(  "AFTER_LAST" );
                    break;
                    
                default :
                    sb.append( "0/0" );
                    break;
            }
            
            return sb.toString();
        }
    }

    
    /**
     * Creates a new instance of a ValueHolder, containing the serialized values.
     * 
     * @param btree the container BTree
     * @param valueSerializer The Value's serializer
     * @param raw The raw data containing the values
     * @param nbValues the number of stored values
     * @param raw the byte[] containing either the serialized array of values or the sub-btree offset
     */
    /* No qualifier */ValueHolder( BTree<?, V> btree, int nbValues )
    {
        this.btree = btree;
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

        if ( ( values != null ) && ( values.length > 0 ) )
        {
            int nbValues = values.length;

            if ( nbValues < 2 )
            {
                // Store the value
                value = values[0];
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
     * @return a cursor on top of the values
     */
    public ValueCursor<V> getCursor()
    {
        ValueCursor<V> cursor;

        if ( valueBtree != null )
        {
            cursor = new ValueBTreeCursor<V>( valueBtree );
        }
        else
        {
            cursor = new ValueSingletonCursor();
        }

        return cursor;
    }


    /**
     * @return the isSubBtree
     */
    public boolean isSubBtree()
    {
        return valueBtree != null;
    }


    /**
     * @return the number of stored values
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
    private void createSubTree()
    {
        try
        {
            BTreeConfiguration<V, V> configuration = new BTreeConfiguration<V, V>();
            configuration.setAllowDuplicates( false );
            configuration.setName( UUID.randomUUID().toString() );
            
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
        value = null;
    }
    
    
    /**
     * Add the value in an array
     */
    private void addInBTree( V newValue )
    {
        // Ok, create a sub-btree
        try
        {
            valueBtree = new InMemoryBTree<V, V>( UUID.randomUUID().toString(), btree.getValueSerializer(),
                btree.getValueSerializer() );

            valueBtree.insert( value, null, 0 );
            valueBtree.insert( newValue, null, 0 );
            value = null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }


    /**
     * Add a new value in the ValueHolder
     * 
     * @param newValue The added value
     */
    public void add( V newValue )
    {
        if ( value != null )
        {
            try
            {
                valueBtree = new InMemoryBTree<V, V>( UUID.randomUUID().toString(), btree.getValueSerializer(),
                    btree.getValueSerializer() );

                valueBtree.insert( value, null, 0 );
                valueBtree.insert( newValue, null, 0 );
                value = null;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        else if ( valueBtree != null )
        {
            try
            {
                valueBtree.insert( newValue, null, 0 );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        else
        {
            this.value = newValue;
        }
    }
    
    
    /**
     * Remove a value from the ValueHolder
     * 
     * @param removedValue The value to remove
     */
    public V remove( V removedValue )
    {
        if ( valueBtree == null )
        {
            if ( removedValue == null )
            {
                return null; 
            }
            
            Comparator<V> comparator = btree.getValueSerializer().getComparator();

            int result = comparator.compare( removedValue, value );

            if ( result != 0 )
            {
                // The value does not exists : nothing to do
                return null;
            }
            else
            {
                V returnedValue = value;
                value = null;
                
                return returnedValue;
            }
        }
        else
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
                    value = valueBtree.browse().next().getKey();
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
    }
    
    
    /**
     * Check that a value exists in the ValueHolder
     * 
     * @param checkedValue The value to check
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
            Comparator<V> comparator = btree.getValueSerializer().getComparator();

            int result = comparator.compare( checkedValue, value );
            
            return result == 0;
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
    private int findPos1( V findValue )
    {
        if ( findValue == null )
        {
            return -1;
        }

        Comparator<V> comparator = btree.getValueSerializer().getComparator();

        int result = comparator.compare( findValue, value );
        
        return result;
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
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "ValueHolder[" ).append( btree.getValueSerializer().getClass().getSimpleName() );

        if ( valueBtree != null )
        {
            sb.append( ", SubBTree" );
        }
        else
        {
            sb.append( ", {" );
            sb.append( value );
            sb.append( "}" );
        }
        
        sb.append( "]" );

        return sb.toString();
    }
}
