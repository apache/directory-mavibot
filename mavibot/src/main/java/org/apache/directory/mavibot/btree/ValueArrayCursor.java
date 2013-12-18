package org.apache.directory.mavibot.btree;


import java.io.IOException;

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;


/**
 * A class that encapsulate the values into an array
 */
/* No qualifier */class ValueArrayCursor<V> implements ValueCursor<V>
{
    /** Store the current position in the array or in the BTree */
    private int currentPos;

    /** The array storing values (1 to N) */
    private V[] valueArray;


    /**
     * Create an instance
     */
    public ValueArrayCursor( V[] valueArray )
    {
        // Start at -1 to be positioned before the first element
        currentPos = BEFORE_FIRST;
        this.valueArray = valueArray;
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
