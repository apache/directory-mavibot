package org.apache.directory.mavibot.btree;


import java.io.IOException;

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;
import org.apache.directory.mavibot.btree.BTree;


/**
 * A class that encapsulate the values into an sub-btree
 */
/* No qualifier */class ValueBTreeCursor<V> implements ValueCursor<V>
{
    /** Store the current position in the array or in the BTree */
    private TupleCursor<V, V> cursor;

    /** The Value sub-btree */
    private BTree<V, V> valueBtree;


    /**
     * Create an instance
     */
    public ValueBTreeCursor( BTree<V, V> valueBtree )
    {
        this.valueBtree = valueBtree;

        // Start at -1 to be positioned before the first element
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
                e.printStackTrace();
                return false;
            }
            catch ( IOException e )
            {
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
            e.printStackTrace();
            return null;
        }
        catch ( IOException e )
        {
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
                e.printStackTrace();
                return false;
            }
            catch ( IOException e )
            {
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
            e.printStackTrace();
            return null;
        }
        catch ( IOException e )
        {
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
        return ( int ) valueBtree.getNbElems();
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "BTreeCursor";
    }
}
