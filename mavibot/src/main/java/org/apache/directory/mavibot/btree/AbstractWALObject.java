package org.apache.directory.mavibot.btree;

/**
 * An abstract class implementing the {@link WALObject} interface.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public abstract class AbstractWALObject<K, V> implements WALObject<K, V>
{
    /** The B-tree this header is associated with */
    protected BTree<K, V> btree;
    
    /** The PageIO of the serialized WALObject */
    protected PageIO[] pageIOs;

    /** The first {@link PageIO} storing the serialized Page on disk */
    protected long offset = RecordManager.NO_PAGE;
    
    /** This page ID */
    protected long id;

    /**
     * {@inheritDoc}
     */
    @Override
    public long getOffset()
    {
        return offset;
    }


    /**
     * @param offset the offset to set
     */
    /* No qualifier */ void setOffset( long offset )
    {
        this.offset = offset;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String getName()
    {
        return null;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public PageIO[] getPageIOs()
    {
        return pageIOs;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setPageIOs( PageIO[] pageIOs )
    {
        this.pageIOs = pageIOs;
        offset = pageIOs[0].getOffset();
    }


    /**
     * @return the B-tree
     */
    @Override
    public BTree<K, V> getBtree()
    {
        return btree;
    }


    /**
     * Associate a B-tree with this BTreeHeader instance
     * 
     * @param btree the B-tree to set
     */
    /* no qualifier */void setBtree( BTree<K, V> btree )
    {
        this.btree = btree;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getId()
    {
        return id;
    }


    /**
     * @param id the id to set
     */
    /* no qualifier*/ void setId( long id )
    {
        this.id = id;
    }


    /**
     * Initialize the Page ID, using teh RecordManagerHeader counter
     * 
     * @param recordManagerHeader the RecordManagerHeader which contains the page ID counter
     */
    /* no qualifier*/ void initId( RecordManagerHeader recordManagerHeader )
    {
        this.id = recordManagerHeader.idCounter++;
    }
}
