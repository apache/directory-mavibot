package org.apache.directory.mavibot.btree;

/**
 * An abstract class implementing the {@link WALObject} interface.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public abstract class AbstractWALObject<K, V> implements WALObject<K, V>
{
    /** The current revision */
    protected long revision = 0L;

    /** The B-tree information this object is associated with */
    protected BTreeInfo<K, V> btreeInfo;
    
    /** The PageIO of the serialized WALObject */
    protected PageIO[] pageIOs;

    /** The first {@link PageIO} storing the serialized Page on disk */
    protected long offset = BTreeConstants.NO_PAGE;
    
    /** This page ID */
    protected long id;
    
    /**
     * Create a new instance of WALObject
     */
    public AbstractWALObject()
    {
    }

    /**
     * Create a new instance of WALObject
     * @param btreeInfo The associated BTree information
     */
    public AbstractWALObject( BTreeInfo<K, V> btreeInfo )
    {
        this.btreeInfo = btreeInfo;
    }
    

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
        return btreeInfo.getName();
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
     * @return the revision
     */
    @Override
    public long getRevision()
    {
        return revision;
    }


    /**
     * @param revision the revision to set
     */
    /* no qualifier */void setRevision( long revision )
    {
        this.revision = revision;
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
    public BTreeInfo<K, V> getBtreeInfo()
    {
        return btreeInfo;
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
    protected void initId( RecordManagerHeader recordManagerHeader )
    {
        this.id = recordManagerHeader.idCounter++;
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isBTreeUser()
    {
        return btreeInfo.getType() != BTreeTypeEnum.BTREE_OF_BTREES && btreeInfo.getType() != BTreeTypeEnum.COPIED_PAGES_BTREE;
    }
}
