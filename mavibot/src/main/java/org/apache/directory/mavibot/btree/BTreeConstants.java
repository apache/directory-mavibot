package org.apache.directory.mavibot.btree;

public class BTreeConstants
{
    /** A constant for an offset on a non existing page */
    public static final long NO_PAGE = -1L;

    /** A constant for a no limit page IO fetch */
    public static final long NO_LIMIT = -1L;

    /** The number of bytes used to store the size of a page */
    /* no qualifier*/ static final int PAGE_SIZE = 4;

    /** The size of the link to next page */
    /* no qualifier*/ static final int LINK_SIZE = 8;

    /** Some constants */
    /* no qualifier*/ static final int BYTE_SIZE = 1;
    /* no qualifier */static final int INT_SIZE = 4;
    /* no qualifier */static final int LONG_SIZE = 8;

    /** The default page size */
    public static final int DEFAULT_PAGE_SIZE = 512;
    
    /** The default number of cache pages : 10K pages, or 5Mb*/
    public static final int DEFAULT_CACHE_SIZE = 10_000;

    /** The minimal page size. Can't be below 64, as we have to store many thing sin the RMHeader */
    /* no qualifier*/ static final int MIN_PAGE_SIZE = 64;

    /** The default file name */
    /* no qualifier*/ static final String DEFAULT_FILE_NAME = "mavibot.db";

    /** A flag used by internal btrees */
    public static final boolean INTERNAL_BTREE = true;

    /** A flag used by internal btrees */
    public static final boolean NORMAL_BTREE = false;

    /** The B-tree of B-trees management btree name */
    /* no qualifier */static final String BTREE_OF_BTREES_NAME = "_btree_of_btrees_";

    /** The CopiedPages management btree name */
    /* no qualifier */static final String COPIED_PAGE_BTREE_NAME = "_copiedPageBtree_";

    /** A value stored into the transaction context for rollbacked transactions */
    /* no qualifier*/ static final int ROLLBACKED_TXN = 0;

    /** Hex chars */
    /* no qualifier*/ static final byte[] HEX_CHAR = new byte[]
        { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
}
