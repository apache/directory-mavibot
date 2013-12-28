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


import java.util.concurrent.atomic.AtomicLong;


/**
 * Store in memory the information associated with a BTree. <br>
 * A BTree Header on disk contains the following elements :
 * <pre>
 * +--------------------+-------------+
 * | revision           | 8 bytes     |
 * +--------------------+-------------+
 * | nbElems            | 8 bytes     |
 * +--------------------+-------------+
 * | rootPageOffset     | 8 bytes     |
 * +--------------------+-------------+
 * | nextBtreeHeader    | 8 bytes     |
 * +--------------------+-------------+
 * | pageSize           | 4 bytes     |
 * +--------------------+-------------+
 * | name               | 4 bytes + N |
 * +--------------------+-------------+
 * | keySerializeFQCN   | 4 bytes + N |
 * +--------------------+-------------+
 * | valueSerializeFQCN | 4 bytes + N |
 * +--------------------+-------------+
 * </pre>
 * Each BtreeHeader will be written starting on a new page.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class BTreeHeader
{
    /** The current revision */
    private AtomicLong revision = new AtomicLong( 0L );

    /** The number of elements in this BTree */
    private AtomicLong nbElems = new AtomicLong( 0L );

    /** The offset of the BTree RootPage */
    private long rootPageOffset;

    /** The offset of the next BTree */
    private long nextBTreeOffset;

    /** The number of elements in a page for this BTree */
    private int pageSize;

    /** The BTree name */
    private String name;

    /** The FQCN of the Key serializer */
    private String keySerializerFQCN;

    /** The FQCN of the Value serializer */
    private String valueSerializerFQCN;

    // Those are data which aren't serialized : they are in memory only */
    /** The position in the file */
    private long btreeOffset;

    /** The existing versions */
    private long[] versions;

    private int allowDuplicates = 0;


    /**
     * Creates a BTreeHeader instance
     */
    public BTreeHeader()
    {
    }


    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }


    /**
     * @param name the name to set
     */
    /* no qualifier */void setName( String name )
    {
        this.name = name;
    }


    /**
     * @return the versions
     */
    public long[] getVersions()
    {
        return versions;
    }


    /**
     * @param versions the versions to set
     */
    /* no qualifier */void setVersions( long[] versions )
    {
        this.versions = versions;
    }


    /**
     * @return the btreeOffset
     */
    public long getBTreeOffset()
    {
        return btreeOffset;
    }


    /**
     * @param btreeOffset the btreeOffset to set
     */
    /* no qualifier */void setBTreeOffset( long btreeOffset )
    {
        this.btreeOffset = btreeOffset;
    }


    /**
     * @return the rootPageOffset
     */
    public long getRootPageOffset()
    {
        return rootPageOffset;
    }


    /**
     * @param rootPageOffset the rootPageOffset to set
     */
    /* no qualifier */void setRootPageOffset( long rootPageOffset )
    {
        this.rootPageOffset = rootPageOffset;
    }


    /**
     * @return the revision
     */
    public long getRevision()
    {
        return revision.get();
    }


    /**
     * @param revision the revision to set
     */
    /* no qualifier */void setRevision( long revision )
    {
        this.revision.set( revision );
    }


    /**
     * Increment the revision
     *
     * @return the new revision
     */
    /* no qualifier */long incrementRevision()
    {
        return revision.incrementAndGet();
    }


    /**
     * @return the nbElems
     */
    public long getNbElems()
    {
        return nbElems.get();
    }


    /**
     * Increment the number of elements
     */
    /* no qualifier */void incrementNbElems()
    {
        nbElems.incrementAndGet();
    }


    /**
     * Decrement the number of elements
     */
    public void decrementNbElems()
    {
        nbElems.decrementAndGet();
    }


    /**
     * @param nbElems the nbElems to set
     */
    /* no qualifier */void setNbElems( long nbElems )
    {
        this.nbElems.set( nbElems );
    }


    /**
     * @return the nextBTreeOffset
     */
    public long getNextBTreeOffset()
    {
        return nextBTreeOffset;
    }


    /**
     * @param nextBtreeOffset the nextBtreeOffset to set
     */
    /* no qualifier */void setNextBTreeOffset( long nextBTreeOffset )
    {
        this.nextBTreeOffset = nextBTreeOffset;
    }


    /**
     * @return the pageSize
     */
    public int getPageSize()
    {
        return pageSize;
    }


    /**
     * @param pageSize the pageSize to set
     */
    /* no qualifier */void setPageSize( int pageSize )
    {
        this.pageSize = pageSize;
    }


    /**
     * @return the keySerializerFQCN
     */
    public String getKeySerializerFQCN()
    {
        return keySerializerFQCN;
    }


    /**
     * @param keySerializerFQCN the keySerializerFQCN to set
     */
    /* no qualifier */void setKeySerializerFQCN( String keySerializerFQCN )
    {
        this.keySerializerFQCN = keySerializerFQCN;
    }


    /**
     * @return the valueSerializerFQCN
     */
    public String getValueSerializerFQCN()
    {
        return valueSerializerFQCN;
    }


    /**
     * @param valueSerializerFQCN the valueSerializerFQCN to set
     */
    /* no qualifier */void setValueSerializerFQCN( String valueSerializerFQCN )
    {
        this.valueSerializerFQCN = valueSerializerFQCN;
    }


    public boolean isAllowDuplicates()
    {
        return ( allowDuplicates == 1 );
    }


    /* no qualifier */void setAllowDuplicates( boolean allowDuplicates )
    {
        this.allowDuplicates = ( allowDuplicates ? 1 : 0 );
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Btree '" ).append( name ).append( "'" );
        sb.append( ", revision[" ).append( revision ).append( "]" );
        sb.append( ", btreeOffset[" ).append( btreeOffset ).append( "]" );
        sb.append( ", rootPageOffset[" ).append( rootPageOffset ).append( "]" );
        sb.append( ", nextBTree[" ).append( nextBTreeOffset ).append( "]" );
        sb.append( ", nbElems[" ).append( nbElems ).append( "]" );
        sb.append( ", pageSize[" ).append( pageSize ).append( "]" );
        sb.append( ", hasDuplicates[" ).append( isAllowDuplicates() ).append( "]" );
        sb.append( "{\n" );
        sb.append( "    Key serializer   : " ).append( keySerializerFQCN ).append( "\n" );
        sb.append( "    Value serializer : " ).append( valueSerializerFQCN ).append( "\n" );
        sb.append( "}\n" );

        if ( ( versions != null ) && ( versions.length != 0 ) )
        {
            sb.append( "Versions : \n" );
            sb.append( "{\n" );

            boolean isFirst = true;

            for ( long version : versions )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sb.append( ",\n" );
                }

                sb.append( "    " ).append( version );
            }

            sb.append( "}\n" );
        }

        return sb.toString();
    }
}
