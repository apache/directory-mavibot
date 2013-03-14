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
package org.apache.mavibot.btree.store;


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
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class BTreeHeader
{
    /** The current revision */
    private long revision;

    /** The number of elements in this BTree */
    private long nbElems;

    /** The offset of the BTree RootPage */
    private long rootPageOffset;

    /** The offset of the next BTree */
    private long nextBtreeOffset;

    /** The number of elements in a page for this BTree */
    private long pageSize;

    /** The BTree name */
    private String name;

    /** The FQCN of the Key serializer */
    private String keySerializerFQCN;

    /** The FQCN of the Value serializer */
    private String valueSerializerFQCN;

    // Those are data which aren't serialized : they are in memory only */
    /** The position in the file */
    private long offset;

    /** The existing versions */
    private long[] versions;


    public BTreeHeader( String name )
    {
        this.name = name;
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
    public void setName( String name )
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
    public void setVersions( long[] versions )
    {
        this.versions = versions;
    }


    /**
     * @return the offset
     */
    public long getOffset()
    {
        return offset;
    }


    /**
     * @param offset the offset to set
     */
    public void setOffset( long offset )
    {
        this.offset = offset;
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
    public void setRootPageOffset( long rootPageOffset )
    {
        this.rootPageOffset = rootPageOffset;
    }


    /**
     * @return the revision
     */
    public long getRevision()
    {
        return revision;
    }


    /**
     * @param revision the revision to set
     */
    public void setRevision( long revision )
    {
        this.revision = revision;
    }


    /**
     * @return the nbElems
     */
    public long getNbElems()
    {
        return nbElems;
    }


    /**
     * @param nbElems the nbElems to set
     */
    public void setNbElems( long nbElems )
    {
        this.nbElems = nbElems;
    }


    /**
     * @return the nextBtreeOffset
     */
    public long getNextBtreeOffset()
    {
        return nextBtreeOffset;
    }


    /**
     * @param nextBtreeOffset the nextBtreeOffset to set
     */
    public void setNextBtreeOffset( long nextBtreeOffset )
    {
        this.nextBtreeOffset = nextBtreeOffset;
    }


    /**
     * @return the pageSize
     */
    public long getPageSize()
    {
        return pageSize;
    }


    /**
     * @param pageSize the pageSize to set
     */
    public void setPageSize( long pageSize )
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
    public void setKeySerializerFQCN( String keySerializerFQCN )
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
    public void setValueSerializerFQCN( String valueSerializerFQCN )
    {
        this.valueSerializerFQCN = valueSerializerFQCN;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Btree '" ).append( name ).append( "'" );
        sb.append( ", R[" ).append( revision ).append( "]" );
        sb.append( ", O[" ).append( offset ).append( "]\n" );
        sb.append( ", root[" ).append( rootPageOffset ).append( "]\n" );
        sb.append( ", next[" ).append( nextBtreeOffset ).append( "]\n" );
        sb.append( ", N[" ).append( nbElems ).append( "]" );
        sb.append( ", S[" ).append( pageSize ).append( "]" );
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
