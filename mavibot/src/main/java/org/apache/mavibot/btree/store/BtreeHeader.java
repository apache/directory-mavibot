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
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class BtreeHeader
{
    /** The BTree name */
    private String name;

    /** The current version */
    private long version;

    /** The existing versions */
    private long[] versions;

    /** The position in the file */
    private long offset;

    /** The FQCN of the Key serializer */
    private String keySerializerFQCN;

    /** The FQCN of the Key comparator */
    private String keyComparatorFQCN;

    /** The FQCN of the Value serializer */
    private String valueSerializerFQCN;

    /** The FQCN of the Value serializer */
    private String valueComparatorFQCN;


    public BtreeHeader( String name )
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
     * @return the version
     */
    public long getVersion()
    {
        return version;
    }


    /**
     * @param version the version to set
     */
    public void setVersion( long version )
    {
        this.version = version;
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
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "Btree '" ).append( name ).append( "'" );
        sb.append( ", V[" ).append( version ).append( "]" );
        sb.append( ", O[" ).append( offset ).append( "]\n" );
        sb.append( "{\n" );
        sb.append( "    Key serializer   : " ).append( keySerializerFQCN ).append( "\n" );
        sb.append( "    Value serializer : " ).append( valueSerializerFQCN ).append( "\n" );
        sb.append( "    Key comparator   : " ).append( keyComparatorFQCN ).append( "\n" );
        sb.append( "    Value comparator : " ).append( valueComparatorFQCN ).append( "\n" );
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
