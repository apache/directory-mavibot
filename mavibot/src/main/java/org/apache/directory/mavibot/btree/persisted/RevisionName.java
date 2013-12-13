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
package org.apache.directory.mavibot.btree.persisted;


/**
 * A data structure that stores a revision associated to a BTree name. We use
 * it to allow the access to old revisions.
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RevisionName
{
    /** The revision number on the BTree */
    private long revision;

    /** The BTree name */
    private String name;


    /**
     * A constructor for the RevisionName class
     * @param revision The revision
     * @param name The BTree name
     */
    public RevisionName( long revision, String name )
    {
        this.revision = revision;
        this.name = name;
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
     * @return the btree name
     */
    public String getName()
    {
        return name;
    }


    /**
     * @param name the btree name to set
     */
    public void setName( String name )
    {
        this.name = name;
    }


    /**
     * @see Object#equals(Object)
     */
    public boolean equals( Object that )
    {
        if ( this == that )
        {
            return true;
        }

        if ( !( that instanceof RevisionName ) )
        {
            return false;
        }

        RevisionName revisionName = ( RevisionName ) that;

        if ( revision != revisionName.revision )
        {
            return false;
        }

        if ( name == null )
        {
            return revisionName.name == null;
        }

        return ( name.equals( revisionName.name ) );

    }


    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( name == null ) ? 0 : name.hashCode() );
        result = prime * result + ( int ) ( revision ^ ( revision >>> 32 ) );
        return result;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "[" + name + ":" + revision + "]";
    }
}
