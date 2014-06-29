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


/**
 * A data structure that stores a Btree name associated with a revision. We use
 * it to manage Btree of Btrees.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* no qualifier*/class NameRevision extends Tuple<String, Long>
{
    /**
     * A constructor for the RevisionName class
     * @param revision The revision
     * @param name The BTree name
     */
    /* no qualifier*/NameRevision( String name, long revision )
    {
        super( name, revision );
    }


    /**
     * @return the revision
     */
    /* no qualifier*/long getRevision()
    {
        return getValue();
    }


    /**
     * @param revision the revision to set
     */
    /* no qualifier*/void setRevision( long revision )
    {
        setValue( revision );
    }


    /**
     * @return the btree name
     */
    /* no qualifier*/String getName()
    {
        return getKey();
    }


    /**
     * @param name the btree name to set
     */
    /* no qualifier*/void setName( String name )
    {
        setKey( name );
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

        if ( !( that instanceof NameRevision ) )
        {
            return false;
        }

        NameRevision revisionName = ( NameRevision ) that;

        if ( getRevision() != revisionName.getRevision() )
        {
            return false;
        }

        if ( getName() == null )
        {
            return revisionName.getName() == null;
        }

        return ( getName().equals( revisionName.getName() ) );

    }


    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( getName() == null ) ? 0 : getName().hashCode() );
        result = prime * result + ( int ) ( getRevision() ^ ( getRevision() >>> 32 ) );
        return result;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "[" + getName() + ":" + getRevision() + "]";
    }
}
