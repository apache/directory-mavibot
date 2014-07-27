/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */
package org.apache.directory.mavibot.btree;


import java.util.Arrays;


/**
 * A class to hold name, revision, and copied page offsets of a B-Tree.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RevisionOffset
{
    /** the revision number */
    private long revision;

    /** offsets of copied pages */
    private long[] offsets;


    /**
     * Creates a new instance of RevisionOffset.
     *
     * @param revision the revision number
     * @param offsets array of copied page offsets
     */
    public RevisionOffset( long revision, long[] offsets )
    {
        this.revision = revision;
        this.offsets = offsets;
    }


    public long getRevision()
    {
        return revision;
    }


    /* no qualifier */void setRevision( long revision )
    {
        this.revision = revision;
    }


    public long[] getOffsets()
    {
        return offsets;
    }


    /* no qualifier */void setOffsets( long[] offsets )
    {
        this.offsets = offsets;
    }


    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( int ) ( revision ^ ( revision >>> 32 ) );
        return result;
    }


    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        
        if ( obj == null )
        {
            return false;
        }

        RevisionOffset other = ( RevisionOffset ) obj;

        if ( revision != other.revision )
        {
            return false;
        }

        return true;
    }


    @Override
    public String toString()
    {
        return "RevisionOffset [revision=" + revision + ", offsets=" + Arrays.toString( offsets ) + "]";
    }

}
