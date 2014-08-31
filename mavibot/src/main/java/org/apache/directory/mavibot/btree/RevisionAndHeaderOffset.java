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
 * A class to hold revision and btree offset of a B-Tree.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RevisionAndHeaderOffset
{
    /** the revision number */
    private long revision;

    /** the btree offset of the given revision */
    private long offset;


    /**
     * Creates a new instance of RevisionAndHeaderOffset.
     *
     * @param revision the revision number
     * @param offset btree header offset
     */
    public RevisionAndHeaderOffset( long revision, long offset )
    {
        this.revision = revision;
        this.offset = offset;
    }


    public long getRevision()
    {
        return revision;
    }


    /* no qualifier */void setRevision( long revision )
    {
        this.revision = revision;
    }


    public long getOffset()
    {
        return offset;
    }


    /* no qualifier */void setOffset( long offset )
    {
        this.offset = offset;
    }


    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( int ) ( offset ^ ( offset >>> 32 ) );
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

        RevisionAndHeaderOffset other = ( RevisionAndHeaderOffset ) obj;

        if ( offset != other.offset )
        {
            return false;
        }

        if ( revision != other.revision )
        {
            return false;
        }

        return true;
    }


    @Override
    public String toString()
    {
        return "RevisionAndHeaderOffset [revision=" + revision + ", offset=" + offset + "]";
    }

}
