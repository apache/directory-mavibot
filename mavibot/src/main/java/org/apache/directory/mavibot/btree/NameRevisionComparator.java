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


import java.util.Comparator;


/**
 * A comparator for the RevisionName class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* no qualifier*/class NameRevisionComparator implements Comparator<NameRevision>
{
    /** A static instance of a NameRevisionComparator */
    public static final NameRevisionComparator INSTANCE = new NameRevisionComparator();

    /**
     * A private constructor of the NameRevisionComparator class
     */
    private NameRevisionComparator()
    {
    }


    /**
     * {@inheritDoc}
     */
    public int compare( NameRevision rn1, NameRevision rn2 )
    {
        if ( rn1 == rn2 )
        {
            return 0;
        }

        // First compare the name
        int comp =  rn1.getName().compareTo( rn2.getName() );

        if ( comp < 0 )
        {
            return -1;
        }
        else if ( comp > 0 )
        {
            return 1;
        }

        if ( rn1.getRevision() < rn2.getRevision() )
        {
            return -1;
        }
        else if ( rn1.getRevision() > rn2.getRevision() )
        {
            return 1;
        }

        // The name are equal : check the revision
        if ( rn1.getRevision() < rn2.getRevision() )
        {
            return -1;
        }
        else if ( rn1.getRevision() > rn2.getRevision() )
        {
            return 1;
        }

        return 0;
    }
}
