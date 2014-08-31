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
 * A comparator for the RevisionAndHeaderOffset class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* no qualifier*/class RevisionAndHeaderOffsetComparator implements Comparator<RevisionAndHeaderOffset>
{
    /** A static instance of a RevisionOffsetComparator */
    public static final RevisionAndHeaderOffsetComparator INSTANCE = new RevisionAndHeaderOffsetComparator();

    public static final RevisionAndHeaderOffsetComparator INSTANCE_DESC_ORDER = new RevisionAndHeaderOffsetComparator( true );
    
    private boolean desc;
    
    /**
     * A private constructor of the RevisionOffsetComparator class
     */
    private RevisionAndHeaderOffsetComparator()
    {
    }

    
    private RevisionAndHeaderOffsetComparator( boolean desc )
    {
        this.desc = desc;
    }

    
    /**
     * {@inheritDoc}
     */
    public int compare( RevisionAndHeaderOffset rn1, RevisionAndHeaderOffset rn2 )
    {
        if ( rn1 == rn2 )
        {
            return 0;
        }

        // the RevisionOffset will never be used as a key
        
        // First compare the revisions
        if ( rn1.getRevision() < rn2.getRevision() )
        {
            return desc ? 1 : -1;
        }
        else if ( rn1.getRevision() > rn2.getRevision() )
        {
            return desc ? -1 : 1;
        }
        
        // ignore the offset
        return 0;
    }
}
