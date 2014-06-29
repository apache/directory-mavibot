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
package org.apache.directory.mavibot.btree.comparator;


import java.util.Comparator;


/**
 * Compares Longs
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class LongComparator implements Comparator<Long>
{
    /** A static instance of a LongComparator */
    public static final LongComparator INSTANCE = new LongComparator();

    /**
     * A private constructor of the BooleanArrayComparator class
     */
    private LongComparator()
    {
    }
    /**
     * Compare two longs.
     *
     * @param long1 First long
     * @param long2 Second long
     * @return 1 if long1 > long2, 0 if long1 == long2, -1 if long1 < long2
     */
    public int compare( Long long1, Long long2 )
    {
        if ( long1 == long2 )
        {
            return 0;
        }

        if ( long1 == null )
        {
            if ( long2 == null )
            {
                return 0;
            }
            else
            {
                return -1;
            }
        }
        else
        {
            if ( long2 == null )
            {
                return 1;
            }
            else
            {
                if ( long1 < long2 )
                {
                    return -1;
                }
                else if ( long1 > long2 )
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
        }
    }
}
