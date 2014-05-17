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
 * Compares shorts
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ShortComparator implements Comparator<Short>
{
    /** A static instance of a ShortComparator */
    public static final ShortComparator INSTANCE = new ShortComparator();

    /**
     * A private constructor of the ShortComparator class
     */
    private ShortComparator()
    {
    }


    /**
     * Compare two shorts.
     *
     * @param short1 First short
     * @param short2 Second short
     * @return 1 if short1 > short2, 0 if short1 == short2, -1 if short1 < short2
     */
    public int compare( Short short1, Short short2 )
    {
        if ( short1 == short2 )
        {
            return 0;
        }

        if ( short1 == null )
        {
            if ( short2 == null )
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
            if ( short2 == null )
            {
                return 1;
            }
            else
            {
                if ( short1 < short2 )
                {
                    return -1;
                }
                else if ( short1 > short2 )
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
