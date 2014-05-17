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
 * Compares chars
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class CharComparator implements Comparator<Character>
{
    /** A static instance of a CharComparator */
    public static final CharComparator INSTANCE = new CharComparator();

    /**
     * A private constructor of the CharComparator class
     */
    private CharComparator()
    {
    }


    /**
     * Compare two chars.
     *
     * @param char1 First char
     * @param char2 Second char
     * @return 1 if char1 > char2, 0 if char1 == char2, -1 if char1 < char2
     */
    public int compare( Character char1, Character char2 )
    {
        if ( char1 == char2 )
        {
            return 0;
        }

        if ( char1 == null )
        {
            if ( char2 == null )
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
            if ( char2 == null )
            {
                return 1;
            }
            else
            {
                if ( char1 < char2 )
                {
                    return -1;
                }
                else if ( char1 > char2 )
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
