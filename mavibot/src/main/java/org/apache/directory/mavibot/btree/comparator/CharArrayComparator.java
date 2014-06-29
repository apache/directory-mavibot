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
 * Compares char arrays
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class CharArrayComparator implements Comparator<char[]>
{
    /** A static instance of a CharArrayComparator */
    public static final CharArrayComparator INSTANCE = new CharArrayComparator();

    /**
     * A private constructor of the CharArrayComparator class
     */
    private CharArrayComparator()
    {
    }


    /**
     * Compare two char arrays.
     *
     * @param charArray1 First char array
     * @param charArray2 Second char array
     * @return 1 if charArray1 > charArray2, 0 if charArray1 == charArray2, -1 if charArray1 < charArray2
     */
    public int compare( char[] charArray1, char[] charArray2 )
    {
        if ( charArray1 == charArray2 )
        {
            return 0;
        }

        if ( charArray1 == null )
        {
            if ( charArray2 == null )
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
            if ( charArray2 == null )
            {
                return 1;
            }
            else
            {
                if ( charArray1.length < charArray2.length )
                {
                    int pos = 0;

                    for ( char char1 : charArray1 )
                    {
                        char char2 = charArray2[pos];

                        if ( char1 == char2 )
                        {
                            pos++;
                        }
                        else if ( char1 < char2 )
                        {
                            return -1;
                        }
                        else
                        {
                            return 1;
                        }
                    }

                    return -1;
                }
                else
                {
                    int pos = 0;

                    for ( char char2 : charArray2 )
                    {
                        char char1 = charArray1[pos];

                        if ( char1 == char2 )
                        {
                            pos++;
                        }
                        else if ( char1 < char2 )
                        {
                            return -1;
                        }
                        else
                        {
                            return 1;
                        }
                    }

                    if ( pos < charArray1.length )
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
}
