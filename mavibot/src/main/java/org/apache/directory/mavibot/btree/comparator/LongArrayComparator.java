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
 * Compares long arrays
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class LongArrayComparator implements Comparator<long[]>
{
    /** A static instance of a LongArrayComparator */
    public static final LongArrayComparator INSTANCE = new LongArrayComparator();

    /**
     * A private constructor of the LongArrayComparator class
     */
    private LongArrayComparator()
    {
    }


    /**
     * Compare two long arrays.
     *
     * @param longArray1 First long array
     * @param longArray2 Second long array
     * @return 1 if longArray1 > longArray2, 0 if longArray1 == longArray2, -1 if longArray1 < longArray2
     */
    public int compare( long[] longArray1, long[] longArray2 )
    {
        if ( longArray1 == longArray2 )
        {
            return 0;
        }

        if ( longArray1 == null )
        {
            if ( longArray2 == null )
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
            if ( longArray2 == null )
            {
                return 1;
            }
            else
            {
                if ( longArray1.length < longArray2.length )
                {
                    int pos = 0;

                    for ( long long1 : longArray1 )
                    {
                        long long2 = longArray2[pos];

                        if ( long1 == long2 )
                        {
                            pos++;
                        }
                        else if ( long1 < long2 )
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

                    for ( long long2 : longArray2 )
                    {
                        long long1 = longArray1[pos];

                        if ( long1 == long2 )
                        {
                            pos++;
                        }
                        else if ( long1 < long2 )
                        {
                            return -1;
                        }
                        else
                        {
                            return 1;
                        }
                    }

                    if ( pos < longArray1.length )
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
