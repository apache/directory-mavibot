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
 * Compares int arrays
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class IntArrayComparator implements Comparator<int[]>
{
    /** A static instance of a IntArrayComparator */
    public static final IntArrayComparator INSTANCE = new IntArrayComparator();

    /**
     * A private constructor of the IntArrayComparator class
     */
    private IntArrayComparator()
    {
    }


    /**
     * Compare two long arrays.
     *
     * @param intArray1 First int array
     * @param intArray2 Second int array
     * @return 1 if intArray1 > intArray2, 0 if intArray1 == intArray2, -1 if intArray1 < intArray2
     */
    public int compare( int[] intArray1, int[] intArray2 )
    {
        if ( intArray1 == intArray2 )
        {
            return 0;
        }

        if ( intArray1 == null )
        {
            if ( intArray2 == null )
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
            if ( intArray2 == null )
            {
                return 1;
            }
            else
            {
                if ( intArray1.length < intArray2.length )
                {
                    int pos = 0;

                    for ( int int1 : intArray1 )
                    {
                        int int2 = intArray2[pos];

                        if ( int1 == int2 )
                        {
                            pos++;
                        }
                        else if ( int1 < int2 )
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

                    for ( int int2 : intArray2 )
                    {
                        int int1 = intArray1[pos];

                        if ( int1 == int2 )
                        {
                            pos++;
                        }
                        else if ( int1 < int2 )
                        {
                            return -1;
                        }
                        else
                        {
                            return 1;
                        }
                    }

                    if ( pos < intArray1.length )
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
