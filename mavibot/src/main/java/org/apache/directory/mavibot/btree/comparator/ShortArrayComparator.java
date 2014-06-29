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
 * Compares short arrays
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ShortArrayComparator implements Comparator<short[]>
{
    /** A static instance of a ShortArrayComparator */
    public static final ShortArrayComparator INSTANCE = new ShortArrayComparator();

    /**
     * A private constructor of the ShortArrayComparator class
     */
    private ShortArrayComparator()
    {
    }


    /**
     * Compare two short arrays.
     *
     * @param shortArray1 First short array
     * @param shortArray2 Second short array
     * @return 1 if shortArray1 > shortArray2, 0 if shortArray1 == shortArray2, -1 if shortArray1 < shortArray2
     */
    public int compare( short[] shortArray1, short[] shortArray2 )
    {
        if ( shortArray1 == shortArray2 )
        {
            return 0;
        }

        if ( shortArray1 == null )
        {
            if ( shortArray2 == null )
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
            if ( shortArray2 == null )
            {
                return 1;
            }
            else
            {
                if ( shortArray1.length < shortArray2.length )
                {
                    int pos = 0;

                    for ( short short1 : shortArray1 )
                    {
                        short short2 = shortArray2[pos];

                        if ( short1 == short2 )
                        {
                            pos++;
                        }
                        else if ( short1 < short2 )
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

                    for ( short short2 : shortArray2 )
                    {
                        short short1 = shortArray1[pos];

                        if ( short1 == short2 )
                        {
                            pos++;
                        }
                        else if ( short1 < short2 )
                        {
                            return -1;
                        }
                        else
                        {
                            return 1;
                        }
                    }

                    if ( pos < shortArray1.length )
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
