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
 * Compares byte arrays.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ByteArrayComparator implements Comparator<byte[]>
{
    /** A static instance of a ByteArrayComparator */
    public static final ByteArrayComparator INSTANCE = new ByteArrayComparator();

    /**
     * A private constructor of the ByteArrayComparator class
     */
    private ByteArrayComparator()
    {
    }


    /**
     * Compare two byte arrays.
     *
     * @param byteArray1 First byteArray
     * @param byteArray2 Second byteArray
     * @return 1 if byteArray1 > byteArray2, 0 if byteArray1 == byteArray2, -1 if byteArray1 < byteArray2
     */
    public int compare( byte[] byteArray1, byte[] byteArray2 )
    {
        if ( byteArray1 == byteArray2 )
        {
            return 0;
        }

        if ( byteArray1 == null )
        {
            return -1;
        }
        else
        {
            if ( byteArray2 == null )
            {
                return 1;
            }
            else
            {
                if ( byteArray1.length < byteArray2.length )
                {
                    int pos = 0;

                    for ( byte b1 : byteArray1 )
                    {
                        byte b2 = byteArray2[pos];

                        if ( b1 == b2 )
                        {
                            pos++;
                        }
                        else if ( b1 < b2 )
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

                    for ( byte b2 : byteArray2 )
                    {
                        byte b1 = byteArray1[pos];

                        if ( b1 == b2 )
                        {
                            pos++;
                        }
                        else if ( b1 < b2 )
                        {
                            return -1;
                        }
                        else
                        {
                            return 1;
                        }
                    }

                    if ( pos < byteArray1.length )
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
