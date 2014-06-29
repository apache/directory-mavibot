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
 * Compares Strings
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class StringComparator implements Comparator<String>
{
    /** A static instance of a StringComparator */
    public static final StringComparator INSTANCE = new StringComparator();

    /**
     * A private constructor of the StringComparator class
     */
    private StringComparator()
    {
    }


    /**
     * Compare two Strings.
     *
     * @param string1 First String
     * @param string2 Second String
     * @return 1 if string1 > String2, 0 if string1 == String2, -1 if string1 < String2
     */
    public int compare( String string1, String string2 )
    {
        if ( string1 == string2 )
        {
            return 0;
        }

        if ( string1 == null )
        {
            return -1;
        }
        else if ( string2 == null )
        {
            return 1;
        }

        int result = string1.compareTo( string2 );

        if ( result < 0 )
        {
            return -1;
        }
        else if ( result > 0 )
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }
}
