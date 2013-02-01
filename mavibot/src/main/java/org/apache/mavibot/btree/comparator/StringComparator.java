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
package org.apache.mavibot.btree.comparator;


import java.util.Comparator;


/**
 * Compares Strings
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class StringComparator implements Comparator<String>
{
    /**
     * Compare two Strings.
     * 
     * @param string1 First String
     * @param string2 Second String
     * @return 1 if string1 > String2, 0 if string1 == String2, -1 if string1 < String2
     */
    public int compare( String string1, String String2 )
    {
        if ( string1 == String2 )
        {
            return 0;
        }

        if ( string1 == null )
        {
            throw new IllegalArgumentException( "The first object to compare must not be null" );
        }

        if ( String2 == null )
        {
            throw new IllegalArgumentException( "The second object to compare must not be null" );
        }

        return string1.compareTo( String2 );
    }
}
