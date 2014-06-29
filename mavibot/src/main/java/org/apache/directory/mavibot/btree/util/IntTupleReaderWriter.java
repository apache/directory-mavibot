/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */
package org.apache.directory.mavibot.btree.util;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.directory.mavibot.btree.Tuple;


/**
 * TODO IntTupleReaderWriter.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class IntTupleReaderWriter implements TupleReaderWriter<Integer, Integer>
{

    @Override
    public void storeSortedTuple( Tuple<Integer, Integer> t, DataOutputStream out ) throws IOException
    {
        out.writeInt( t.getKey() );
        out.writeInt( t.getValue() );
    }


    @Override
    public Tuple<Integer, Integer> readSortedTuple( DataInputStream in )
    {
        return readUnsortedTuple( in );
    }


    @Override
    public Tuple<Integer, Integer> readUnsortedTuple( DataInputStream in )
    {

        try
        {
            if ( in.available() <= 0 )
            {
                return null;
            }

            Tuple<Integer, Integer> t = new Tuple<Integer, Integer>( in.readInt(), in.readInt() );

            return t;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

        return null;
    }
}
