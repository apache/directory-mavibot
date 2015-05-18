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
package org.apache.directory.mavibot.btree.memory;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.util.TupleReaderWriter;


/**
 * A utility class for sorting a large number of keys before building a BTree using {@link InMemoryBTreeBuilder}.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BulkDataSorter<K, V>
{
    private File workDir;

    private int splitAfter = 1000;

    private Comparator<Tuple<K, V>> tupleComparator;

    private TupleReaderWriter<K, V> readerWriter;

    private boolean sorted;


    public BulkDataSorter( TupleReaderWriter<K, V> readerWriter, Comparator<Tuple<K, V>> tupleComparator,
        int splitAfter )
    {
        if ( splitAfter <= 0 )
        {
            throw new IllegalArgumentException( "Value of splitAfter parameter cannot be null" );
        }

        this.splitAfter = splitAfter;

        this.workDir = new File( System.getProperty( "java.io.tmpdir" ), System.currentTimeMillis() + "-sort" );
        workDir.mkdir();

        this.readerWriter = readerWriter;
        this.tupleComparator = tupleComparator;
    }


    public void sort( File dataFile ) throws IOException
    {
        int i = 0;

        Tuple<K, V>[] arr = ( Tuple<K, V>[] ) Array.newInstance( Tuple.class, splitAfter );

        Tuple<K, V> t = null;

        DataInputStream in = new DataInputStream( new FileInputStream( dataFile ) );

        while ( ( t = readerWriter.readUnsortedTuple( in ) ) != null )
        {
            arr[i++] = t;

            if ( ( i % splitAfter ) == 0 )
            {
                i = 0;
                Arrays.sort( arr, tupleComparator );

                storeSortedData( arr );
            }
        }

        if ( i != 0 )
        {
            Tuple<K, V>[] tmp = ( Tuple<K, V>[] ) Array.newInstance( Tuple.class, i );
            System.arraycopy( arr, 0, tmp, 0, i );
            Arrays.sort( tmp, tupleComparator );

            storeSortedData( tmp );
        }

        sorted = true;
    }


    private void storeSortedData( Tuple<K, V>[] arr ) throws IOException
    {
        File tempFile = File.createTempFile( UUID.randomUUID().toString(), ".batch", workDir );
        DataOutputStream out = new DataOutputStream( new FileOutputStream( tempFile ) );

        for ( Tuple<K, V> t : arr )
        {
            readerWriter.storeSortedTuple( t, out );
        }

        out.flush();
        out.close();
    }


    public File getWorkDir()
    {
        return workDir;
    }


    public Iterator<Tuple<K, V>> getMergeSortedTuples() throws IOException
    {
        if ( !sorted )
        {
            throw new IllegalStateException( "Data is not sorted" );
        }

        File[] batches = workDir.listFiles();

        if ( batches.length == 0 )
        {
            return Collections.EMPTY_LIST.iterator();
        }

        final DataInputStream[] streams = new DataInputStream[batches.length];

        for ( int i = 0; i < batches.length; i++ )
        {
            streams[i] = new DataInputStream( new FileInputStream( batches[i] ) );
        }

        Iterator<Tuple<K, V>> itr = new Iterator<Tuple<K, V>>()
        {
            private Tuple<K, V>[] heads = ( Tuple<K, V>[] ) Array.newInstance( Tuple.class, streams.length );

            private Tuple<K, V> candidate = null;

            private boolean closed;

            private int candidatePos = -1;


            @Override
            public boolean hasNext()
            {

                if ( closed )
                {
                    throw new IllegalStateException( "No elements to read" );
                }

                Tuple<K, V> available = null;

                for ( int i = 0; i < streams.length; i++ )
                {
                    if ( heads[i] == null )
                    {
                        heads[i] = readerWriter.readUnsortedTuple( streams[i] );
                    }

                    if ( available == null )
                    {
                        available = heads[i];
                        candidatePos = i;
                    }
                    else
                    {
                        if ( ( available != null ) && ( heads[i] != null ) )
                        {
                            int comp = tupleComparator.compare( heads[i], available );
                            if ( comp <= 0 )
                            {
                                available = heads[i];
                                candidatePos = i;
                            }
                        }
                    }
                }

                heads[candidatePos] = null;

                if ( available == null )
                {
                    for ( int i = 0; i < streams.length; i++ )
                    {
                        if ( heads[i] != null )
                        {
                            available = heads[i];
                            heads[i] = readerWriter.readUnsortedTuple( streams[i] );
                            break;
                        }
                    }
                }

                if ( available != null )
                {
                    candidate = available;
                    return true;
                }

                // finally close the streams
                for ( DataInputStream in : streams )
                {
                    try
                    {
                        in.close();
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                    }
                }

                closed = true;

                return false;
            }


            @Override
            public Tuple<K, V> next()
            {
                if ( candidate == null )
                {
                    if ( !closed )
                    {
                        hasNext();
                    }
                }

                if ( candidate == null )
                {
                    throw new NoSuchElementException( "No tuples found" );
                }

                return candidate;
            }


            @Override
            public void remove()
            {
                throw new UnsupportedOperationException( "Not supported" );
            }

        };

        return itr;
    }
}
