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

package org.apache.directory.mavibot.btree;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.junit.Test;


/**
 * Test cases for ManagedBTreeBuilder.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
//@Ignore("until ApacheDS works with mavibot")
public class BTreeBuilderTest
{

    @Test
    public void testManagedBTreeBuilding() throws Exception
    {
        List<Tuple<Integer, Integer>> sortedTuple = new ArrayList<Tuple<Integer, Integer>>();

        for ( int i = 1; i < 8; i++ )
        {
            Tuple<Integer, Integer> t = new Tuple<Integer, Integer>( i, i );
            sortedTuple.add( t );
        }

        File file = File.createTempFile( "managedbtreebuilder", ".data" );
        file.deleteOnExit();

        try
        {
            RecordManager recordManager = new RecordManager( file.getAbsolutePath() );

            IntSerializer ser = IntSerializer.INSTANCE;
            System.out.println( "Init" );
            System.out.println( "-----------------------------------------------------------" );

            MavibotInspector.dumpInfos( recordManager, recordManager.getCurrentRecordManagerHeader() );

            System.out.println( "-----------------------------------------------------------" );
            System.out.println();
            System.out.println( "Adding the master b-tree" );
            System.out.println( "-----------------------------------------------------------" );

            // contains 1, 2, 3, 4, 5, 6, 7
            BTree<Integer, Integer> btree = null;
            
            WriteTransaction writeTxn = null;
            
            try
            {
                writeTxn = recordManager.beginWriteTransaction();
                
                btree = recordManager.addBTree( writeTxn, "master", IntSerializer.INSTANCE, IntSerializer.INSTANCE );

                writeTxn.commit();
            }
            catch ( IOException ioe )
            {
                if ( writeTxn != null )
                {
                    writeTxn.abort();
                }
            }

            MavibotInspector.dumpInfos( recordManager, recordManager.getCurrentRecordManagerHeader() );
            
            System.out.println( "-----------------------------------------------------------" );
            System.out.println();
            System.out.println( "Inserting 7 values in master" );
            System.out.println( "-----------------------------------------------------------" );
            
            writeTxn = null;
            
            try
            {
                writeTxn = recordManager.beginWriteTransaction();
                
                for ( Tuple<Integer, Integer> tuple : sortedTuple )
                {
                    btree.insert( writeTxn, tuple.key, tuple.value );
                }
                
                //btree = bb.build( writeTxn, sortedTuple.iterator() );

                writeTxn.commit();
            }
            catch ( IOException ioe )
            {
                if ( writeTxn != null )
                {
                    writeTxn.abort();
                }
            }

            MavibotInspector.dumpInfos( recordManager, recordManager.getCurrentRecordManagerHeader() );
            
            System.out.println( "-----------------------------------------------------------" );
            recordManager.close();

            recordManager = new RecordManager( file.getAbsolutePath() );
            
            MavibotInspector.dumpInfos( recordManager, recordManager.getCurrentRecordManagerHeader() );
            
            Transaction readTxn = null;
            
            try
            {
                readTxn = recordManager.beginReadTransaction();
                
                btree = recordManager.getBtree( readTxn, "master" );
    
                assertEquals( 7, btree.getRootPage().getPageNbElems() );
    
                assertEquals( 7, btree.getRootPage().findRightMost( readTxn ).getKey().intValue() );
    
                assertEquals( 1, btree.getRootPage().findLeftMost( readTxn ).getKey().intValue() );
    
                TupleCursor<Integer, Integer> cursor = btree.browse( readTxn );
                int i = 0;
    
                while ( cursor.hasNext() )
                {
                    Tuple<Integer, Integer> expected = sortedTuple.get( i++ );
                    Tuple<Integer, Integer> actual = cursor.next();
                    assertEquals( expected.getKey(), actual.getKey() );
                    assertEquals( expected.getValue(), actual.getValue() );
                }
    
                cursor.close();

                readTxn.commit();
            }
            catch ( IOException ioe )
            {
                if ( readTxn != null )
                {
                    readTxn.abort();
                }
            }
            
            btree.close();
        }
        finally
        {
            file.delete();
        }
    }
}
