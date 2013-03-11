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
package org.apache.mavibot.btree.store;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.mavibot.btree.BTree;
import org.apache.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.mavibot.btree.serializer.LongSerializer;
import org.apache.mavibot.btree.serializer.StringSerializer;
import org.junit.Test;


/**
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class RecordManagerTest
{

    @Test
    public void testRecordManager() throws IOException, BTreeAlreadyManagedException
    {
        File tempFile = File.createTempFile( "mavibot", ".db" );
        String tempFileName = tempFile.getAbsolutePath();
        tempFile.deleteOnExit();

        RecordManager recordManager = new RecordManager( tempFileName, 32 );

        assertNotNull( recordManager );

        // Create a new BTree
        BTree<Long, String> btree = new BTree<Long, String>( "test", new LongSerializer(), new StringSerializer() );

        // And make it managed by the RM
        recordManager.manage( btree );

        // Close the recordManager
        recordManager.close();

        // Now, try to reload the file back
        RecordManager recordManager1 = new RecordManager( tempFileName );

        assertEquals( 1, recordManager1.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager1.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        BTree btree1 = recordManager1.getManagedTree( "test" );

        assertNotNull( btree1 );
        assertEquals( btree.getComparator().getClass().getName(), btree1.getComparator().getClass().getName() );
        assertEquals( btree.getFile(), btree1.getFile() );
        assertEquals( btree.getKeySerializer().getClass().getName(), btree1.getKeySerializer().getClass().getName() );
        assertEquals( btree.getName(), btree1.getName() );
        assertEquals( btree.getNbElems(), btree1.getNbElems() );
        assertEquals( btree.getPageSize(), btree1.getPageSize() );
        assertEquals( btree.getRevision(), btree1.getRevision() );
        assertEquals( btree.getValueSerializer().getClass().getName(), btree1.getValueSerializer().getClass().getName() );
    }
}
