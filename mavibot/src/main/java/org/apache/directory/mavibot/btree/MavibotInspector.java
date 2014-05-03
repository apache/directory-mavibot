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


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Set;

import org.apache.directory.mavibot.btree.serializer.StringSerializer;


/**
 * A class to examine a Mavibot database file.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class MavibotInspector
{
    private File dbFile;

    private RecordManager rm;

    private BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );


    public MavibotInspector( File dbFile )
    {
        this.dbFile = dbFile;
    }


    boolean checkFilePresence()
    {
        if ( dbFile == null )
        {
            System.out.println( "No mavibot database file was given" );
            return false;
        }

        if ( !dbFile.exists() )
        {
            System.out.println( "Given mavibot database file " + dbFile + " doesn't exist" );
            return false;
        }

        return true;
    }


    public void printFileSize() throws IOException
    {
        FileChannel fileChannel = new RandomAccessFile( dbFile, "r" ).getChannel();

        long l = fileChannel.size();

        fileChannel.close();

        String msg;

        if ( l < 1024 )
        {
            msg = l + " bytes";
        }
        else
        {
            msg = ( l / 1024 ) + " KB";
        }

        System.out.println( msg );
    }


    public void printNumberOfBTrees()
    {
        int nbBtrees = 0;
        if ( rm != null )
        {
            nbBtrees = rm.getNbManagedTrees();
        }

        // The number of trees. It must be at least 2 and > 0
        System.out.println( "Total Number of BTrees: " + nbBtrees );
    }


    public void printBTreeNames()
    {
        if ( rm == null )
        {
            System.out.println( "Couldn't find the number of managed btrees" );
            return;
        }

        Set<String> trees = rm.getManagedTrees();
        System.out.println( "\nManaged BTrees:" );
        for ( String t : trees )
        {
            System.out.println( t );
        }
        System.out.println();
    }


    public void checkBTree()
    {
        if ( rm == null )
        {
            System.out.println( "Cannot check BTree(s)" );
            return;
        }

        System.out.print( "BTree Name: " );
        String name = readLine();

        PersistedBTree pb = ( PersistedBTree ) rm.getManagedTree( name );

        if ( pb == null )
        {
            System.out.println( "No BTree exists with the name '" + name + "'" );
            return;
        }

        System.out.println( "\nBTree offset: " + pb.getBtreeOffset() );
        System.out.println( "BTree _info_ offset: " + pb.getBtreeInfoOffset() );
        System.out.println( "BTree root page offset: " + pb.getRootPageOffset() );
        System.out.println( "Number of elements present: " + pb.getNbElems() );
        System.out.println( "BTree Page size: " + pb.getPageSize() );
        System.out.println( "BTree revision: " + pb.getRevision() );
        System.out.println( "Key serializer: " + pb.getKeySerializerFQCN() );
        System.out.println( "Value serializer: " + pb.getValueSerializerFQCN() );
        System.out.println();

    }


    private boolean loadRm()
    {
        try
        {
            if( rm != null )
            {
                System.out.println("Closing record manager");
                rm.close();
            }
            
            rm = new RecordManager( dbFile.getAbsolutePath() );
            System.out.println("Loaded record manager");
        }
        catch ( Exception e )
        {
            System.out.println( "Given database file seems to be corrupted. " + e.getMessage() );
            return false;
        }

        return true;
    }


    public void start() throws Exception
    {
        if ( !checkFilePresence() )
        {
            return;
        }

        if ( !loadRm() )
        {
            return;
        }

        boolean stop = false;

        while ( !stop )
        {
            System.out.println( "Choose an option:" );
            System.out.println( "1. Print Number of BTrees" );
            System.out.println( "2. Print BTree Names" );
            System.out.println( "3. Inspect BTree" );
            System.out.println( "4. Check Free Pages" );
            System.out.println( "5. Get database file size" );
            System.out.println( "6. Dump RecordManager" );
            System.out.println( "7. Reload RecordManager" );
            System.out.println( "q. Quit" );

            char c = readOption();

            switch ( c )
            {
                case '1':
                    printNumberOfBTrees();
                    break;

                case '2':
                    printBTreeNames();
                    break;

                case '3':
                    checkBTree();
                    break;

                case '4':
                    rm.checkFreePages();
                    break;

                case '5':
                    printFileSize();
                    break;

                case '6':
                    rm.check();
                    break;

                case '7':
                    loadRm();
                    break;

                case 'q':
                    stop = true;
                    break;

                default:
                    System.out.println( "Invalid option" );
                    //c = readOption( br );
                    break;
            }
        }

        try
        {
            rm.close();
            br.close();
        }
        catch ( Exception e )
        {
            //ignore
        }
    }


    private String readLine()
    {
        try
        {
            return br.readLine().trim();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    private char readOption()
    {
        try
        {
            String s = br.readLine();

            if ( s.length() == 0 )
            {
                return ' ';
            }

            return s.charAt( 0 );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    public static void main( String[] args ) throws Exception
    {
        File f = new File( "/tmp/mavibotispector.db" );

        RecordManager rm = new RecordManager( f.getAbsolutePath() );
        String name = "corpus";
        
        if ( !rm.getManagedTrees().contains( name ) )
        {
            rm.addBTree( name, StringSerializer.INSTANCE, StringSerializer.INSTANCE, true );
        }

        BTree btree = rm.getManagedTree( name );
        
        for ( int i = 0; i < 10; i++ )
        {
            btree.insert( "" + i, "" + i );
        }

        rm.close();

        MavibotInspector mi = new MavibotInspector( f );
        mi.start();
    }
}
