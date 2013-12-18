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
package org.apache.directory.mavibot.btree.util;


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Various string manipulation methods that are more efficient then chaining
 * string operations: all is done in the same buffer without creating a bunch of
 * string objects.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public final class Strings
{
    /** Hex chars */
    private static final byte[] HEX_CHAR = new byte[]
        { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    /** A empty byte array */
    public static final byte[] EMPTY_BYTES = new byte[0];


    /**
     * Helper function that dump an array of bytes in hex form
     *
     * @param buffer The bytes array to dump
     * @return A string representation of the array of bytes
     */
    public static String dumpBytes( byte[] buffer )
    {
        if ( buffer == null )
        {
            return "";
        }

        StringBuffer sb = new StringBuffer();

        for ( byte b : buffer )
        {
            sb.append( "0x" ).append( ( char ) ( HEX_CHAR[( b & 0x00F0 ) >> 4] ) ).append(
                ( char ) ( HEX_CHAR[b & 0x000F] ) ).append( " " );
        }

        return sb.toString();
    }


    /**
     * Helper function that dump a byte in hex form
     *
     * @param octet The byte to dump
     * @return A string representation of the byte
     */
    public static String dumpByte( byte octet )
    {
        return new String( new byte[]
            { '0', 'x', HEX_CHAR[( octet & 0x00F0 ) >> 4], HEX_CHAR[octet & 0x000F] } );
    }


    /**
     * Helper function that returns a char from an hex
     *
     * @param hex The hex to dump
     * @return A char representation of the hex
     */
    public static char dumpHex( byte hex )
    {
        return ( char ) HEX_CHAR[hex & 0x000F];
    }


    /**
     * Helper function that dump an array of bytes in hex pair form,
     * without '0x' and space chars
     *
     * @param buffer The bytes array to dump
     * @return A string representation of the array of bytes
     */
    public static String dumpHexPairs( byte[] buffer )
    {
        if ( buffer == null )
        {
            return "";
        }

        char[] str = new char[buffer.length << 1];

        int pos = 0;

        for ( byte b : buffer )
        {
            str[pos++] = ( char ) ( HEX_CHAR[( b & 0x00F0 ) >> 4] );
            str[pos++] = ( char ) ( HEX_CHAR[b & 0x000F] );
        }

        return new String( str );
    }


    /**
     * Gets a hex string from byte array.
     *
     * @param res the byte array
     * @return the hex string representing the binary values in the array
     */
    public static String toHexString( byte[] res )
    {
        StringBuffer buf = new StringBuffer( res.length << 1 );

        for ( byte b : res )
        {
            String digit = Integer.toHexString( 0xFF & b );

            if ( digit.length() == 1 )
            {
                digit = '0' + digit;
            }

            buf.append( digit );
        }

        return buf.toString().toUpperCase();
    }


    /**
     * Get byte array from hex string
     *
     * @param hexString the hex string to convert to a byte array
     * @return the byte form of the hex string.
     */
    public static byte[] toByteArray( String hexString )
    {
        int arrLength = hexString.length() >> 1;
        byte[] buf = new byte[arrLength];

        for ( int ii = 0; ii < arrLength; ii++ )
        {
            int index = ii << 1;

            String digit = hexString.substring( index, index + 2 );
            buf[ii] = ( byte ) Integer.parseInt( digit, 16 );
        }

        return buf;
    }

    private static final byte[] UTF8 = new byte[]
        { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
            0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C,
            0x1D, 0x1E, 0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E,
            0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F, 0x40,
            0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52,
            0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F, 0x60, 0x61, 0x62, 0x63, 0x64,
            0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
            0x77, 0x78, 0x79, 0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F };


    /**
     * Return an UTF-8 encoded String
     *
     * @param bytes The byte array to be transformed to a String
     * @return A String.
     */
    public static String utf8ToString( byte[] bytes )
    {
        if ( bytes == null )
        {
            return "";
        }

        char[] chars = new char[bytes.length];
        int pos = 0;

        try
        {
            for ( byte b : bytes )
            {
                chars[pos++] = ( char ) UTF8[b];
            }
        }
        catch ( ArrayIndexOutOfBoundsException aioobe )
        {
            try
            {
                return new String( bytes, "UTF-8" );
            }
            catch ( UnsupportedEncodingException uee )
            {
                // if this happens something is really strange
                throw new RuntimeException( uee );
            }
        }

        return new String( chars );
    }


    /**
     * Return an UTF-8 encoded String
     *
     * @param bytes The byte array to be transformed to a String
     * @return A String.
     */
    public static String utf8ToString( ByteBuffer bytes )
    {
        if ( bytes == null )
        {
            return "";
        }

        char[] chars = new char[bytes.limit()];
        int pos = 0;
        int currentPos = bytes.position();

        do
        {
            chars[pos++] = ( char ) UTF8[bytes.get()];
        }
        while ( bytes.position() < bytes.limit() );

        // restore the buffer
        bytes.position( currentPos );

        return new String( chars );
    }


    /**
     * Return an UTF-8 encoded String
     *
     * @param bytes The byte array to be transformed to a String
     * @param length The length of the byte array to be converted
     * @return A String.
     */
    public static String utf8ToString( byte[] bytes, int length )
    {
        if ( bytes == null )
        {
            return "";
        }

        try
        {
            return new String( bytes, 0, length, "UTF-8" );
        }
        catch ( UnsupportedEncodingException uee )
        {
            // if this happens something is really strange
            throw new RuntimeException( uee );
        }
    }


    /**
     * Return an UTF-8 encoded String
     *
     * @param bytes  The byte array to be transformed to a String
     * @param start the starting position in the byte array
     * @param length The length of the byte array to be converted
     * @return A String.
     */
    public static String utf8ToString( byte[] bytes, int start, int length )
    {
        if ( bytes == null )
        {
            return "";
        }

        try
        {
            return new String( bytes, start, length, "UTF-8" );
        }
        catch ( UnsupportedEncodingException uee )
        {
            // if this happens something is really strange
            throw new RuntimeException( uee );
        }
    }


    /**
     * <p>
     * Checks if a String is empty ("") or null.
     * </p>
     *
     * <pre>
     *  StringUtils.isEmpty(null)      = true
     *  StringUtils.isEmpty(&quot;&quot;)        = true
     *  StringUtils.isEmpty(&quot; &quot;)       = false
     *  StringUtils.isEmpty(&quot;bob&quot;)     = false
     *  StringUtils.isEmpty(&quot;  bob  &quot;) = false
     * </pre>
     *
     * <p>
     * NOTE: This method changed in Lang version 2.0. It no longer trims the
     * String. That functionality is available in isBlank().
     * </p>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if the String is empty or null
     */
    public static boolean isEmpty( String str )
    {
        return ( str == null ) || ( str.length() == 0 );
    }


    /**
     * Checks if a bytes array is empty or null.
     *
     * @param bytes The bytes array to check, may be null
     * @return <code>true</code> if the bytes array is empty or null
     */
    public static boolean isEmpty( byte[] bytes )
    {
        return ( bytes == null ) || ( bytes.length == 0 );
    }


    /**
     * Return UTF-8 encoded byte[] representation of a String
     *
     * @param string The string to be transformed to a byte array
     * @return The transformed byte array
     */
    public static byte[] getBytesUtf8( String string )
    {
        if ( string == null )
        {
            return EMPTY_BYTES;
        }

        try
        {
            return string.getBytes( "UTF-8" );
        }
        catch ( UnsupportedEncodingException uee )
        {
            // if this happens something is really strange
            throw new RuntimeException( uee );
        }
    }


    /**
     * When the string to convert to bytes is pure ascii, this is a faster 
     * method than the getBytesUtf8. Otherwise, it's slower.
     * 
     * @param string The string to convert to byte[]
     * @return The bytes 
     */
    public static byte[] getBytesUtf8Ascii( String string )
    {
        if ( string == null )
        {
            return new byte[0];
        }

        try
        {
            try
            {
                char[] chars = string.toCharArray();
                byte[] bytes = new byte[chars.length];
                int pos = 0;

                for ( char c : chars )
                {
                    bytes[pos++] = UTF8[c];
                }

                return bytes;
            }
            catch ( ArrayIndexOutOfBoundsException aioobe )
            {
                return string.getBytes( "UTF-8" );
            }
        }
        catch ( UnsupportedEncodingException uee )
        {
            // if this happens something is really strange
            throw new RuntimeException( uee );
        }
    }


    /**
     * Utility method that return a String representation of a list
     *
     * @param list The list to transform to a string
     * @return A csv string
     */
    public static String listToString( List<?> list )
    {
        if ( ( list == null ) || ( list.size() == 0 ) )
        {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;

        for ( Object elem : list )
        {
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                sb.append( ", " );
            }

            sb.append( elem );
        }

        return sb.toString();
    }


    /**
     * Utility method that return a String representation of a set
     *
     * @param set The set to transform to a string
     * @return A csv string
     */
    public static String setToString( Set<?> set )
    {
        if ( ( set == null ) || ( set.size() == 0 ) )
        {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;

        for ( Object elem : set )
        {
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                sb.append( ", " );
            }

            sb.append( elem );
        }

        return sb.toString();
    }


    /**
     * Utility method that return a String representation of a list
     *
     * @param list The list to transform to a string
     * @param tabs The tabs to add in ffront of the elements
     * @return A csv string
     */
    public static String listToString( List<?> list, String tabs )
    {
        if ( ( list == null ) || ( list.size() == 0 ) )
        {
            return "";
        }

        StringBuffer sb = new StringBuffer();

        for ( Object elem : list )
        {
            sb.append( tabs );
            sb.append( elem );
            sb.append( '\n' );
        }

        return sb.toString();
    }


    /**
     * Utility method that return a String representation of a map. The elements
     * will be represented as "key = value"
     *
     * @param map The map to transform to a string
     * @return A csv string
     */
    public static String mapToString( Map<?, ?> map )
    {
        if ( ( map == null ) || ( map.size() == 0 ) )
        {
            return "";
        }

        StringBuffer sb = new StringBuffer();
        boolean isFirst = true;

        for ( Map.Entry<?, ?> entry : map.entrySet() )
        {
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                sb.append( ", " );
            }

            sb.append( entry.getKey() );
            sb.append( " = '" ).append( entry.getValue() ).append( "'" );
        }

        return sb.toString();
    }


    /**
     * Utility method that return a String representation of a map. The elements
     * will be represented as "key = value"
     *
     * @param map The map to transform to a string
     * @param tabs The tabs to add in ffront of the elements
     * @return A csv string
     */
    public static String mapToString( Map<?, ?> map, String tabs )
    {
        if ( ( map == null ) || ( map.size() == 0 ) )
        {
            return "";
        }

        StringBuffer sb = new StringBuffer();

        for ( Map.Entry<?, ?> entry : map.entrySet() )
        {
            sb.append( tabs );
            sb.append( entry.getKey() );

            sb.append( " = '" ).append( entry.getValue().toString() ).append( "'\n" );
        }

        return sb.toString();
    }
}
