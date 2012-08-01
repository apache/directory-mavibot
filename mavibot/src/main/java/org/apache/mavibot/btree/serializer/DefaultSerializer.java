package org.apache.mavibot.btree.serializer;


import java.util.HashMap;
import java.util.Map;


/**
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class DefaultSerializer<K, V> implements Serializer<K, V>
{
    /** The type to use to create the keys */
    private Class<K> keyType;

    /** The type to use to create the values */
    private Class<V> valueType;

    /** A mapping to retrive the default element serializers */
    private Map<Class<?>, ElementSerializer<?>> typeClass = new HashMap<Class<?>, ElementSerializer<?>>();


    /**
     * Create a default serializer, using the default ElementSerializer we have already created
     */
    public DefaultSerializer( Class<K> keyType, Class<V> valueType )
    {
        this.keyType = keyType;
        this.valueType = valueType;

        typeClass.put( Byte.class, new ByteSerializer() );
        typeClass.put( Character.class, new CharSerializer() );
        typeClass.put( Short.class, new ShortSerializer() );
        typeClass.put( Integer.class, new IntSerializer() );
        typeClass.put( Long.class, new LongSerializer() );
        typeClass.put( Boolean.class, new BooleanSerializer() );
        typeClass.put( String.class, new StringSerializer() );
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serializeKey( K key )
    {
        ElementSerializer<K> serializer = ( ElementSerializer<K> ) typeClass.get( keyType );

        if ( serializer != null )
        {
            byte[] result = serializer.serialize( key );

            return result;
        }
        else
        {
            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    public K deserializeKey( byte[] in )
    {
        ElementSerializer<K> serializer = ( ElementSerializer<K> ) typeClass.get( keyType );

        if ( serializer != null )
        {
            K key = serializer.deserialize( in );

            return key;
        }
        else
        {
            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    public byte[] serializeValue( V value )
    {
        ElementSerializer<V> serializer = ( ElementSerializer<V> ) typeClass.get( valueType );

        if ( serializer != null )
        {
            byte[] result = serializer.serialize( value );

            return result;
        }
        else
        {
            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    public V deserializeValue( byte[] in )
    {
        ElementSerializer<V> serializer = ( ElementSerializer<V> ) typeClass.get( valueType );

        if ( serializer != null )
        {
            V value = serializer.deserialize( in );

            return value;
        }
        else
        {
            return null;
        }
    }
}
