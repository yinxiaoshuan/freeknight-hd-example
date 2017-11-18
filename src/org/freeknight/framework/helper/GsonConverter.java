
package org.freeknight.framework.helper;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Google Gson类库解析转换器.
 * 
 * @author yrj
 *
 */
public final class GsonConverter
{
	private final static Gson	gson	= new GsonBuilder ( ).enableComplexMapKeySerialization ( )/* .setDateFormat("yyyy-MM-dd") */
																	/* .setPrettyPrinting ( ) */.create ( );

	/**
	 * 对象转换为JSON字符串.
	 * 
	 * @param src
	 * @return
	 */
	public static String toJson (
			final Object src )
	{
		return gson.toJson ( src );
	}

	/**
	 * JSON字符串转换为JavaBean对象.
	 * 
	 * @param json
	 * @param classOfT
	 * @return
	 */
	public static < T, X > T toBean (
			final String json, final Class< T > outer )
	{
		return toBean ( json, outer, Void.class );
	}

	/**
	 * JSON字符串转换为JavaBean对象.
	 * 
	 * @param json
	 * @param classOfT
	 * @return
	 */
	public static < T > T toBean (
			final String json, final Class< T > outer, final Class< ? > args )
	{
		return gson.fromJson ( json, getType ( outer, args ) );
	}

	/**
	 * 二进制流转换为JavaBean对象.
	 * 
	 * @param input
	 * @param clazz
	 * @return
	 */
	public static < T > T toBean (
			final InputStream input, final Class< T > clazz )
	{
		return gson.fromJson ( new InputStreamReader ( input ), clazz );
	}

	/**
	 * JSON字符串转换为List<T>对象.
	 * 
	 * @param json
	 * @param args
	 * @return
	 */
	public static < T > List< T > toList (
			final String json, final Class< T > args )
	{
		return gson.fromJson ( json, getType ( List.class, args ) );
	}

	/**
	 * JSON字符串转换为Map<X, T>对象.
	 * 
	 * @param json
	 * @param keyClass
	 * @param valueClass
	 * @return
	 */
	public static < X, T > Map< X, T > toMap (
			final String json, final Class< X > keyClass, final Class< T > valueClass )
	{
		return gson.fromJson ( json, getType ( Map.class, keyClass, valueClass ) );
	}

	private final static Type getType (
			final Class< ? > raw, final Class< ? >... actual )
	{
		return new ParameterizedType ( )
		{

			@Override
			public Type getRawType ( )
			{
				return raw;
			}

			@Override
			public Type getOwnerType ( )
			{
				return null;
			}

			@Override
			public Type[ ] getActualTypeArguments ( )
			{
				return actual;
			}
		};
	}

	private GsonConverter ( ) {
		throw new AssertionError ( "Uninstantiable class." );
	}

}
