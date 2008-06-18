/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.util;

/**
 * 
 * A ShortString
 * 
 * A simple String class that only stores single byte characters, and stores as simple byte[],
 * this minimises expensive copying between String objects
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ShortString
{
	private final byte[] data;
	
	public ShortString(final String string)
	{
		int len = string.length();
		
		data = new byte[len];
		
		for (int i = 0; i < len; i++)
		{
			char c = string.charAt(i);
			
			if (c > 0xFF)
			{
				throw new IllegalArgumentException("Cannot encode string - contains multi-byte character(s)");
			}
			
			data[i] = (byte)c;  // low byte
		}
	}
	
	public ShortString(final byte[] data)
	{
		this.data = data;
	}
	
	public byte[] getData()
	{
		return data;
	}
	
	public String asString() throws Exception
	{
		int len = data.length;
		
		char[] chars = new char[len];
		
		for (int i = 0; i < len; i++)
		{
			chars[i] = (char)data[i];
		}
		
		return new String(chars);
	}
}
