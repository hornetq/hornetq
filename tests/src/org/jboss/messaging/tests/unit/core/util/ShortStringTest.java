/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.tests.unit.core.util;

import junit.framework.TestCase;

import org.jboss.messaging.util.ShortString;

/**
 * 
 * A ShortStringTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ShortStringTest extends TestCase
{
	public void testString() throws Exception
	{
		final String str = "hello123ABC";
		
		ShortString s = new ShortString(str);
		
		assertEquals(str, s.asString());
		
		assertEquals(str.length(), s.getData().length);
		
		byte[] data = s.getData();
		
		ShortString s2 = new ShortString(data);
		
		assertEquals(str, s2.asString());
	}
	
	public void testInvalidString() throws Exception
	{
		final String str = "hello123ABC\uA3FA";
		
		try
		{
			new ShortString(str);
			
			fail("Should throw exception");
		}
		catch (IllegalArgumentException e)
		{
			//OK
		}
		
	}
	
//	public void testPerf() throws Exception
//	{
//		StringBuffer buff = new StringBuffer();
//		
//		for (int i = 0; i < 1000; i++)
//		{
//			buff.append('X');
//		}
//		
//		String s = buff.toString();
//		
//		long start = System.currentTimeMillis();
//		
//		long tot = 0;
//		
//		for (int i = 0; i < 1000000; i++)
//		{
//			ShortString ss = new ShortString(s);
//			
//			byte[] data = ss.getData();	
//			
//			tot += data.length;
//		}
//		
//		long end = System.currentTimeMillis();
//		
//		double rate = 1000 * (double)1000000 / ( end - start);
//		
//		System.out.println("Rate: " + rate);
//	}
	
	
}
