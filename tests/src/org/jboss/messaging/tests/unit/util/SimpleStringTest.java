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

package org.jboss.messaging.tests.unit.util;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A SimpleStringTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SimpleStringTest extends UnitTestCase
{
	public void testString() throws Exception
	{
		final String str = "hello123ABC__524`16254`6125!%^$!%$!%$!%$!%!$%!$$!\uA324";
		
		SimpleString s = new SimpleString(str);
		
		assertEquals(str, s.toString());
		
		assertEquals(2 * str.length(), s.getData().length);
		
		byte[] data = s.getData();
		
		SimpleString s2 = new SimpleString(data);
		
		assertEquals(str, s2.toString());
	}
	
	public void testStartsWith() throws Exception
	{
		SimpleString s1 = new SimpleString("abcdefghi");
		
		assertTrue(s1.startsWith(new SimpleString("abc")));
		
		assertTrue(s1.startsWith(new SimpleString("abcdef")));
		
		assertTrue(s1.startsWith(new SimpleString("abcdefghi")));
		
		assertFalse(s1.startsWith(new SimpleString("abcdefghijklmn")));
		
		assertFalse(s1.startsWith(new SimpleString("aardvark")));
		
		assertFalse(s1.startsWith(new SimpleString("z")));
	}
	
	public void testCharSequence() throws Exception
	{
		String s = "abcdefghijkl";
		SimpleString s1 = new SimpleString(s);
		
		assertEquals('a', s1.charAt(0));
		assertEquals('b', s1.charAt(1));
		assertEquals('c', s1.charAt(2));
		assertEquals('k', s1.charAt(10));
		assertEquals('l', s1.charAt(11));
		
		try
		{
			s1.charAt(-1);
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
		
		try
		{
			s1.charAt(-2);
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
		
		try
		{
			s1.charAt(s.length());
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
		
		try
		{
			s1.charAt(s.length() + 1);
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
		
		assertEquals(s.length(), s1.length());
		
		CharSequence ss = s1.subSequence(0, s1.length());
		
		assertEquals(ss, s1);
		
		ss = s1.subSequence(1, 4);
		assertEquals(ss, new SimpleString("bcd"));
		
		ss = s1.subSequence(5, 10);
		assertEquals(ss, new SimpleString("fghij"));
		
		ss = s1.subSequence(5, 12);
		assertEquals(ss, new SimpleString("fghijkl"));
		
		try
		{
			s1.subSequence(-1, 2);
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
		
		try
		{
			s1.subSequence(-4, -2);
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
		
		try
		{
			s1.subSequence(0, s1.length() + 1);
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
		
		try
		{
			s1.subSequence(0, s1.length() + 2);
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
		
		try
		{
			s1.subSequence(5, 1);
			fail("Should throw exception");
		}
		catch (IndexOutOfBoundsException e)
		{
			//OK
		}
	}
	
	public void testEquals() throws Exception
	{
	   assertFalse(new SimpleString("abcdef").equals(new Object()));
	   
		assertEquals(new SimpleString("abcdef"), new SimpleString("abcdef"));
		
		assertFalse(new SimpleString("abcdef").equals(new SimpleString("abggcdef")));
      assertFalse(new SimpleString("abcdef").equals(new SimpleString("ghijkl")));
	}
	
	public void testHashcode() throws Exception
   {
	   SimpleString str = new SimpleString("abcdef");
      SimpleString sameStr = new SimpleString("abcdef");
      SimpleString differentStr = new SimpleString("ghijk");
      
      assertTrue(str.hashCode() == sameStr.hashCode());
      assertFalse(str.hashCode() == differentStr.hashCode());
   }
	
	public void testUnicode() throws Exception
   {
      String myString = "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5";

      SimpleString s = new SimpleString(myString);
      byte[] data = s.getData();
      s = new SimpleString(data);
      
      assertEquals(myString, s.toString());
   }
	
	public void testUnicodeWithSurrogates() throws Exception
   {
      String myString = "abcdef&^*&!^ghijkl\uD900\uDD00";

      SimpleString s = new SimpleString(myString);
      byte[] data = s.getData();
      s = new SimpleString(data);
      
      assertEquals(myString, s.toString());
   }
	
	public void testSizeofString() throws Exception
   {	   
      assertEquals(DataConstants.SIZE_INT, SimpleString.sizeofString(new SimpleString("")));

	   SimpleString str = new SimpleString(randomString());
	   assertEquals(DataConstants.SIZE_INT + str.getData().length, SimpleString.sizeofString(str));
   }
	
	public void testSizeofNullableString() throws Exception
   {     
      assertEquals(1, SimpleString.sizeofNullableString(null));
      
      assertEquals(1 + DataConstants.SIZE_INT, SimpleString.sizeofNullableString(new SimpleString("")));

      SimpleString str = new SimpleString(randomString());
      assertEquals(1 + DataConstants.SIZE_INT + str.getData().length, SimpleString.sizeofNullableString(str));
   }

   public void testSplitNoDelimeter() throws Exception
   {
      SimpleString s = new SimpleString("abcdefghi");
      SimpleString[] strings = s.split('.');
      assertNotNull(strings);
      assertEquals(strings.length, 1);
      assertEquals(strings[0], s);
   }

   public void testSplit1Delimeter() throws Exception
   {
      SimpleString s = new SimpleString("abcd.efghi");
      SimpleString[] strings = s.split('.');
      assertNotNull(strings);
      assertEquals(strings.length, 2);
      assertEquals(strings[0], new SimpleString("abcd"));
      assertEquals(strings[1], new SimpleString("efghi"));
   }
   public void testSplitmanyDelimeters() throws Exception
   {
      SimpleString s = new SimpleString("abcd.efghi.jklmn.opqrs.tuvw.xyz");
      SimpleString[] strings = s.split('.');
      assertNotNull(strings);
      assertEquals(strings.length, 6);
      assertEquals(strings[0], new SimpleString("abcd"));
      assertEquals(strings[1], new SimpleString("efghi"));
      assertEquals(strings[2], new SimpleString("jklmn"));
      assertEquals(strings[3], new SimpleString("opqrs"));
      assertEquals(strings[4], new SimpleString("tuvw"));
      assertEquals(strings[5], new SimpleString("xyz"));
   }

   public void testContains()
   {
      SimpleString simpleString = new SimpleString("abcdefghijklmnopqrst");
      assertFalse(simpleString.contains('.'));
      assertFalse(simpleString.contains('%'));
      assertFalse(simpleString.contains('8'));
      assertFalse(simpleString.contains('.'));
      assertTrue(simpleString.contains('a'));
      assertTrue(simpleString.contains('b'));
      assertTrue(simpleString.contains('c'));
      assertTrue(simpleString.contains('d'));
      assertTrue(simpleString.contains('e'));
      assertTrue(simpleString.contains('f'));
      assertTrue(simpleString.contains('g'));
      assertTrue(simpleString.contains('h'));
      assertTrue(simpleString.contains('i'));
      assertTrue(simpleString.contains('j'));
      assertTrue(simpleString.contains('k'));
      assertTrue(simpleString.contains('l'));
      assertTrue(simpleString.contains('m'));
      assertTrue(simpleString.contains('n'));
      assertTrue(simpleString.contains('o'));
      assertTrue(simpleString.contains('p'));
      assertTrue(simpleString.contains('q'));
      assertTrue(simpleString.contains('r'));
      assertTrue(simpleString.contains('s'));
      assertTrue(simpleString.contains('t'));
   }

   public void testConcat()
   {
      SimpleString start = new SimpleString("abcdefg");
      SimpleString middle = new SimpleString("hijklmnop");
      SimpleString end = new SimpleString("qrstuvwxyz");
      assertEquals(start.concat(middle).concat(end), new SimpleString("abcdefghijklmnopqrstuvwxyz"));
      assertEquals(start.concat('.').concat(end), new SimpleString("abcdefg.qrstuvwxyz"));
      // Testing concat of SimpleString with String
      for (int i = 0; i < 10; i++)
      {
         assertEquals(new SimpleString("abcdefg-" + i), start.concat("-" + Integer.toString(i)));
         
      }
   }
   
   
   public void testMultithreadHashCode() throws Exception
   {
      for (int repeat = 0; repeat < 10; repeat++)
      {
         
         StringBuffer buffer = new StringBuffer();
         
         for (int i = 0 ; i < 100; i++)
         {
            buffer.append("Some Big String " + i);
         }
         String strvalue = buffer.toString();

         final int initialhash = new SimpleString(strvalue).hashCode();
         
         final SimpleString value = new SimpleString(strvalue);
         
         
         int nThreads = 100;
         final CountDownLatch latch = new CountDownLatch(nThreads);
         final CountDownLatch start = new CountDownLatch(1);

         class T extends Thread
         {
            boolean failed = false;

            public void run()
            {
               try
               {
                  latch.countDown();
                  start.await();

                  int newhash = value.hashCode();
                  
                  if (newhash != initialhash)
                  {
                     failed = true;
                  }
               }
               catch (Exception e)
               {
                  e.printStackTrace();
                  failed = true;
               }
            }
         }

         T x[] = new T[nThreads];
         for (int i = 0; i < nThreads; i++)
         {
            x[i] = new T();
            x[i].start();
         }

         latch.await();
         start.countDown();

         for (T t : x)
         {
            t.join();
         }

         for (T t : x)
         {
            assertFalse(t.failed);
         }
      }
   }
   
}
