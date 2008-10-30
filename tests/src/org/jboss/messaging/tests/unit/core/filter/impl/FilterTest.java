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

package org.jboss.messaging.tests.unit.core.filter.impl;

import junit.framework.TestCase;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.util.SimpleString;

/**
 * Tests the compliance with the JBoss Messaging Filter syntax.
 *
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision: 3514 $
 */
public class FilterTest  extends TestCase
{
   private Filter filter;
   
   private ServerMessage message;
   
   public FilterTest(String name)
   {
      super(name);
   }
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      message = new ServerMessageImpl();
   }

   public void testFilterForgets() throws Exception
   {
      filter = new FilterImpl(new SimpleString("color = 'RED'"));  

      message.putStringProperty(new  SimpleString("color"), new SimpleString("RED"));
      assertTrue(filter.match(message));
      message = new ServerMessageImpl();
      assertFalse(filter.match(message));
   }


   public void testInvalidString() throws Exception
   {
      testInvalidFilter("invalid");
      
      testInvalidFilter("color = 'red");
      
      testInvalidFilter("3");
      
      testInvalidFilter(null);
   }
   
   public void testJBMDurable() throws Exception
   {
      filter = new FilterImpl(new SimpleString("JBMDurable='DURABLE'"));
      
      message.setDurable(true);
      
      assertTrue(filter.match(message));
      
      message.setDurable(false);
      
      assertFalse(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("JBMDurable='NON_DURABLE'"));
      
      message = new ServerMessageImpl();
      message.setDurable(true);
      
      assertFalse(filter.match(message));
      
      message.setDurable(false);
      
      assertTrue(filter.match(message));
      
   }

   public void testJBMPriority() throws Exception
   {
      filter = new FilterImpl(new SimpleString("JBMPriority=3"));
      
      for (int i = 0; i < 10; i++)
      {         
         message.setPriority((byte)i);
         
         if (i == 3)
         {
            assertTrue(filter.match(message));
         }
         else
         {
            assertFalse(filter.match(message));
         }                     
      }
   }
   
   public void testJBMMessageID() throws Exception
   {
      filter = new FilterImpl(new SimpleString("JBMMessageID=11223344"));
      
      message.setMessageID(78676);
      
      assertFalse(filter.match(message));
      
      message.setMessageID(11223344);
      
      assertTrue(filter.match(message));
   }
   
   public void testJBMTimestamp() throws Exception
   {
      filter = new FilterImpl(new SimpleString("JBMTimestamp=12345678"));
      
      message.setTimestamp(87654321);
      
      assertFalse(filter.match(message));
      
      message.setTimestamp(12345678);
      
      assertTrue(filter.match(message));
   }
         
   public void testBooleanTrue() throws Exception
   {
      filter = new FilterImpl(new SimpleString("MyBoolean=true"));
      
      testBoolean("MyBoolean", true);
   }
   
   public void testBooleanFalse() throws Exception
   {
      filter = new FilterImpl(new SimpleString("MyBoolean=false"));
      testBoolean("MyBoolean", false);
   }
   
   private void testBoolean(String name, boolean flag) throws Exception
   {
      message.putBooleanProperty(new SimpleString(name), flag);
      assertTrue(filter.match(message));
      
      message.putBooleanProperty(new SimpleString(name), !flag);
      assertTrue(!filter.match(message));
   }
   
   public void testStringEquals() throws Exception
   {
      // First, simple test of string equality and inequality
      filter = new FilterImpl(new SimpleString("MyString='astring'"));
      
      doPutStringProperty("MyString", "astring");
      assertTrue(filter.match(message));
      
      doPutStringProperty("MyString", "NOTastring");
      assertTrue(!filter.match(message));
      
      // test empty string
      filter = new FilterImpl(new SimpleString("MyString=''"));
      
      doPutStringProperty("MyString", "");
      assertTrue("test 1", filter.match(message));
      
      doPutStringProperty("MyString", "NOTastring");
      assertTrue("test 2", !filter.match(message));
      
      // test literal apostrophes (which are escaped using two apostrophes
      // in selectors)
      filter = new FilterImpl(new SimpleString("MyString='test JBoss''s filter'"));
      
      // note: apostrophes are not escaped in string properties
      doPutStringProperty("MyString", "test JBoss's filter");
      // this test fails -- bug 530120
      //assertTrue("test 3", filter.match(message));
      
      doPutStringProperty("MyString", "NOTastring");
      assertTrue("test 4", !filter.match(message));
      
   }
   
   public void testStringLike() throws Exception
   {
      // test LIKE operator with no wildcards
      filter = new FilterImpl(new SimpleString("MyString LIKE 'astring'"));
      
      // test where LIKE operand matches
      doPutStringProperty("MyString", "astring");
      assertTrue(filter.match(message));
      
      // test one character string
      filter = new FilterImpl(new SimpleString("MyString LIKE 'a'"));
      doPutStringProperty("MyString","a");
      assertTrue(filter.match(message));
      
      // test empty string
      filter = new FilterImpl(new SimpleString("MyString LIKE ''"));
      doPutStringProperty("MyString", "");
      assertTrue(filter.match(message));
      
      // tests where operand does not match
      filter = new FilterImpl(new SimpleString("MyString LIKE 'astring'"));
      
      // test with extra characters at beginning
      doPutStringProperty("MyString", "NOTastring");
      assertTrue(!filter.match(message));
      
      // test with extra characters at end
      doPutStringProperty("MyString", "astringNOT");
      assertTrue(!filter.match(message));
      
      // test with extra characters in the middle
      doPutStringProperty("MyString", "astNOTring");
      assertTrue(!filter.match(message));
      
      // test where operand is entirely different
      doPutStringProperty("MyString", "totally different");
      assertTrue(!filter.match(message));
      
      // test case sensitivity
      doPutStringProperty("MyString", "ASTRING");
      assertTrue(!filter.match(message));
      
      // test empty string
      doPutStringProperty("MyString", "");
      assertTrue(!filter.match(message));
      
      
      // test lower-case 'like' operator?
   }
   
   public void testStringLikeUnderbarWildcard() throws Exception
   {
      // test LIKE operator with the _ wildcard, which
      // matches any single character
      
      // first, some tests with the wildcard by itself
      filter = new FilterImpl(new SimpleString("MyString LIKE '_'"));
      
      // test match against single character
      doPutStringProperty("MyString", "a");
      assertTrue(filter.match(message));
      
      // test match failure against multiple characters
      doPutStringProperty("MyString", "aaaaa");
      assertTrue(!filter.match(message));
      
      // test match failure against the empty string
      doPutStringProperty("MyString", "");
      assertTrue(!filter.match(message));
      
      
      // next, tests with wildcard at the beginning of the string
      filter = new FilterImpl(new SimpleString("MyString LIKE '_bcdf'"));
      
      // test match at beginning of string
      doPutStringProperty("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // match failure in first character after wildcard
      doPutStringProperty("MyString", "aXcdf");
      assertTrue(!filter.match(message));
      
      // match failure in middle character
      doPutStringProperty("MyString", "abXdf");
      assertTrue(!filter.match(message));
      
      // match failure in last character
      doPutStringProperty("MyString", "abcdX");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      doPutStringProperty("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      doPutStringProperty("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      doPutStringProperty("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the _ wildcard does not match the 'empty' character
      doPutStringProperty("MyString", "bcdf");
      assertTrue(!filter.match(message));
      
      // next, tests with wildcard at the end of the string
      filter = new FilterImpl(new SimpleString("MyString LIKE 'abcd_'"));
      
      // test match at end of string
      doPutStringProperty("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // match failure in first character before wildcard
      doPutStringProperty("MyString", "abcXf");
      assertTrue(!filter.match(message));
      
      // match failure in middle character
      doPutStringProperty("MyString", "abXdf");
      assertTrue(!filter.match(message));
      
      // match failure in first character
      doPutStringProperty("MyString", "Xbcdf");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      doPutStringProperty("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      doPutStringProperty("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      doPutStringProperty("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the _ wildcard does not match the 'empty' character
      doPutStringProperty("MyString", "abcd");
      assertTrue(!filter.match(message));
      
      // test match in middle of string
      
      // next, tests with wildcard in the middle of the string
      filter = new FilterImpl(new SimpleString("MyString LIKE 'ab_df'"));
      
      // test match in the middle of string
      doPutStringProperty("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // match failure in first character before wildcard
      doPutStringProperty("MyString", "aXcdf");
      assertTrue(!filter.match(message));
      
      // match failure in first character after wildcard
      doPutStringProperty("MyString", "abcXf");
      assertTrue(!filter.match(message));
      
      // match failure in last character
      doPutStringProperty("MyString", "abcdX");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      doPutStringProperty("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      doPutStringProperty("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      doPutStringProperty("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the _ wildcard does not match the 'empty' character
      doPutStringProperty("MyString", "abdf");
      assertTrue(!filter.match(message));
      
      // test match failures
   }
   
   public void testStringLikePercentWildcard() throws Exception
   {
      // test LIKE operator with the % wildcard, which
      // matches any sequence of characters
      // note many of the tests are similar to those for _
      
      
      // first, some tests with the wildcard by itself
      filter = new FilterImpl(new SimpleString("MyString LIKE '%'"));
      
      // test match against single character
      doPutStringProperty("MyString", "a");
      assertTrue(filter.match(message));
      
      // test match against multiple characters
      doPutStringProperty("MyString", "aaaaa");
      assertTrue(filter.match(message));
      
      doPutStringProperty("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // test match against the empty string
      doPutStringProperty("MyString", "");
      assertTrue(filter.match(message));
      
      
      // next, tests with wildcard at the beginning of the string
      filter = new FilterImpl(new SimpleString("MyString LIKE '%bcdf'"));
      
      // test match with single character at beginning of string
      doPutStringProperty("MyString", "Xbcdf");
      assertTrue(filter.match(message));
      
      // match with multiple characters at beginning
      doPutStringProperty("MyString", "XXbcdf");
      assertTrue(filter.match(message));
      
      // match failure in middle character
      doPutStringProperty("MyString", "abXdf");
      assertTrue(!filter.match(message));
      
      // match failure in last character
      doPutStringProperty("MyString", "abcdX");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      doPutStringProperty("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      doPutStringProperty("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the % wildcard matches the empty string
      doPutStringProperty("MyString", "bcdf");
      assertTrue(filter.match(message));
      
      // next, tests with wildcard at the end of the string
      filter = new FilterImpl(new SimpleString("MyString LIKE 'abcd%'"));
      
      // test match of single character at end of string
      doPutStringProperty("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // test match of multiple characters at end of string
      doPutStringProperty("MyString", "abcdfgh");
      assertTrue(filter.match(message));
      
      // match failure in first character before wildcard
      doPutStringProperty("MyString", "abcXf");
      assertTrue(!filter.match(message));
      
      // match failure in middle character
      doPutStringProperty("MyString", "abXdf");
      assertTrue(!filter.match(message));
      
      // match failure in first character
      doPutStringProperty("MyString", "Xbcdf");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      doPutStringProperty("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      doPutStringProperty("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // test that the % wildcard matches the empty string
      doPutStringProperty("MyString", "abcd");
      assertTrue(filter.match(message));
      
      // next, tests with wildcard in the middle of the string
      filter = new FilterImpl(new SimpleString("MyString LIKE 'ab%df'"));
      
      // test match with single character in the middle of string
      doPutStringProperty("MyString", "abXdf");
      assertTrue(filter.match(message));
      
      // test match with multiple characters in the middle of string
      doPutStringProperty("MyString", "abXXXdf");
      assertTrue(filter.match(message));
      
      // match failure in first character before wildcard
      doPutStringProperty("MyString", "aXcdf");
      assertTrue(!filter.match(message));
      
      // match failure in first character after wildcard
      doPutStringProperty("MyString", "abcXf");
      assertTrue(!filter.match(message));
      
      // match failure in last character
      doPutStringProperty("MyString", "abcdX");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      doPutStringProperty("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      doPutStringProperty("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      doPutStringProperty("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the % wildcard matches the empty string
      doPutStringProperty("MyString", "abdf");
      assertTrue(filter.match(message));
      
   }
   
   public void testStringLikePunctuation() throws Exception
   {
      // test proper handling of some punctuation characters.
      // non-trivial since the underlying implementation might
      // (and in fact currently does) use a general-purpose
      // RE library, which has a different notion of which
      // characters are wildcards
      
      // the particular tests here are motivated by the
      // wildcards of the current underlying RE engine,
      // GNU regexp.
      
      filter = new FilterImpl(new SimpleString("MyString LIKE 'a^$b'"));
      doPutStringProperty("MyString", "a^$b");
      assertTrue(filter.match(message));
      
      // this one has a double backslash since backslash
      // is interpreted specially by Java
      filter = new FilterImpl(new SimpleString("MyString LIKE 'a\\dc'"));
      doPutStringProperty("MyString", "a\\dc");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE 'a.c'"));
      doPutStringProperty("MyString", "abc");
      assertTrue(!filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '[abc]'"));
      doPutStringProperty("MyString", "[abc]");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '[^abc]'"));
      doPutStringProperty("MyString", "[^abc]");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '[a-c]'"));
      doPutStringProperty("MyString", "[a-c]");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '[:alpha]'"));
      doPutStringProperty("MyString", "[:alpha]");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(abc)'"));
      doPutStringProperty("MyString", "(abc)");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE 'a|bc'"));
      doPutStringProperty("MyString", "a|bc");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(abc)?'"));
      doPutStringProperty("MyString", "(abc)?");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(abc)*'"));
      doPutStringProperty("MyString", "(abc)*");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(abc)+'"));
      doPutStringProperty("MyString", "(abc)+");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(abc){3}'"));
      doPutStringProperty("MyString", "(abc){3}");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(abc){3,5}'"));
      doPutStringProperty("MyString", "(abc){3,5}");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(abc){3,}'"));
      doPutStringProperty("MyString", "(abc){3,}");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(?=abc)'"));
      doPutStringProperty("MyString", "(?=abc)");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl(new SimpleString("MyString LIKE '(?!abc)'"));
      doPutStringProperty("MyString", "(?!abc)");
      assertTrue(filter.match(message));
   }
   
   // Private -----------------------------------------------------------------------------------
   
   private void doPutStringProperty(String key, String value)
   {
   	message.putStringProperty(new SimpleString(key), new SimpleString(value));
   }
   
   
   private void testInvalidFilter(String filterString) throws Exception
   {
      try
      {
      	if (filterString != null)
      	{
      		filter = new FilterImpl(new SimpleString(filterString));
      	}
      	else
      	{
      		filter = new FilterImpl(null);
      	}
         
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.INVALID_FILTER_EXPRESSION, e.getCode());
      }            
   }
   
}
