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
package org.jboss.messaging.core.filter.impl.test.unit;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.MessagingException;
import org.jboss.test.messaging.JBMBaseTestCase;

/**
 * Tests the compliance with the JBoss Messaging Filter syntax.
 *
 * <p>Needs a lot of work...
 *
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision: 3514 $
 */
public class FilterTest  extends JBMBaseTestCase
{
   private Filter filter;
   
   private Message message;
   
   public FilterTest(String name)
   {
      super(name);
   }
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      message = new MessageImpl();
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
      filter = new FilterImpl("JBMDurable='DURABLE'");
      
      Message message = new MessageImpl();
      message.setDurable(true);
      
      assertTrue(filter.match(message));
      
      message.setDurable(false);
      
      assertFalse(filter.match(message));
      
      filter = new FilterImpl("JBMDurable='NON_DURABLE'");
      
      message = new MessageImpl();
      message.setDurable(true);
      
      assertFalse(filter.match(message));
      
      message.setDurable(false);
      
      assertTrue(filter.match(message));
      
   }

   public void testJBMPriority() throws Exception
   {
      filter = new FilterImpl("JBMPriority=3");
      
      Message message = new MessageImpl();
      
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
      filter = new FilterImpl("JBMMessageID=11223344");
      
      Message message = new MessageImpl();
      
      message.setMessageID(78676);
      
      assertFalse(filter.match(message));
      
      message.setMessageID(11223344);
      
      assertTrue(filter.match(message));
   }
   
   public void testJBMTimestamp() throws Exception
   {
      filter = new FilterImpl("JBMTimestamp=12345678");
      
      Message message = new MessageImpl();
      
      message.setTimestamp(87654321);
      
      assertFalse(filter.match(message));
      
      message.setTimestamp(12345678);
      
      assertTrue(filter.match(message));
   }
         
   public void testBooleanTrue() throws Exception
   {
      filter = new FilterImpl("MyBoolean=true");
      
      testBoolean("MyBoolean", true);
   }
   
   public void testBooleanFalse() throws Exception
   {
      filter = new FilterImpl("MyBoolean=false");
      testBoolean("MyBoolean", false);
   }
   
   private void testBoolean(String name, boolean flag) throws Exception
   {
      message.putHeader(name, flag);
      assertTrue(filter.match(message));
      
      message.putHeader(name, !flag);
      assertTrue(!filter.match(message));
   }
   
   public void testStringEquals() throws Exception
   {
      // First, simple test of string equality and inequality
      filter = new FilterImpl("MyString='astring'");
      
      message.putHeader("MyString", "astring");
      assertTrue(filter.match(message));
      
      message.putHeader("MyString", "NOTastring");
      assertTrue(!filter.match(message));
      
      // test empty string
      filter = new FilterImpl("MyString=''");
      
      message.putHeader("MyString", "");
      assertTrue("test 1", filter.match(message));
      
      message.putHeader("MyString", "NOTastring");
      assertTrue("test 2", !filter.match(message));
      
      // test literal apostrophes (which are escaped using two apostrophes
      // in selectors)
      filter = new FilterImpl("MyString='test JBoss''s filter'");
      
      // note: apostrophes are not escaped in string properties
      message.putHeader("MyString", "test JBoss's filter");
      // this test fails -- bug 530120
      //assertTrue("test 3", filter.match(message));
      
      message.putHeader("MyString", "NOTastring");
      assertTrue("test 4", !filter.match(message));
      
   }
   
   public void testStringLike() throws Exception
   {
      // test LIKE operator with no wildcards
      filter = new FilterImpl("MyString LIKE 'astring'");
      
      // test where LIKE operand matches
      message.putHeader("MyString", "astring");
      assertTrue(filter.match(message));
      
      // test one character string
      filter = new FilterImpl("MyString LIKE 'a'");
      message.putHeader("MyString","a");
      assertTrue(filter.match(message));
      
      // test empty string
      filter = new FilterImpl("MyString LIKE ''");
      message.putHeader("MyString", "");
      assertTrue(filter.match(message));
      
      // tests where operand does not match
      filter = new FilterImpl("MyString LIKE 'astring'");
      
      // test with extra characters at beginning
      message.putHeader("MyString", "NOTastring");
      assertTrue(!filter.match(message));
      
      // test with extra characters at end
      message.putHeader("MyString", "astringNOT");
      assertTrue(!filter.match(message));
      
      // test with extra characters in the middle
      message.putHeader("MyString", "astNOTring");
      assertTrue(!filter.match(message));
      
      // test where operand is entirely different
      message.putHeader("MyString", "totally different");
      assertTrue(!filter.match(message));
      
      // test case sensitivity
      message.putHeader("MyString", "ASTRING");
      assertTrue(!filter.match(message));
      
      // test empty string
      message.putHeader("MyString", "");
      assertTrue(!filter.match(message));
      
      
      // test lower-case 'like' operator?
   }
   
   public void testStringLikeUnderbarWildcard() throws Exception
   {
      // test LIKE operator with the _ wildcard, which
      // matches any single character
      
      // first, some tests with the wildcard by itself
      filter = new FilterImpl("MyString LIKE '_'");
      
      // test match against single character
      message.putHeader("MyString", "a");
      assertTrue(filter.match(message));
      
      // test match failure against multiple characters
      message.putHeader("MyString", "aaaaa");
      assertTrue(!filter.match(message));
      
      // test match failure against the empty string
      message.putHeader("MyString", "");
      assertTrue(!filter.match(message));
      
      
      // next, tests with wildcard at the beginning of the string
      filter = new FilterImpl("MyString LIKE '_bcdf'");
      
      // test match at beginning of string
      message.putHeader("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // match failure in first character after wildcard
      message.putHeader("MyString", "aXcdf");
      assertTrue(!filter.match(message));
      
      // match failure in middle character
      message.putHeader("MyString", "abXdf");
      assertTrue(!filter.match(message));
      
      // match failure in last character
      message.putHeader("MyString", "abcdX");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      message.putHeader("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      message.putHeader("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      message.putHeader("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the _ wildcard does not match the 'empty' character
      message.putHeader("MyString", "bcdf");
      assertTrue(!filter.match(message));
      
      // next, tests with wildcard at the end of the string
      filter = new FilterImpl("MyString LIKE 'abcd_'");
      
      // test match at end of string
      message.putHeader("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // match failure in first character before wildcard
      message.putHeader("MyString", "abcXf");
      assertTrue(!filter.match(message));
      
      // match failure in middle character
      message.putHeader("MyString", "abXdf");
      assertTrue(!filter.match(message));
      
      // match failure in first character
      message.putHeader("MyString", "Xbcdf");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      message.putHeader("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      message.putHeader("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      message.putHeader("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the _ wildcard does not match the 'empty' character
      message.putHeader("MyString", "abcd");
      assertTrue(!filter.match(message));
      
      // test match in middle of string
      
      // next, tests with wildcard in the middle of the string
      filter = new FilterImpl("MyString LIKE 'ab_df'");
      
      // test match in the middle of string
      message.putHeader("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // match failure in first character before wildcard
      message.putHeader("MyString", "aXcdf");
      assertTrue(!filter.match(message));
      
      // match failure in first character after wildcard
      message.putHeader("MyString", "abcXf");
      assertTrue(!filter.match(message));
      
      // match failure in last character
      message.putHeader("MyString", "abcdX");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      message.putHeader("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      message.putHeader("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      message.putHeader("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the _ wildcard does not match the 'empty' character
      message.putHeader("MyString", "abdf");
      assertTrue(!filter.match(message));
      
      // test match failures
   }
   
   public void testStringLikePercentWildcard() throws Exception
   {
      // test LIKE operator with the % wildcard, which
      // matches any sequence of characters
      // note many of the tests are similar to those for _
      
      
      // first, some tests with the wildcard by itself
      filter = new FilterImpl("MyString LIKE '%'");
      
      // test match against single character
      message.putHeader("MyString", "a");
      assertTrue(filter.match(message));
      
      // test match against multiple characters
      message.putHeader("MyString", "aaaaa");
      assertTrue(filter.match(message));
      
      message.putHeader("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // test match against the empty string
      message.putHeader("MyString", "");
      assertTrue(filter.match(message));
      
      
      // next, tests with wildcard at the beginning of the string
      filter = new FilterImpl("MyString LIKE '%bcdf'");
      
      // test match with single character at beginning of string
      message.putHeader("MyString", "Xbcdf");
      assertTrue(filter.match(message));
      
      // match with multiple characters at beginning
      message.putHeader("MyString", "XXbcdf");
      assertTrue(filter.match(message));
      
      // match failure in middle character
      message.putHeader("MyString", "abXdf");
      assertTrue(!filter.match(message));
      
      // match failure in last character
      message.putHeader("MyString", "abcdX");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      message.putHeader("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      message.putHeader("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the % wildcard matches the empty string
      message.putHeader("MyString", "bcdf");
      assertTrue(filter.match(message));
      
      // next, tests with wildcard at the end of the string
      filter = new FilterImpl("MyString LIKE 'abcd%'");
      
      // test match of single character at end of string
      message.putHeader("MyString", "abcdf");
      assertTrue(filter.match(message));
      
      // test match of multiple characters at end of string
      message.putHeader("MyString", "abcdfgh");
      assertTrue(filter.match(message));
      
      // match failure in first character before wildcard
      message.putHeader("MyString", "abcXf");
      assertTrue(!filter.match(message));
      
      // match failure in middle character
      message.putHeader("MyString", "abXdf");
      assertTrue(!filter.match(message));
      
      // match failure in first character
      message.putHeader("MyString", "Xbcdf");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      message.putHeader("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      message.putHeader("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // test that the % wildcard matches the empty string
      message.putHeader("MyString", "abcd");
      assertTrue(filter.match(message));
      
      // next, tests with wildcard in the middle of the string
      filter = new FilterImpl("MyString LIKE 'ab%df'");
      
      // test match with single character in the middle of string
      message.putHeader("MyString", "abXdf");
      assertTrue(filter.match(message));
      
      // test match with multiple characters in the middle of string
      message.putHeader("MyString", "abXXXdf");
      assertTrue(filter.match(message));
      
      // match failure in first character before wildcard
      message.putHeader("MyString", "aXcdf");
      assertTrue(!filter.match(message));
      
      // match failure in first character after wildcard
      message.putHeader("MyString", "abcXf");
      assertTrue(!filter.match(message));
      
      // match failure in last character
      message.putHeader("MyString", "abcdX");
      assertTrue(!filter.match(message));
      
      // match failure with empty string
      message.putHeader("MyString", "");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at beginning
      message.putHeader("MyString", "XXXabcdf");
      assertTrue(!filter.match(message));
      
      // match failure due to extra characters at the end
      message.putHeader("MyString", "abcdfXXX");
      assertTrue(!filter.match(message));
      
      // test that the % wildcard matches the empty string
      message.putHeader("MyString", "abdf");
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
      
      filter = new FilterImpl("MyString LIKE 'a^$b'");
      message.putHeader("MyString", "a^$b");
      assertTrue(filter.match(message));
      
      // this one has a double backslash since backslash
      // is interpreted specially by Java
      filter = new FilterImpl("MyString LIKE 'a\\dc'");
      message.putHeader("MyString", "a\\dc");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE 'a.c'");
      message.putHeader("MyString", "abc");
      assertTrue(!filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '[abc]'");
      message.putHeader("MyString", "[abc]");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '[^abc]'");
      message.putHeader("MyString", "[^abc]");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '[a-c]'");
      message.putHeader("MyString", "[a-c]");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '[:alpha]'");
      message.putHeader("MyString", "[:alpha]");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(abc)'");
      message.putHeader("MyString", "(abc)");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE 'a|bc'");
      message.putHeader("MyString", "a|bc");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(abc)?'");
      message.putHeader("MyString", "(abc)?");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(abc)*'");
      message.putHeader("MyString", "(abc)*");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(abc)+'");
      message.putHeader("MyString", "(abc)+");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(abc){3}'");
      message.putHeader("MyString", "(abc){3}");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(abc){3,5}'");
      message.putHeader("MyString", "(abc){3,5}");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(abc){3,}'");
      message.putHeader("MyString", "(abc){3,}");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(?=abc)'");
      message.putHeader("MyString", "(?=abc)");
      assertTrue(filter.match(message));
      
      filter = new FilterImpl("MyString LIKE '(?!abc)'");
      message.putHeader("MyString", "(?!abc)");
      assertTrue(filter.match(message));
   }
   
   // Private -----------------------------------------------------------------------------------
   
   private void testInvalidFilter(String filterString) throws Exception
   {
      try
      {
         filter = new FilterImpl(filterString);
         
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.INVALID_FILTER_EXPRESSION, e.getCode());
      }            
   }
   
}
