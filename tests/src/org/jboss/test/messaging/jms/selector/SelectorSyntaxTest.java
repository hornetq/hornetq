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
package org.jboss.test.messaging.jms.selector;

import org.jboss.jms.selector.Selector;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.jms.message.JBossMessage;

/**
 * Tests the complinace with the JMS Selector syntax.
 *
 * <p>Needs a lot of work...
 *
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Ported from JBossMQ
 * @version $Revision$
 */
public class SelectorSyntaxTest
   extends MessagingTestCase
{
   private Selector selector;
   private JBossMessage message;
   
   public SelectorSyntaxTest(String name)
   {
      super(name);
   }
   
   protected void setUp() throws Exception
   {
      super.setUp();
      message = new JBossMessage((String)null);
   }
   
   public void testBooleanTrue() throws Exception
   {
      selector = new Selector("MyBoolean=true");
      testBoolean("MyBoolean", true);
   }
   
   public void testBooleanFalse() throws Exception
   {
      selector = new Selector("MyBoolean=false");
      testBoolean("MyBoolean", false);
   }
   
   private void testBoolean(String name, boolean flag) throws Exception
   {
      message.setBooleanProperty(name, flag);
      assertTrue(selector.accept(message));
      
      message.setBooleanProperty(name, !flag);
      assertTrue(!selector.accept(message));
   }
   
   public void testStringEquals() throws Exception
   {
      // First, simple test of string equality and inequality
      selector = new Selector("MyString='astring'");
      
      message.setStringProperty("MyString", "astring");
      assertTrue(selector.accept(message));
      
      message.setStringProperty("MyString", "NOTastring");
      assertTrue(!selector.accept(message));
      
      // test empty string
      selector = new Selector("MyString=''");
      
      message.setStringProperty("MyString", "");
      assertTrue("test 1", selector.accept(message));
      
      message.setStringProperty("MyString", "NOTastring");
      assertTrue("test 2", !selector.accept(message));
      
      // test literal apostrophes (which are escaped using two apostrophes
      // in selectors)
      selector = new Selector("MyString='test JBoss''s selector'");
      
      // note: apostrophes are not escaped in string properties
      message.setStringProperty("MyString", "test JBoss's selector");
      // this test fails -- bug 530120
      //assertTrue("test 3", selector.accept(message));
      
      message.setStringProperty("MyString", "NOTastring");
      assertTrue("test 4", !selector.accept(message));
      
   }
   
   public void testStringLike() throws Exception
   {
      // test LIKE operator with no wildcards
      selector = new Selector("MyString LIKE 'astring'");
      
      // test where LIKE operand matches
      message.setStringProperty("MyString", "astring");
      assertTrue(selector.accept(message));
      
      // test one character string
      selector = new Selector("MyString LIKE 'a'");
      message.setStringProperty("MyString","a");
      assertTrue(selector.accept(message));
      
      // test empty string
      selector = new Selector("MyString LIKE ''");
      message.setStringProperty("MyString", "");
      assertTrue(selector.accept(message));
      
      // tests where operand does not match
      selector = new Selector("MyString LIKE 'astring'");
      
      // test with extra characters at beginning
      message.setStringProperty("MyString", "NOTastring");
      assertTrue(!selector.accept(message));
      
      // test with extra characters at end
      message.setStringProperty("MyString", "astringNOT");
      assertTrue(!selector.accept(message));
      
      // test with extra characters in the middle
      message.setStringProperty("MyString", "astNOTring");
      assertTrue(!selector.accept(message));
      
      // test where operand is entirely different
      message.setStringProperty("MyString", "totally different");
      assertTrue(!selector.accept(message));
      
      // test case sensitivity
      message.setStringProperty("MyString", "ASTRING");
      assertTrue(!selector.accept(message));
      
      // test empty string
      message.setStringProperty("MyString", "");
      assertTrue(!selector.accept(message));
      
      
      // test lower-case 'like' operator?
   }
   
   public void testStringLikeUnderbarWildcard() throws Exception
   {
      // test LIKE operator with the _ wildcard, which
      // matches any single character
      
      // first, some tests with the wildcard by itself
      selector = new Selector("MyString LIKE '_'");
      
      // test match against single character
      message.setStringProperty("MyString", "a");
      assertTrue(selector.accept(message));
      
      // test match failure against multiple characters
      message.setStringProperty("MyString", "aaaaa");
      assertTrue(!selector.accept(message));
      
      // test match failure against the empty string
      message.setStringProperty("MyString", "");
      assertTrue(!selector.accept(message));
      
      
      // next, tests with wildcard at the beginning of the string
      selector = new Selector("MyString LIKE '_bcdf'");
      
      // test match at beginning of string
      message.setStringProperty("MyString", "abcdf");
      assertTrue(selector.accept(message));
      
      // match failure in first character after wildcard
      message.setStringProperty("MyString", "aXcdf");
      assertTrue(!selector.accept(message));
      
      // match failure in middle character
      message.setStringProperty("MyString", "abXdf");
      assertTrue(!selector.accept(message));
      
      // match failure in last character
      message.setStringProperty("MyString", "abcdX");
      assertTrue(!selector.accept(message));
      
      // match failure with empty string
      message.setStringProperty("MyString", "");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at beginning
      message.setStringProperty("MyString", "XXXabcdf");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at the end
      message.setStringProperty("MyString", "abcdfXXX");
      assertTrue(!selector.accept(message));
      
      // test that the _ wildcard does not match the 'empty' character
      message.setStringProperty("MyString", "bcdf");
      assertTrue(!selector.accept(message));
      
      // next, tests with wildcard at the end of the string
      selector = new Selector("MyString LIKE 'abcd_'");
      
      // test match at end of string
      message.setStringProperty("MyString", "abcdf");
      assertTrue(selector.accept(message));
      
      // match failure in first character before wildcard
      message.setStringProperty("MyString", "abcXf");
      assertTrue(!selector.accept(message));
      
      // match failure in middle character
      message.setStringProperty("MyString", "abXdf");
      assertTrue(!selector.accept(message));
      
      // match failure in first character
      message.setStringProperty("MyString", "Xbcdf");
      assertTrue(!selector.accept(message));
      
      // match failure with empty string
      message.setStringProperty("MyString", "");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at beginning
      message.setStringProperty("MyString", "XXXabcdf");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at the end
      message.setStringProperty("MyString", "abcdfXXX");
      assertTrue(!selector.accept(message));
      
      // test that the _ wildcard does not match the 'empty' character
      message.setStringProperty("MyString", "abcd");
      assertTrue(!selector.accept(message));
      
      // test match in middle of string
      
      // next, tests with wildcard in the middle of the string
      selector = new Selector("MyString LIKE 'ab_df'");
      
      // test match in the middle of string
      message.setStringProperty("MyString", "abcdf");
      assertTrue(selector.accept(message));
      
      // match failure in first character before wildcard
      message.setStringProperty("MyString", "aXcdf");
      assertTrue(!selector.accept(message));
      
      // match failure in first character after wildcard
      message.setStringProperty("MyString", "abcXf");
      assertTrue(!selector.accept(message));
      
      // match failure in last character
      message.setStringProperty("MyString", "abcdX");
      assertTrue(!selector.accept(message));
      
      // match failure with empty string
      message.setStringProperty("MyString", "");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at beginning
      message.setStringProperty("MyString", "XXXabcdf");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at the end
      message.setStringProperty("MyString", "abcdfXXX");
      assertTrue(!selector.accept(message));
      
      // test that the _ wildcard does not match the 'empty' character
      message.setStringProperty("MyString", "abdf");
      assertTrue(!selector.accept(message));
      
      // test match failures
   }
   
   public void testStringLikePercentWildcard() throws Exception
   {
      // test LIKE operator with the % wildcard, which
      // matches any sequence of characters
      // note many of the tests are similar to those for _
      
      
      // first, some tests with the wildcard by itself
      selector = new Selector("MyString LIKE '%'");
      
      // test match against single character
      message.setStringProperty("MyString", "a");
      assertTrue(selector.accept(message));
      
      // test match against multiple characters
      message.setStringProperty("MyString", "aaaaa");
      assertTrue(selector.accept(message));
      
      message.setStringProperty("MyString", "abcdf");
      assertTrue(selector.accept(message));
      
      // test match against the empty string
      message.setStringProperty("MyString", "");
      assertTrue(selector.accept(message));
      
      
      // next, tests with wildcard at the beginning of the string
      selector = new Selector("MyString LIKE '%bcdf'");
      
      // test match with single character at beginning of string
      message.setStringProperty("MyString", "Xbcdf");
      assertTrue(selector.accept(message));
      
      // match with multiple characters at beginning
      message.setStringProperty("MyString", "XXbcdf");
      assertTrue(selector.accept(message));
      
      // match failure in middle character
      message.setStringProperty("MyString", "abXdf");
      assertTrue(!selector.accept(message));
      
      // match failure in last character
      message.setStringProperty("MyString", "abcdX");
      assertTrue(!selector.accept(message));
      
      // match failure with empty string
      message.setStringProperty("MyString", "");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at the end
      message.setStringProperty("MyString", "abcdfXXX");
      assertTrue(!selector.accept(message));
      
      // test that the % wildcard matches the empty string
      message.setStringProperty("MyString", "bcdf");
      assertTrue(selector.accept(message));
      
      // next, tests with wildcard at the end of the string
      selector = new Selector("MyString LIKE 'abcd%'");
      
      // test match of single character at end of string
      message.setStringProperty("MyString", "abcdf");
      assertTrue(selector.accept(message));
      
      // test match of multiple characters at end of string
      message.setStringProperty("MyString", "abcdfgh");
      assertTrue(selector.accept(message));
      
      // match failure in first character before wildcard
      message.setStringProperty("MyString", "abcXf");
      assertTrue(!selector.accept(message));
      
      // match failure in middle character
      message.setStringProperty("MyString", "abXdf");
      assertTrue(!selector.accept(message));
      
      // match failure in first character
      message.setStringProperty("MyString", "Xbcdf");
      assertTrue(!selector.accept(message));
      
      // match failure with empty string
      message.setStringProperty("MyString", "");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at beginning
      message.setStringProperty("MyString", "XXXabcdf");
      assertTrue(!selector.accept(message));
      
      // test that the % wildcard matches the empty string
      message.setStringProperty("MyString", "abcd");
      assertTrue(selector.accept(message));
      
      // next, tests with wildcard in the middle of the string
      selector = new Selector("MyString LIKE 'ab%df'");
      
      // test match with single character in the middle of string
      message.setStringProperty("MyString", "abXdf");
      assertTrue(selector.accept(message));
      
      // test match with multiple characters in the middle of string
      message.setStringProperty("MyString", "abXXXdf");
      assertTrue(selector.accept(message));
      
      // match failure in first character before wildcard
      message.setStringProperty("MyString", "aXcdf");
      assertTrue(!selector.accept(message));
      
      // match failure in first character after wildcard
      message.setStringProperty("MyString", "abcXf");
      assertTrue(!selector.accept(message));
      
      // match failure in last character
      message.setStringProperty("MyString", "abcdX");
      assertTrue(!selector.accept(message));
      
      // match failure with empty string
      message.setStringProperty("MyString", "");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at beginning
      message.setStringProperty("MyString", "XXXabcdf");
      assertTrue(!selector.accept(message));
      
      // match failure due to extra characters at the end
      message.setStringProperty("MyString", "abcdfXXX");
      assertTrue(!selector.accept(message));
      
      // test that the % wildcard matches the empty string
      message.setStringProperty("MyString", "abdf");
      assertTrue(selector.accept(message));
      
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
      
      selector = new Selector("MyString LIKE 'a^$b'");
      message.setStringProperty("MyString", "a^$b");
      assertTrue(selector.accept(message));
      
      // this one has a double backslash since backslash
      // is interpreted specially by Java
      selector = new Selector("MyString LIKE 'a\\dc'");
      message.setStringProperty("MyString", "a\\dc");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE 'a.c'");
      message.setStringProperty("MyString", "abc");
      assertTrue(!selector.accept(message));
      
      selector = new Selector("MyString LIKE '[abc]'");
      message.setStringProperty("MyString", "[abc]");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '[^abc]'");
      message.setStringProperty("MyString", "[^abc]");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '[a-c]'");
      message.setStringProperty("MyString", "[a-c]");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '[:alpha]'");
      message.setStringProperty("MyString", "[:alpha]");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(abc)'");
      message.setStringProperty("MyString", "(abc)");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE 'a|bc'");
      message.setStringProperty("MyString", "a|bc");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(abc)?'");
      message.setStringProperty("MyString", "(abc)?");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(abc)*'");
      message.setStringProperty("MyString", "(abc)*");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(abc)+'");
      message.setStringProperty("MyString", "(abc)+");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(abc){3}'");
      message.setStringProperty("MyString", "(abc){3}");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(abc){3,5}'");
      message.setStringProperty("MyString", "(abc){3,5}");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(abc){3,}'");
      message.setStringProperty("MyString", "(abc){3,}");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(?=abc)'");
      message.setStringProperty("MyString", "(?=abc)");
      assertTrue(selector.accept(message));
      
      selector = new Selector("MyString LIKE '(?!abc)'");
      message.setStringProperty("MyString", "(?!abc)");
      assertTrue(selector.accept(message));
   }
}
