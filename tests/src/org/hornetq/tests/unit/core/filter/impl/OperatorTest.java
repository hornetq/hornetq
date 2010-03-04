/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.unit.core.filter.impl;

import java.util.HashSet;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.impl.Operator;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A OperatorTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 3 nov. 2008 17:22:22
 *
 *
 */
public class OperatorTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------
   private static void assertSuccess(final int op, final Object arg1, final Object expectedResult) throws Exception
   {
      OperatorTest.assertOperationSuccess(new Operator(op, arg1), expectedResult);
   }

   private static void assertSuccess(final int op, final Object arg1, final Object arg2, final Object expectedResult) throws Exception
   {
      OperatorTest.assertOperationSuccess(new Operator(op, arg1, arg2), expectedResult);
   }

   private static void assertSuccess(final int op,
                                     final Object arg1,
                                     final Object arg2,
                                     final Object arg3,
                                     final Object expectedResult) throws Exception
   {
      OperatorTest.assertOperationSuccess(new Operator(op, arg1, arg2, arg3), expectedResult);
   }

   private static void assertOperationSuccess(final Operator operator, final Object expectedResult) throws Exception
   {
      Assert.assertEquals(expectedResult, operator.apply());
   }

   private static void assertFailure(final int op, final Object arg1) throws Exception
   {
      OperatorTest.assertOperationFailure(new Operator(op, arg1));
   }

   private static void assertFailure(final int op, final Object arg1, final Object arg2) throws Exception
   {
      OperatorTest.assertOperationFailure(new Operator(op, arg1, arg2));
   }

   private static void assertFailure(final int op, final Object arg1, final Object arg2, final Object arg3) throws Exception
   {
      OperatorTest.assertOperationFailure(new Operator(op, arg1, arg2, arg3));
   }

   private static void assertOperationFailure(final Operator operator)
   {
      try
      {
         operator.apply();
         Assert.fail("expected to throw an exception");
      }
      catch (Exception e)
      {
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void test_EQUAL() throws Exception
   {
      OperatorTest.assertSuccess(Operator.EQUAL, 1, 1, true);
      OperatorTest.assertSuccess(Operator.EQUAL, 1, 1.0, true);
      OperatorTest.assertSuccess(Operator.EQUAL, 1, null, null);
      OperatorTest.assertSuccess(Operator.EQUAL, 1.0, 1, true);
      OperatorTest.assertSuccess(Operator.EQUAL, 1.0, 1.0, true);
      OperatorTest.assertSuccess(Operator.EQUAL, 1.0, null, null);

      OperatorTest.assertSuccess(Operator.EQUAL, false, false, true);
      OperatorTest.assertSuccess(Operator.EQUAL, true, false, false);
      OperatorTest.assertSuccess(Operator.EQUAL, false, true, false);
      OperatorTest.assertSuccess(Operator.EQUAL, true, true, true);

      SimpleString foo = new SimpleString("foo");
      SimpleString foo2 = new SimpleString("foo");
      SimpleString bar = new SimpleString("bar");
      OperatorTest.assertSuccess(Operator.EQUAL, foo, foo, true);
      OperatorTest.assertSuccess(Operator.EQUAL, foo, foo2, true);
      OperatorTest.assertSuccess(Operator.EQUAL, foo, bar, false);
      OperatorTest.assertSuccess(Operator.EQUAL, foo, null, false);

      OperatorTest.assertSuccess(Operator.EQUAL, null, 1.0, false);
   }

   public void test_DIFFERENT() throws Exception
   {
      OperatorTest.assertSuccess(Operator.DIFFERENT, 2, 1, true);
      OperatorTest.assertSuccess(Operator.DIFFERENT, 2, 1.0, true);
      OperatorTest.assertSuccess(Operator.DIFFERENT, 2, null, null);
      OperatorTest.assertSuccess(Operator.DIFFERENT, 2.0, 1, true);
      OperatorTest.assertSuccess(Operator.DIFFERENT, 2.0, 1.0, true);
      OperatorTest.assertSuccess(Operator.DIFFERENT, 2.0, null, null);

      OperatorTest.assertSuccess(Operator.DIFFERENT, false, false, false);
      OperatorTest.assertSuccess(Operator.DIFFERENT, true, false, true);
      OperatorTest.assertSuccess(Operator.DIFFERENT, false, true, true);
      OperatorTest.assertSuccess(Operator.DIFFERENT, true, true, false);

      SimpleString foo = new SimpleString("foo");
      SimpleString foo2 = new SimpleString("foo");
      SimpleString bar = new SimpleString("bar");
      OperatorTest.assertSuccess(Operator.DIFFERENT, foo, foo, false);
      OperatorTest.assertSuccess(Operator.DIFFERENT, foo, foo2, false);
      OperatorTest.assertSuccess(Operator.DIFFERENT, foo, bar, true);
      OperatorTest.assertSuccess(Operator.DIFFERENT, foo, null, null);

      OperatorTest.assertSuccess(Operator.DIFFERENT, null, 1.0, true);
      OperatorTest.assertSuccess(Operator.DIFFERENT, null, null, false);
   }

   public void test_IS_NULL() throws Exception
   {
      OperatorTest.assertSuccess(Operator.IS_NULL, null, true);
      OperatorTest.assertSuccess(Operator.IS_NULL, 1, false);
   }

   public void test_IS_NOT_NULL() throws Exception
   {
      OperatorTest.assertSuccess(Operator.IS_NOT_NULL, null, false);
      OperatorTest.assertSuccess(Operator.IS_NOT_NULL, 1, true);
   }

   public void test_ADD() throws Exception
   {
      OperatorTest.assertSuccess(Operator.ADD, 1, 1, 2L);
      OperatorTest.assertSuccess(Operator.ADD, 1.0, 1, 2.0);
      OperatorTest.assertSuccess(Operator.ADD, 1, 1.0, 2.0);
      OperatorTest.assertSuccess(Operator.ADD, 1.0, 1.0, 2.0);

      // incompatible types
      OperatorTest.assertFailure(Operator.ADD, true, 1.0);
      OperatorTest.assertFailure(Operator.ADD, 1, true);
   }

   public void test_SUB() throws Exception
   {
      OperatorTest.assertSuccess(Operator.SUB, 2, 1, 1L);
      OperatorTest.assertSuccess(Operator.SUB, 2.0, 1, 1.0);
      OperatorTest.assertSuccess(Operator.SUB, 2, 1.0, 1.0);
      OperatorTest.assertSuccess(Operator.SUB, 2.0, 1.0, 1.0);

      // incompatible types
      OperatorTest.assertFailure(Operator.SUB, true, 1.0);
      OperatorTest.assertFailure(Operator.SUB, 1, true);
   }

   public void test_MUL() throws Exception
   {
      OperatorTest.assertSuccess(Operator.MUL, 2, 1, 2L);
      OperatorTest.assertSuccess(Operator.MUL, 2.0, 1, 2.0);
      OperatorTest.assertSuccess(Operator.MUL, 2, 1.0, 2.0);
      OperatorTest.assertSuccess(Operator.MUL, 2.0, 1.0, 2.0);

      // incompatible types
      OperatorTest.assertSuccess(Operator.MUL, 2, null, null);
      OperatorTest.assertSuccess(Operator.MUL, null, 1.0, null);
      OperatorTest.assertFailure(Operator.MUL, true, 1.0);
      OperatorTest.assertFailure(Operator.MUL, 1, true);
   }

   public void test_DIV() throws Exception
   {
      OperatorTest.assertSuccess(Operator.DIV, 2, 2, 1L);
      OperatorTest.assertSuccess(Operator.DIV, 2.0, 2, 1.0);
      OperatorTest.assertSuccess(Operator.DIV, 2, 2.0, 1.0);
      OperatorTest.assertSuccess(Operator.DIV, 2.0, 2.0, 1.0);

      // incompatible types
      OperatorTest.assertSuccess(Operator.DIV, 2, null, null);
      OperatorTest.assertSuccess(Operator.DIV, null, 1.0, null);
      OperatorTest.assertFailure(Operator.DIV, true, 1.0);
      OperatorTest.assertFailure(Operator.DIV, 1, true);
   }

   public void test_NEG() throws Exception
   {
      OperatorTest.assertSuccess(Operator.NEG, 1, -1L);
      OperatorTest.assertSuccess(Operator.NEG, -1.0, 1.0);

      // incompatible types
      OperatorTest.assertFailure(Operator.NEG, true);
   }

   public void test_AND() throws Exception
   {
      // NULL and NULL -> NULL
      OperatorTest.assertSuccess(Operator.AND, null, null, null);
      // NULL and F -> F
      OperatorTest.assertSuccess(Operator.AND, null, false, false);
      // NULL and T -> NULL
      OperatorTest.assertSuccess(Operator.AND, null, true, null);

      // F and NULL -> F
      OperatorTest.assertSuccess(Operator.AND, false, null, false);
      // F and F -> F
      OperatorTest.assertSuccess(Operator.AND, false, false, false);
      // F and T -> F
      OperatorTest.assertSuccess(Operator.AND, false, true, false);

      // T and NULL -> NULL
      OperatorTest.assertSuccess(Operator.AND, true, null, null);
      // T and F -> F
      OperatorTest.assertSuccess(Operator.AND, true, false, false);
      // T and T -> T
      OperatorTest.assertSuccess(Operator.AND, true, true, true);

      // incompatible types
      OperatorTest.assertFailure(Operator.AND, 1.0, true);
      OperatorTest.assertFailure(Operator.AND, true, 1.0);
      OperatorTest.assertFailure(Operator.AND, null, 1.0);
   }

   public void test_OR() throws Exception
   {
      // NULL OR NULL -> NULL
      OperatorTest.assertSuccess(Operator.OR, null, null, null);
      // NULL OR F -> NULL
      OperatorTest.assertSuccess(Operator.OR, null, false, null);
      // NULL OR T -> T
      OperatorTest.assertSuccess(Operator.OR, null, true, true);

      // F or NULL -> NULL
      OperatorTest.assertSuccess(Operator.OR, false, null, null);
      // F or F -> F
      OperatorTest.assertSuccess(Operator.OR, false, false, false);
      // F or T -> F
      OperatorTest.assertSuccess(Operator.OR, false, true, true);

      // T or NULL -> T
      OperatorTest.assertSuccess(Operator.OR, true, null, true);
      // T or F -> T
      OperatorTest.assertSuccess(Operator.OR, true, false, true);
      // T or T -> T
      OperatorTest.assertSuccess(Operator.OR, true, true, true);

      // incompatible types
      OperatorTest.assertFailure(Operator.OR, 1.0, true);
      OperatorTest.assertFailure(Operator.OR, false, 1.0);
      OperatorTest.assertFailure(Operator.OR, null, 1.0);
   }

   public void test_NOT() throws Exception
   {
      // NOT NULL -> NULL
      OperatorTest.assertSuccess(Operator.NOT, null, null);
      // NOT F -> T
      OperatorTest.assertSuccess(Operator.NOT, false, true);
      // NOT T -> F
      OperatorTest.assertSuccess(Operator.NOT, true, false);

      // incompatible types
      OperatorTest.assertFailure(Operator.NOT, 1.0);
   }

   public void test_GT() throws Exception
   {
      OperatorTest.assertSuccess(Operator.GT, 2, 1, true);
      OperatorTest.assertSuccess(Operator.GT, 2.0, 1, true);
      OperatorTest.assertSuccess(Operator.GT, 2, 1.0, true);
      OperatorTest.assertSuccess(Operator.GT, 2.0, 1.0, true);

      // incompatible types
      OperatorTest.assertSuccess(Operator.GT, 2.0, true, false);
      OperatorTest.assertSuccess(Operator.GT, 2, null, null);
      OperatorTest.assertSuccess(Operator.GT, true, 1.0, false);
      OperatorTest.assertSuccess(Operator.GT, null, 1, null);
      OperatorTest.assertSuccess(Operator.GT, true, true, false);
      OperatorTest.assertSuccess(Operator.GT, null, null, null);
   }

   public void test_GE() throws Exception
   {
      OperatorTest.assertSuccess(Operator.GE, 1, 1, true);
      OperatorTest.assertSuccess(Operator.GE, 1.0, 1, true);
      OperatorTest.assertSuccess(Operator.GE, 1, 1.0, true);
      OperatorTest.assertSuccess(Operator.GE, 1.0, 1.0, true);

      // incompatible types
      OperatorTest.assertSuccess(Operator.GE, 2.0, true, false);
      OperatorTest.assertSuccess(Operator.GE, 2, null, null);
      OperatorTest.assertSuccess(Operator.GE, true, 1.0, false);
      OperatorTest.assertSuccess(Operator.GE, null, 1, null);
      OperatorTest.assertSuccess(Operator.GE, true, true, false);
      OperatorTest.assertSuccess(Operator.GE, null, null, null);
   }

   public void test_LT() throws Exception
   {
      OperatorTest.assertSuccess(Operator.LT, 1, 2, true);
      OperatorTest.assertSuccess(Operator.LT, 1.0, 2, true);
      OperatorTest.assertSuccess(Operator.LT, 1, 2.0, true);
      OperatorTest.assertSuccess(Operator.LT, 1.0, 2.0, true);

      // incompatible types
      OperatorTest.assertSuccess(Operator.LT, 1.0, true, false);
      OperatorTest.assertSuccess(Operator.LT, 1, null, null);
      OperatorTest.assertSuccess(Operator.LT, true, 2.0, false);
      OperatorTest.assertSuccess(Operator.LT, null, 2, null);
      OperatorTest.assertSuccess(Operator.LT, true, true, false);
      OperatorTest.assertSuccess(Operator.LT, null, null, null);
   }

   public void test_LE() throws Exception
   {
      OperatorTest.assertSuccess(Operator.LE, 1, 1, true);
      OperatorTest.assertSuccess(Operator.LE, 1.0, 1, true);
      OperatorTest.assertSuccess(Operator.LE, 1, 1.0, true);
      OperatorTest.assertSuccess(Operator.LE, 1.0, 1.0, true);

      // incompatible types
      OperatorTest.assertSuccess(Operator.LE, 1.0, true, false);
      OperatorTest.assertSuccess(Operator.LE, 1, null, null);
      OperatorTest.assertSuccess(Operator.LE, true, 1.0, false);
      OperatorTest.assertSuccess(Operator.LE, null, 1, null);
      OperatorTest.assertSuccess(Operator.LE, true, true, false);
      OperatorTest.assertSuccess(Operator.LE, null, null, null);
   }

   public void test_BETWEEN() throws Exception
   {
      // 2 BETWEEN 1 AND 3
      OperatorTest.assertSuccess(Operator.BETWEEN, 2, 1, 3, true);
      OperatorTest.assertSuccess(Operator.BETWEEN, 2.0, 1.0, 3.0, true);

      // incompatible types
      OperatorTest.assertSuccess(Operator.BETWEEN, true, 1, 3, false);
      OperatorTest.assertSuccess(Operator.BETWEEN, null, null, 3, null);
   }

   public void test_NOT_BETWEEN() throws Exception
   {
      // 2 NOT BETWEEN 3 AND 4
      OperatorTest.assertSuccess(Operator.NOT_BETWEEN, 2, 3, 4, true);
      OperatorTest.assertSuccess(Operator.NOT_BETWEEN, 2.0, 3.0, 4.0, true);

      // incompatible types
      OperatorTest.assertSuccess(Operator.NOT_BETWEEN, true, 1, 3, false);
      OperatorTest.assertSuccess(Operator.NOT_BETWEEN, null, null, 3, null);
   }

   public void test_IN() throws Exception
   {
      HashSet set = new HashSet();
      set.add(new SimpleString("foo"));
      set.add(new SimpleString("bar"));
      set.add(new SimpleString("baz"));

      SimpleString foo = new SimpleString("foo");

      OperatorTest.assertSuccess(Operator.IN, foo, set, true);
      OperatorTest.assertSuccess(Operator.IN, foo, new HashSet(), false);

      // incompatible types
      OperatorTest.assertFailure(Operator.IN, true, set);
   }

   public void test_NOT_IN() throws Exception
   {
      HashSet set = new HashSet();
      set.add(new SimpleString("foo"));
      set.add(new SimpleString("bar"));
      set.add(new SimpleString("baz"));

      SimpleString foo = new SimpleString("foo");

      OperatorTest.assertSuccess(Operator.NOT_IN, foo, set, false);
      OperatorTest.assertSuccess(Operator.NOT_IN, foo, new HashSet(), true);

      // incompatible types
      OperatorTest.assertFailure(Operator.NOT_IN, true, set);
   }

   public void test_LIKE() throws Exception
   {
      SimpleString pattern = new SimpleString("12%3");
      OperatorTest.assertSuccess(Operator.LIKE, new SimpleString("123"), pattern, true);
      OperatorTest.assertSuccess(Operator.LIKE, new SimpleString("12993"), pattern, true);
      OperatorTest.assertSuccess(Operator.LIKE, new SimpleString("1234"), pattern, false);

      pattern = new SimpleString("l_se");
      OperatorTest.assertSuccess(Operator.LIKE, new SimpleString("lose"), pattern, true);
      OperatorTest.assertSuccess(Operator.LIKE, new SimpleString("loose"), pattern, false);

      OperatorTest.assertSuccess(Operator.LIKE, null, pattern, null);
   }

   public void test_LIKE_ESCAPE() throws Exception
   {
      SimpleString pattern = new SimpleString("\\_%");
      SimpleString escapeChar = new SimpleString("\\");
      OperatorTest.assertSuccess(Operator.LIKE_ESCAPE, new SimpleString("_foo"), pattern, escapeChar, true);
      OperatorTest.assertSuccess(Operator.LIKE_ESCAPE, new SimpleString("bar"), pattern, escapeChar, false);
      OperatorTest.assertSuccess(Operator.LIKE_ESCAPE, null, pattern, escapeChar, null);

      OperatorTest.assertFailure(Operator.LIKE_ESCAPE,
                                 new SimpleString("_foo"),
                                 pattern,
                                 new SimpleString("must be a single char"));
   }

   public void test_NOT_LIKE() throws Exception
   {
      SimpleString pattern = new SimpleString("12%3");
      OperatorTest.assertSuccess(Operator.NOT_LIKE, new SimpleString("123"), pattern, false);
      OperatorTest.assertSuccess(Operator.NOT_LIKE, new SimpleString("12993"), pattern, false);
      OperatorTest.assertSuccess(Operator.NOT_LIKE, new SimpleString("1234"), pattern, true);
      OperatorTest.assertSuccess(Operator.NOT_LIKE, null, pattern, null);
   }

   public void test_NOT_LIKE_ESCAPE() throws Exception
   {
      SimpleString pattern = new SimpleString("\\_%");
      SimpleString escapeChar = new SimpleString("\\");
      OperatorTest.assertSuccess(Operator.NOT_LIKE_ESCAPE, new SimpleString("_foo"), pattern, escapeChar, false);
      OperatorTest.assertSuccess(Operator.NOT_LIKE_ESCAPE, new SimpleString("bar"), pattern, escapeChar, true);
      OperatorTest.assertSuccess(Operator.NOT_LIKE_ESCAPE, null, pattern, escapeChar, null);

      OperatorTest.assertFailure(Operator.NOT_LIKE_ESCAPE,
                                 new SimpleString("_foo"),
                                 pattern,
                                 new SimpleString("must be a single char"));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
