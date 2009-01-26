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

import static org.jboss.messaging.core.filter.impl.Operator.ADD;
import static org.jboss.messaging.core.filter.impl.Operator.AND;
import static org.jboss.messaging.core.filter.impl.Operator.BETWEEN;
import static org.jboss.messaging.core.filter.impl.Operator.DIFFERENT;
import static org.jboss.messaging.core.filter.impl.Operator.DIV;
import static org.jboss.messaging.core.filter.impl.Operator.EQUAL;
import static org.jboss.messaging.core.filter.impl.Operator.GE;
import static org.jboss.messaging.core.filter.impl.Operator.GT;
import static org.jboss.messaging.core.filter.impl.Operator.IN;
import static org.jboss.messaging.core.filter.impl.Operator.IS_NOT_NULL;
import static org.jboss.messaging.core.filter.impl.Operator.IS_NULL;
import static org.jboss.messaging.core.filter.impl.Operator.LE;
import static org.jboss.messaging.core.filter.impl.Operator.LIKE;
import static org.jboss.messaging.core.filter.impl.Operator.LIKE_ESCAPE;
import static org.jboss.messaging.core.filter.impl.Operator.LT;
import static org.jboss.messaging.core.filter.impl.Operator.MUL;
import static org.jboss.messaging.core.filter.impl.Operator.NEG;
import static org.jboss.messaging.core.filter.impl.Operator.NOT;
import static org.jboss.messaging.core.filter.impl.Operator.NOT_BETWEEN;
import static org.jboss.messaging.core.filter.impl.Operator.NOT_IN;
import static org.jboss.messaging.core.filter.impl.Operator.NOT_LIKE;
import static org.jboss.messaging.core.filter.impl.Operator.NOT_LIKE_ESCAPE;
import static org.jboss.messaging.core.filter.impl.Operator.OR;
import static org.jboss.messaging.core.filter.impl.Operator.SUB;

import java.util.HashSet;

import junit.framework.TestCase;

import org.jboss.messaging.core.filter.impl.Operator;
import org.jboss.messaging.util.SimpleString;

/**
 * A OperatorTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 3 nov. 2008 17:22:22
 *
 *
 */
public class OperatorTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------
   private static void assertSuccess(int op, Object arg1, Object expectedResult) throws Exception
   {
      assertOperationSuccess(new Operator(op, arg1), expectedResult);
   }

   private static void assertSuccess(int op, Object arg1, Object arg2, Object expectedResult) throws Exception
   {
      assertOperationSuccess(new Operator(op, arg1, arg2), expectedResult);
   }

   private static void assertSuccess(int op, Object arg1, Object arg2, Object arg3, Object expectedResult) throws Exception
   {
      assertOperationSuccess(new Operator(op, arg1, arg2, arg3), expectedResult);
   }

   private static void assertOperationSuccess(Operator operator, Object expectedResult) throws Exception
   {
      assertEquals(expectedResult, operator.apply());
   }

   private static void assertFailure(int op, Object arg1) throws Exception
   {
      assertOperationFailure(new Operator(op, arg1));
   }

   private static void assertFailure(int op, Object arg1, Object arg2) throws Exception
   {
      assertOperationFailure(new Operator(op, arg1, arg2));
   }

   private static void assertFailure(int op, Object arg1, Object arg2, Object arg3) throws Exception
   {
      assertOperationFailure(new Operator(op, arg1, arg2, arg3));
   }

   private static void assertOperationFailure(Operator operator)
   {
      try
      {
         operator.apply();
         fail("expected to throw an exception");
      }
      catch (Exception e)
      {
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void test_EQUAL() throws Exception
   {
      assertSuccess(EQUAL, 1, 1, true);
      assertSuccess(EQUAL, 1, 1.0, true);
      assertSuccess(EQUAL, 1, null, null);
      assertSuccess(EQUAL, 1.0, 1, true);
      assertSuccess(EQUAL, 1.0, 1.0, true);
      assertSuccess(EQUAL, 1.0, null, null);

      assertSuccess(EQUAL, false, false, true);
      assertSuccess(EQUAL, true, false, false);
      assertSuccess(EQUAL, false, true, false);
      assertSuccess(EQUAL, true, true, true);

      SimpleString foo = new SimpleString("foo");
      SimpleString foo2 = new SimpleString("foo");
      SimpleString bar = new SimpleString("bar");
      assertSuccess(EQUAL, foo, foo, true);
      assertSuccess(EQUAL, foo, foo2, true);
      assertSuccess(EQUAL, foo, bar, false);
      assertSuccess(EQUAL, foo, null, false);

      assertSuccess(EQUAL, null, 1.0, false);
   }
   
   public void test_DIFFERENT() throws Exception
   {
      assertSuccess(DIFFERENT, 2, 1, true);
      assertSuccess(DIFFERENT, 2, 1.0, true);
      assertSuccess(DIFFERENT, 2, null, null);
      assertSuccess(DIFFERENT, 2.0, 1, true);
      assertSuccess(DIFFERENT, 2.0, 1.0, true);
      assertSuccess(DIFFERENT, 2.0, null, null);

      assertSuccess(DIFFERENT, false, false, false);
      assertSuccess(DIFFERENT, true, false, true);
      assertSuccess(DIFFERENT, false, true, true);
      assertSuccess(DIFFERENT, true, true, false);

      SimpleString foo = new SimpleString("foo");
      SimpleString foo2 = new SimpleString("foo");
      SimpleString bar = new SimpleString("bar");
      assertSuccess(DIFFERENT, foo, foo, false);
      assertSuccess(DIFFERENT, foo, foo2, false);
      assertSuccess(DIFFERENT, foo, bar, true);
      assertSuccess(DIFFERENT, foo, null, null);

      assertSuccess(DIFFERENT, null, 1.0, true);
      assertSuccess(DIFFERENT, null, null, false);
   }

   public void test_IS_NULL() throws Exception
   {
      assertSuccess(IS_NULL, null, true);
      assertSuccess(IS_NULL, 1, false);
   }

   public void test_IS_NOT_NULL() throws Exception
   {
      assertSuccess(IS_NOT_NULL, null, false);
      assertSuccess(IS_NOT_NULL, 1, true);
   }

   public void test_ADD() throws Exception
   {
      assertSuccess(ADD, 1, 1, 2L);
      assertSuccess(ADD, 1.0, 1, 2.0);
      assertSuccess(ADD, 1, 1.0, 2.0);
      assertSuccess(ADD, 1.0, 1.0, 2.0);

      // incompatible types
      assertFailure(ADD, true, 1.0);
      assertFailure(ADD, 1, true);
   }

   public void test_SUB() throws Exception
   {
      assertSuccess(SUB, 2, 1, 1L);
      assertSuccess(SUB, 2.0, 1, 1.0);
      assertSuccess(SUB, 2, 1.0, 1.0);
      assertSuccess(SUB, 2.0, 1.0, 1.0);

      // incompatible types
      assertFailure(SUB, true, 1.0);
      assertFailure(SUB, 1, true);
   }

   public void test_MUL() throws Exception
   {
      assertSuccess(MUL, 2, 1, 2L);
      assertSuccess(MUL, 2.0, 1, 2.0);
      assertSuccess(MUL, 2, 1.0, 2.0);
      assertSuccess(MUL, 2.0, 1.0, 2.0);

      // incompatible types
      assertSuccess(MUL, 2, null, null);
      assertSuccess(MUL, null, 1.0, null);
      assertFailure(MUL, true, 1.0);
      assertFailure(MUL, 1, true);
   }

   public void test_DIV() throws Exception
   {
      assertSuccess(DIV, 2, 2, 1L);
      assertSuccess(DIV, 2.0, 2, 1.0);
      assertSuccess(DIV, 2, 2.0, 1.0);
      assertSuccess(DIV, 2.0, 2.0, 1.0);

      // incompatible types
      assertSuccess(DIV, 2, null, null);
      assertSuccess(DIV, null, 1.0, null);
      assertFailure(DIV, true, 1.0);
      assertFailure(DIV, 1, true);
   }

   public void test_NEG() throws Exception
   {
      assertSuccess(NEG, 1, -1L);
      assertSuccess(NEG, -1.0, 1.0);

      // incompatible types
      assertFailure(NEG, true);
   }

   public void test_AND() throws Exception
   {
      // NULL and NULL -> NULL
      assertSuccess(AND, null, null, null);
      // NULL and F -> F
      assertSuccess(AND, null, false, false);
      // NULL and T -> NULL
      assertSuccess(AND, null, true, null);

      // F and NULL -> F
      assertSuccess(AND, false, null, false);
      // F and F -> F
      assertSuccess(AND, false, false, false);
      // F and T -> F
      assertSuccess(AND, false, true, false);

      // T and NULL -> NULL
      assertSuccess(AND, true, null, null);
      // T and F -> F
      assertSuccess(AND, true, false, false);
      // T and T -> T
      assertSuccess(AND, true, true, true);

      // incompatible types
      assertFailure(AND, 1.0, true);
      assertFailure(AND, true, 1.0);
      assertFailure(AND, null, 1.0);
   }

   public void test_OR() throws Exception
   {
      // NULL OR NULL -> NULL
      assertSuccess(OR, null, null, null);
      // NULL OR F -> NULL
      assertSuccess(OR, null, false, null);
      // NULL OR T -> T
      assertSuccess(OR, null, true, true);

      // F or NULL -> NULL
      assertSuccess(OR, false, null, null);
      // F or F -> F
      assertSuccess(OR, false, false, false);
      // F or T -> F
      assertSuccess(OR, false, true, true);

      // T or NULL -> T
      assertSuccess(OR, true, null, true);
      // T or F -> T
      assertSuccess(OR, true, false, true);
      // T or T -> T
      assertSuccess(OR, true, true, true);

      // incompatible types
      assertFailure(OR, 1.0, true);
      assertFailure(OR, false, 1.0);
      assertFailure(OR, null, 1.0);
   }

   public void test_NOT() throws Exception
   {
      // NOT NULL -> NULL
      assertSuccess(NOT, null, null);
      // NOT F -> T
      assertSuccess(NOT, false, true);
      // NOT T -> F
      assertSuccess(NOT, true, false);

      // incompatible types
      assertFailure(NOT, 1.0);
   }

   public void test_GT() throws Exception
   {
      assertSuccess(GT, 2, 1, true);
      assertSuccess(GT, 2.0, 1, true);
      assertSuccess(GT, 2, 1.0, true);
      assertSuccess(GT, 2.0, 1.0, true);

      // incompatible types
      assertSuccess(GT, 2.0, true, false);
      assertSuccess(GT, 2, null, null);
      assertSuccess(GT, true, 1.0, false);
      assertSuccess(GT, null, 1, null);
      assertSuccess(GT, true, true, false);
      assertSuccess(GT, null, null, null);
   }

   public void test_GE() throws Exception
   {
      assertSuccess(GE, 1, 1, true);
      assertSuccess(GE, 1.0, 1, true);
      assertSuccess(GE, 1, 1.0, true);
      assertSuccess(GE, 1.0, 1.0, true);

      // incompatible types
      assertSuccess(GE, 2.0, true, false);
      assertSuccess(GE, 2, null, null);
      assertSuccess(GE, true, 1.0, false);
      assertSuccess(GE, null, 1, null);
      assertSuccess(GE, true, true, false);
      assertSuccess(GE, null, null, null);
   }

   public void test_LT() throws Exception
   {
      assertSuccess(LT, 1, 2, true);
      assertSuccess(LT, 1.0, 2, true);
      assertSuccess(LT, 1, 2.0, true);
      assertSuccess(LT, 1.0, 2.0, true);

      // incompatible types
      assertSuccess(LT, 1.0, true, false);
      assertSuccess(LT, 1, null, null);
      assertSuccess(LT, true, 2.0, false);
      assertSuccess(LT, null, 2, null);
      assertSuccess(LT, true, true, false);
      assertSuccess(LT, null, null, null);
   }

   public void test_LE() throws Exception
   {
      assertSuccess(LE, 1, 1, true);
      assertSuccess(LE, 1.0, 1, true);
      assertSuccess(LE, 1, 1.0, true);
      assertSuccess(LE, 1.0, 1.0, true);

      // incompatible types
      assertSuccess(LE, 1.0, true, false);
      assertSuccess(LE, 1, null, null);
      assertSuccess(LE, true, 1.0, false);
      assertSuccess(LE, null, 1, null);
      assertSuccess(LE, true, true, false);
      assertSuccess(LE, null, null, null);
   }

   public void test_BETWEEN() throws Exception
   {
      // 2 BETWEEN 1 AND 3
      assertSuccess(BETWEEN, 2, 1, 3, true);
      assertSuccess(BETWEEN, 2.0, 1.0, 3.0, true);

      // incompatible types
      assertSuccess(BETWEEN, true, 1, 3, false);
      assertSuccess(BETWEEN, null, null, 3, null);
   }

   public void test_NOT_BETWEEN() throws Exception
   {
      // 2 NOT BETWEEN 3 AND 4
      assertSuccess(NOT_BETWEEN, 2, 3, 4, true);
      assertSuccess(NOT_BETWEEN, 2.0, 3.0, 4.0, true);

      // incompatible types
      assertSuccess(NOT_BETWEEN, true, 1, 3, false);
      assertSuccess(NOT_BETWEEN, null, null, 3, null);
   }

   public void test_IN() throws Exception
   {
      HashSet set = new HashSet();
      set.add(new SimpleString("foo"));
      set.add(new SimpleString("bar"));
      set.add(new SimpleString("baz"));

      SimpleString foo = new SimpleString("foo");

      assertSuccess(IN, foo, set, true);
      assertSuccess(IN, foo, new HashSet(), false);

      // incompatible types
      assertFailure(IN, true, set);
   }

   public void test_NOT_IN() throws Exception
   {
      HashSet set = new HashSet();
      set.add(new SimpleString("foo"));
      set.add(new SimpleString("bar"));
      set.add(new SimpleString("baz"));

      SimpleString foo = new SimpleString("foo");

      assertSuccess(NOT_IN, foo, set, false);
      assertSuccess(NOT_IN, foo, new HashSet(), true);

      // incompatible types
      assertFailure(NOT_IN, true, set);
   }

   public void test_LIKE() throws Exception
   {
      SimpleString pattern = new SimpleString("12%3");
      assertSuccess(LIKE, new SimpleString("123"), pattern, true);
      assertSuccess(LIKE, new SimpleString("12993"), pattern, true);
      assertSuccess(LIKE, new SimpleString("1234"), pattern, false);

      pattern = new SimpleString("l_se");
      assertSuccess(LIKE, new SimpleString("lose"), pattern, true);
      assertSuccess(LIKE, new SimpleString("loose"), pattern, false);

      assertSuccess(LIKE, null, pattern, null);
   }

   public void test_LIKE_ESCAPE() throws Exception
   {
      SimpleString pattern = new SimpleString("\\_%");
      SimpleString escapeChar = new SimpleString("\\");
      assertSuccess(LIKE_ESCAPE, new SimpleString("_foo"), pattern, escapeChar, true);
      assertSuccess(LIKE_ESCAPE, new SimpleString("bar"), pattern, escapeChar, false);
      assertSuccess(LIKE_ESCAPE, null, pattern, escapeChar, null);

      assertFailure(LIKE_ESCAPE, new SimpleString("_foo"), pattern, new SimpleString("must be a single char"));
   }

   public void test_NOT_LIKE() throws Exception
   {
      SimpleString pattern = new SimpleString("12%3");
      assertSuccess(NOT_LIKE, new SimpleString("123"), pattern, false);
      assertSuccess(NOT_LIKE, new SimpleString("12993"), pattern, false);
      assertSuccess(NOT_LIKE, new SimpleString("1234"), pattern, true);
      assertSuccess(NOT_LIKE, null, pattern, null);
   }

   public void test_NOT_LIKE_ESCAPE() throws Exception
   {
      SimpleString pattern = new SimpleString("\\_%");
      SimpleString escapeChar = new SimpleString("\\");
      assertSuccess(NOT_LIKE_ESCAPE, new SimpleString("_foo"), pattern, escapeChar, false);
      assertSuccess(NOT_LIKE_ESCAPE, new SimpleString("bar"), pattern, escapeChar, true);
      assertSuccess(NOT_LIKE_ESCAPE, null, pattern, escapeChar, null);

      assertFailure(NOT_LIKE_ESCAPE, new SimpleString("_foo"), pattern, new SimpleString("must be a single char"));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
