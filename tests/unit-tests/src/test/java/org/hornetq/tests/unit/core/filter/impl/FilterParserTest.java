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
import org.junit.Before;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.impl.FilterParser;
import org.hornetq.core.filter.impl.Identifier;
import org.hornetq.core.filter.impl.Operator;
import org.hornetq.tests.unit.UnitTestLogger;
import org.hornetq.tests.util.UnitTestCase;

/**
 Tests of the JavaCC LL(1) parser for the HornetQ filters

 @author Scott.Stark@jboss.org
 @author d_jencks@users.sourceforge.net
 @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 @version $Revision: 3465 $

 * (david jencks)  Used constructor of SelectorParser taking a stream
 * to avoid reInit npe in all tests.  Changed to JBossTestCase and logging.
 */
public class FilterParserTest extends UnitTestCase
{
   private Map<SimpleString, Identifier> identifierMap;

   private FilterParser parser;

   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      identifierMap = new HashMap<SimpleString, Identifier>();

      parser = new FilterParser(new ByteArrayInputStream(new byte[0]));
   }

   @Test
   public void testSimpleUnary() throws Exception
   {
      // Neg Long
      UnitTestLogger.LOGGER.trace("parse(-12345 = -1 * 12345)");
      Operator result = (Operator)parser.parse(new SimpleString("-12345 = -1 * 12345"), identifierMap);
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean b = (Boolean)result.apply();
      Assert.assertTrue("is true", b.booleanValue());

      // Neg Double
      UnitTestLogger.LOGGER.trace("parse(-1 * 12345.67 = -12345.67)");
      result = (Operator)parser.parse(new SimpleString("-1 * 12345.67 = -12345.67"), identifierMap);
      UnitTestLogger.LOGGER.trace("result -> " + result);
      b = (Boolean)result.apply();
      Assert.assertTrue("is true", b.booleanValue());

      UnitTestLogger.LOGGER.trace("parse(-(1 * 12345.67) = -12345.67)");
      result = (Operator)parser.parse(new SimpleString("-(1 * 12345.67) = -12345.67"), identifierMap);
      UnitTestLogger.LOGGER.trace("result -> " + result);
      b = (Boolean)result.apply();
      Assert.assertTrue("is true", b.booleanValue());
   }

   @Test
   public void testPrecedenceNAssoc() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse(4 + 2 * 3 / 2 = 7)");
      Operator result = (Operator)parser.parse(new SimpleString("4 + 2 * 3 / 2 = 7"), identifierMap);
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean b = (Boolean)result.apply();
      Assert.assertTrue("is true", b.booleanValue());

      UnitTestLogger.LOGGER.trace("parse(4 + ((2 * 3) / 2) = 7)");
      result = (Operator)parser.parse(new SimpleString("4 + ((2 * 3) / 2) = 7"), identifierMap);
      UnitTestLogger.LOGGER.trace("result -> " + result);
      b = (Boolean)result.apply();
      Assert.assertTrue("is true", b.booleanValue());

      UnitTestLogger.LOGGER.trace("parse(4 * -2 / -1 - 4 = 4)");
      result = (Operator)parser.parse(new SimpleString("4 * -2 / -1 - 4 = 4"), identifierMap);
      UnitTestLogger.LOGGER.trace("result -> " + result);
      b = (Boolean)result.apply();
      Assert.assertTrue("is true", b.booleanValue());

      UnitTestLogger.LOGGER.trace("parse(4 * ((-2 / -1) - 4) = -8)");
      result = (Operator)parser.parse(new SimpleString("4 * ((-2 / -1) - 4) = -8"), identifierMap);
      UnitTestLogger.LOGGER.trace("result -> " + result);
      b = (Boolean)result.apply();
      Assert.assertTrue("is true", b.booleanValue());
   }

   @Test
   public void testIds() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse(a + b * c / d = e)");
      Operator result = (Operator)parser.parse(new SimpleString("a + b * c / d = e"), identifierMap);
      // 4 + 2 * 3 / 2 = 7
      Identifier a = identifierMap.get(new SimpleString("a"));
      a.setValue(new Long(4));
      Identifier b = identifierMap.get(new SimpleString("b"));
      b.setValue(new Long(2));
      Identifier c = identifierMap.get(new SimpleString("c"));
      c.setValue(new Long(3));
      Identifier d = identifierMap.get(new SimpleString("d"));
      d.setValue(new Long(2));
      Identifier e = identifierMap.get(new SimpleString("e"));
      e.setValue(new Long(7));
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean bool = (Boolean)result.apply();
      Assert.assertTrue("is true", bool.booleanValue());

   }

   @Test
   public void testTrueINOperator() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse(Status IN ('new', 'cleared', 'acknowledged'))");
      Operator result = (Operator)parser.parse(new SimpleString("Status IN ('new', 'cleared', 'acknowledged')"),
                                               identifierMap);
      Identifier a = identifierMap.get(new SimpleString("Status"));
      a.setValue(new SimpleString("new"));
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean bool = (Boolean)result.apply();
      Assert.assertTrue("is true", bool.booleanValue());
   }

   @Test
   public void testFalseINOperator() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse(Status IN ('new', 'cleared', 'acknowledged'))");
      Operator result = (Operator)parser.parse(new SimpleString("Status IN ('new', 'cleared', 'acknowledged')"),
                                               identifierMap);
      Identifier a = identifierMap.get(new SimpleString("Status"));
      a.setValue(new SimpleString("none"));
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean bool = (Boolean)result.apply();
      Assert.assertTrue("is false", !bool.booleanValue());
   }

   @Test
   public void testTrueNOTINOperator() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse(Status IN ('new', 'cleared', 'acknowledged'))");
      Operator result = (Operator)parser.parse(new SimpleString("Status NOT IN ('new', 'cleared', 'acknowledged')"),
                                               identifierMap);
      Identifier a = identifierMap.get(new SimpleString("Status"));
      a.setValue(new SimpleString("none"));
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean bool = (Boolean)result.apply();
      Assert.assertTrue(bool.booleanValue());
   }

   @Test
   public void testFalseNOTINOperator() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse(Status IN ('new', 'cleared', 'acknowledged'))");
      Operator result = (Operator)parser.parse(new SimpleString("Status NOT IN ('new', 'cleared', 'acknowledged')"),
                                               identifierMap);
      Identifier a = identifierMap.get(new SimpleString("Status"));
      a.setValue(new SimpleString("new"));
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean bool = (Boolean)result.apply();
      Assert.assertFalse(bool.booleanValue());
   }

   @Test
   public void testTrueOROperator() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse((Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged'))");
      Operator result = (Operator)parser.parse(new SimpleString("(Status = 'new') OR (Status = 'cleared') OR (Status= 'acknowledged')"),
                                               identifierMap);
      Identifier a = identifierMap.get(new SimpleString("Status"));
      a.setValue(new SimpleString("new"));
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean bool = (Boolean)result.apply();
      Assert.assertTrue("is true", bool.booleanValue());
   }

   @Test
   public void testFalseOROperator() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse((Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged'))");
      Operator result = (Operator)parser.parse(new SimpleString("(Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged')"),
                                               identifierMap);
      Identifier a = identifierMap.get(new SimpleString("Status"));
      a.setValue(new SimpleString("none"));
      UnitTestLogger.LOGGER.trace("result -> " + result);
      Boolean bool = (Boolean)result.apply();
      Assert.assertTrue("is false", !bool.booleanValue());
   }

   @Test
   public void testInvalidSelector() throws Exception
   {
      UnitTestLogger.LOGGER.trace("parse(definitely not a message selector!)");
      try
      {
         Object result = parser.parse(new SimpleString("definitely not a message selector!"), identifierMap);
         UnitTestLogger.LOGGER.trace("result -> " + result);
         Assert.fail("Should throw an Exception.\n");
      }
      catch (Exception e)
      {
         UnitTestLogger.LOGGER.trace("testInvalidSelector failed as expected", e);
      }
   }

   /**
    * Test diffent syntax for approximate numeric literal (+6.2, -95.7, 7.)
    */
   @Test
   public void testApproximateNumericLiteral1()
   {
      try
      {
         UnitTestLogger.LOGGER.trace("parse(average = +6.2)");
         Object result = parser.parse(new SimpleString("average = +6.2"), identifierMap);
         UnitTestLogger.LOGGER.trace("result -> " + result);
      }
      catch (Exception e)
      {
         Assert.fail("" + e);
      }
   }

   @Test
   public void testApproximateNumericLiteral2()
   {
      try
      {
         UnitTestLogger.LOGGER.trace("parse(average = -95.7)");
         Object result = parser.parse(new SimpleString("average = -95.7"), identifierMap);
         UnitTestLogger.LOGGER.trace("result -> " + result);
      }
      catch (Exception e)
      {
         Assert.fail("" + e);
      }
   }

   @Test
   public void testApproximateNumericLiteral3()
   {
      try
      {
         UnitTestLogger.LOGGER.trace("parse(average = 7.)");
         Object result = parser.parse(new SimpleString("average = 7."), identifierMap);
         UnitTestLogger.LOGGER.trace("result -> " + result);
      }
      catch (Exception e)
      {
         Assert.fail("" + e);
      }
   }

   @Test
   public void testGTExact()
   {
      try
      {
         UnitTestLogger.LOGGER.trace("parse(weight > 2500)");
         Operator result = (Operator)parser.parse(new SimpleString("weight > 2500"), identifierMap);
         identifierMap.get(new SimpleString("weight")).setValue(new Integer(3000));
         UnitTestLogger.LOGGER.trace("result -> " + result);
         Boolean bool = (Boolean)result.apply();
         Assert.assertTrue("is true", bool.booleanValue());
      }
      catch (Exception e)
      {
         UnitTestLogger.LOGGER.trace("failed", e);
         Assert.fail("" + e);
      }
   }

   @Test
   public void testGTFloat()
   {
      try
      {
         UnitTestLogger.LOGGER.trace("parse(weight > 2500)");
         Operator result = (Operator)parser.parse(new SimpleString("weight > 2500"), identifierMap);
         identifierMap.get(new SimpleString("weight")).setValue(new Float(3000));
         UnitTestLogger.LOGGER.trace("result -> " + result);
         Boolean bool = (Boolean)result.apply();
         Assert.assertTrue("is true", bool.booleanValue());
      }
      catch (Exception e)
      {
         UnitTestLogger.LOGGER.trace("failed", e);
         Assert.fail("" + e);
      }
   }

   @Test
   public void testLTDouble()
   {
      try
      {
         UnitTestLogger.LOGGER.trace("parse(weight < 1.5)");
         Operator result = (Operator)parser.parse(new SimpleString("weight < 1.5"), identifierMap);
         identifierMap.get(new SimpleString("weight")).setValue(new Double(1.2));
         UnitTestLogger.LOGGER.trace("result -> " + result);
         Boolean bool = (Boolean)result.apply();
         Assert.assertTrue("is true", bool.booleanValue());
      }
      catch (Exception e)
      {
         UnitTestLogger.LOGGER.trace("failed", e);
         Assert.fail("" + e);
      }
   }

   @Test
   public void testAndCombination()
   {
      try
      {
         UnitTestLogger.LOGGER.trace("parse(JMSType = 'car' AND color = 'blue' AND weight > 2500)");
         Operator result = (Operator)parser.parse(new SimpleString("JMSType = 'car' AND color = 'blue' AND weight > 2500"),
                                                  identifierMap);
         identifierMap.get(new SimpleString("JMSType")).setValue(new SimpleString("car"));
         identifierMap.get(new SimpleString("color")).setValue(new SimpleString("blue"));
         identifierMap.get(new SimpleString("weight")).setValue(new SimpleString("3000"));

         UnitTestLogger.LOGGER.trace("result -> " + result);
         Boolean bool = (Boolean)result.apply();
         Assert.assertTrue("is false", !bool.booleanValue());
      }
      catch (Exception e)
      {
         UnitTestLogger.LOGGER.trace("failed", e);
         Assert.fail("" + e);
      }
   }

   @Test
   public void testINANDCombination()
   {
      try
      {
         UnitTestLogger.LOGGER.trace("parse(Cateogry IN ('category1') AND Rating >= 2");
         Operator result = (Operator)parser.parse(new SimpleString("Category IN ('category1') AND Rating >= 2"),
                                                  identifierMap);
         identifierMap.get(new SimpleString("Category")).setValue(new SimpleString("category1"));
         identifierMap.get(new SimpleString("Rating")).setValue(new Integer(3));
         UnitTestLogger.LOGGER.trace("result -> " + result);
         Boolean bool = (Boolean)result.apply();
         Assert.assertTrue("is true", bool.booleanValue());
      }
      catch (Exception e)
      {
         UnitTestLogger.LOGGER.trace("failed", e);
         Assert.fail("" + e);
      }
   }
}
