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

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.filter.impl.Identifier;
import org.jboss.messaging.core.filter.impl.Operator;
import org.jboss.messaging.core.filter.impl.FilterParser;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 Tests of the JavaCC LL(1) parser for the JBoss Messaging filters
 
 @author Scott.Stark@jboss.org
 @author d_jencks@users.sourceforge.net
 @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 
 @version $Revision: 3465 $
 
 * (david jencks)  Used constructor of SelectorParser taking a stream
 * to avoid reInit npe in all tests.  Changed to JBossTestCase and logging.
 */
public class FilterParserTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(FilterParserTest.class);
   
   private Map<String, Identifier> identifierMap;
   
   private FilterParser parser;
    
   protected void setUp() throws Exception
   {
      super.setUp();
      
      identifierMap = new HashMap<String, Identifier>();
      
      parser = new FilterParser(new ByteArrayInputStream(new byte[0]));      
   }
 
   public void testSimpleUnary() throws Exception
   {
      // Neg Long
      log.trace("parse(-12345 = -1 * 12345)");
      Operator result = (Operator) parser.parse("-12345 = -1 * 12345", identifierMap);
      log.trace("result -> "+result);
      Boolean b = (Boolean) result.apply();
      assertTrue("is true", b.booleanValue());

      // Neg Double
      log.trace("parse(-1 * 12345.67 = -12345.67)");
      result = (Operator) parser.parse("-1 * 12345.67 = -12345.67", identifierMap);
      log.trace("result -> "+result);
      b = (Boolean) result.apply();
      assertTrue("is true", b.booleanValue());

      log.trace("parse(-(1 * 12345.67) = -12345.67)");
      result = (Operator) parser.parse("-(1 * 12345.67) = -12345.67", identifierMap);
      log.trace("result -> "+result);
      b = (Boolean) result.apply();
      assertTrue("is true", b.booleanValue());
   }
   
   public void testPrecedenceNAssoc() throws Exception
   {
      log.trace("parse(4 + 2 * 3 / 2 = 7)");
      Operator result = (Operator) parser.parse("4 + 2 * 3 / 2 = 7", identifierMap);
      log.trace("result -> "+result);
      Boolean b = (Boolean) result.apply();
      assertTrue("is true", b.booleanValue());
      
      log.trace("parse(4 + ((2 * 3) / 2) = 7)");
      result = (Operator) parser.parse("4 + ((2 * 3) / 2) = 7", identifierMap);
      log.trace("result -> "+result);
      b = (Boolean) result.apply();
      assertTrue("is true", b.booleanValue());
      
      log.trace("parse(4 * -2 / -1 - 4 = 4)");
      result = (Operator) parser.parse("4 * -2 / -1 - 4 = 4", identifierMap);
      log.trace("result -> "+result);
      b = (Boolean) result.apply();
      assertTrue("is true", b.booleanValue());
      
      log.trace("parse(4 * ((-2 / -1) - 4) = -8)");
      result = (Operator) parser.parse("4 * ((-2 / -1) - 4) = -8", identifierMap);
      log.trace("result -> "+result);
      b = (Boolean) result.apply();
      assertTrue("is true", b.booleanValue());
   }
   
   public void testIds() throws Exception
   {
      log.trace("parse(a + b * c / d = e)");
      Operator result = (Operator) parser.parse("a + b * c / d = e", identifierMap);
      // 4 + 2 * 3 / 2 = 7
      Identifier a = identifierMap.get("a");
      a.setValue(new Long(4));
      Identifier b = identifierMap.get("b");
      b.setValue(new Long(2));
      Identifier c = identifierMap.get("c");
      c.setValue(new Long(3));
      Identifier d = identifierMap.get("d");
      d.setValue(new Long(2));
      Identifier e = identifierMap.get("e");
      e.setValue(new Long(7));
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is true", bool.booleanValue());
      
   }
   
   public void testTrueINOperator() throws Exception
   {
      log.trace("parse(Status IN ('new', 'cleared', 'acknowledged'))");
      Operator result = (Operator) parser.parse("Status IN ('new', 'cleared', 'acknowledged')", identifierMap);
      Identifier a = identifierMap.get("Status");
      a.setValue("new");
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is true", bool.booleanValue());
   }
   public void testFalseINOperator() throws Exception
   {
      log.trace("parse(Status IN ('new', 'cleared', 'acknowledged'))");
      Operator result = (Operator) parser.parse("Status IN ('new', 'cleared', 'acknowledged')", identifierMap);
      Identifier a = identifierMap.get("Status");
      a.setValue("none");
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is false", !bool.booleanValue());
   }
   
   public void testTrueOROperator() throws Exception
   {
      log.trace("parse((Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged'))");
      Operator result = (Operator) parser.parse("(Status = 'new') OR (Status = 'cleared') OR (Status= 'acknowledged')", identifierMap);
      Identifier a = identifierMap.get("Status");
      a.setValue("new");
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is true", bool.booleanValue());
   }
   public void testFalseOROperator() throws Exception
   {
      log.trace("parse((Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged'))");
      Operator result = (Operator) parser.parse("(Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged')", identifierMap);
      Identifier a = identifierMap.get("Status");
      a.setValue("none");
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is false", !bool.booleanValue());
   }
   
   public void testInvalidSelector() throws Exception
   {
      log.trace("parse(definitely not a message selector!)");
      try
      {
         Object result = parser.parse("definitely not a message selector!", identifierMap);
         log.trace("result -> "+result);
         fail("Should throw an Exception.\n");
      }
      catch (Exception e)
      {
         log.trace("testInvalidSelector failed as expected", e);
      }
   }
 
   /**
    * Test diffent syntax for approximate numeric literal (+6.2, -95.7, 7.)
    */
   public void testApproximateNumericLiteral1()
   {
      try
      {
         log.trace("parse(average = +6.2)");
         Object result = parser.parse("average = +6.2", identifierMap);
         log.trace("result -> "+result);
      } catch (Exception e)
      {
         fail(""+e);
      }
   }
   
   public void testApproximateNumericLiteral2()
   {
      try
      {
         log.trace("parse(average = -95.7)");
         Object result = parser.parse("average = -95.7", identifierMap);
         log.trace("result -> "+result);
      } catch (Exception e)
      {
         fail(""+e);
      }
   }
   public void testApproximateNumericLiteral3()
   {
      try
      {
         log.trace("parse(average = 7.)");
         Object result = parser.parse("average = 7.", identifierMap);
         log.trace("result -> "+result);
      } catch (Exception e)
      {
         fail(""+e);
      }
   }
   
   public void testGTExact()
   {
      try
      {
         log.trace("parse(weight > 2500)");
         Operator result = (Operator)parser.parse("weight > 2500", identifierMap);
         (identifierMap.get("weight")).setValue(new Integer(3000));
         log.trace("result -> "+result);
         Boolean bool = (Boolean) result.apply();
         assertTrue("is true", bool.booleanValue());
      } catch (Exception e)
      {
         log.trace("failed", e);
         fail(""+e);
      }
   }

   public void testGTFloat()
   {
      try
      {
         log.trace("parse(weight > 2500)");
         Operator result = (Operator)parser.parse("weight > 2500", identifierMap);
         (identifierMap.get("weight")).setValue(new Float(3000));
         log.trace("result -> "+result);
         Boolean bool = (Boolean) result.apply();
         assertTrue("is true", bool.booleanValue());
      } catch (Exception e)
      {
         log.trace("failed", e);
         fail(""+e);
      }
   }

   public void testLTDouble()
   {
      try
      {
         log.trace("parse(weight < 1.5)");
         Operator result = (Operator)parser.parse("weight < 1.5", identifierMap);
         (identifierMap.get("weight")).setValue(new Double(1.2));
         log.trace("result -> "+result);
         Boolean bool = (Boolean) result.apply();
         assertTrue("is true", bool.booleanValue());
      } catch (Exception e)
      {
         log.trace("failed", e);
         fail(""+e);
      }
   }

   public void testAndCombination()
   {
      try
      {
         log.trace("parse(JMSType = 'car' AND color = 'blue' AND weight > 2500)");
         Operator result = (Operator)parser.parse("JMSType = 'car' AND color = 'blue' AND weight > 2500", identifierMap);
         (identifierMap.get("JMSType")).setValue("car");
         (identifierMap.get("color")).setValue("blue");
         (identifierMap.get("weight")).setValue("3000");
         
         log.trace("result -> "+result);
         Boolean bool = (Boolean) result.apply();
         assertTrue("is false", !bool.booleanValue());
      } catch (Exception e)
      {
         log.trace("failed", e);
         fail(""+e);
      }
   }
   
   public void testINANDCombination()
   {
      try
      {
         log.trace("parse(Cateogry IN ('category1') AND Rating >= 2");
         Operator result = (Operator)parser.parse("Cateogry IN ('category1') AND Rating >= 2", identifierMap);
         (identifierMap.get("Cateogry")).setValue("category1");
         (identifierMap.get("Rating")).setValue(new Integer(3));
         log.trace("result -> "+result);
         Boolean bool = (Boolean) result.apply();
         assertTrue("is true", bool.booleanValue());
      } catch (Exception e)
      {
         log.trace("failed", e);
         fail(""+e);
      }
   }

   /** This testcase does not use the JBossServer so override
   the testServerFound to be a noop
   */
   public void testServerFound()
   {
   }

   public static void main(java.lang.String[] args)
   {
      junit.textui.TestRunner.run(FilterParserTest.class);
   }
}
