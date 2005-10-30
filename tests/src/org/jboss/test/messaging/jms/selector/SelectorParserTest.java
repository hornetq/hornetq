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

import java.io.ByteArrayInputStream;
import java.util.HashMap;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.jms.selector.ISelectorParser;
import org.jboss.jms.selector.SelectorParser;
import org.jboss.jms.selector.Identifier;
import org.jboss.jms.selector.Operator;


/** Tests of the JavaCC LL(1) parser.
 
 @author Scott.Stark@jboss.org
 @author d_jencks@users.sourceforge.net

 @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a> Ported from JBossMQ 
 
 @version $Revision$
 
 * (david jencks)  Used constructor of SelectorParser taking a stream
 * to avoid reInit npe in all tests.  Changed to JBossTestCase and logging.
 */
public class SelectorParserTest extends MessagingTestCase
{
   static HashMap identifierMap = new HashMap();
   static ISelectorParser parser;
   
   public SelectorParserTest(String name)
   {
      super(name);
   }
   
   protected void setUp() throws Exception
   {
      super.setUp();
      identifierMap.clear();
      if( parser == null )
      {
         parser = new SelectorParser(new ByteArrayInputStream(new byte[0]));
      }
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
      Identifier a = (Identifier) identifierMap.get("a");
      a.setValue(new Long(4));
      Identifier b = (Identifier) identifierMap.get("b");
      b.setValue(new Long(2));
      Identifier c = (Identifier) identifierMap.get("c");
      c.setValue(new Long(3));
      Identifier d = (Identifier) identifierMap.get("d");
      d.setValue(new Long(2));
      Identifier e = (Identifier) identifierMap.get("e");
      e.setValue(new Long(7));
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is true", bool.booleanValue());
      
   }
   
   public void testTrueINOperator() throws Exception
   {
      log.trace("parse(Status IN ('new', 'cleared', 'acknowledged'))");
      Operator result = (Operator) parser.parse("Status IN ('new', 'cleared', 'acknowledged')", identifierMap);
      Identifier a = (Identifier) identifierMap.get("Status");
      a.setValue("new");
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is true", bool.booleanValue());
   }
   public void testFalseINOperator() throws Exception
   {
      log.trace("parse(Status IN ('new', 'cleared', 'acknowledged'))");
      Operator result = (Operator) parser.parse("Status IN ('new', 'cleared', 'acknowledged')", identifierMap);
      Identifier a = (Identifier) identifierMap.get("Status");
      a.setValue("none");
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is false", !bool.booleanValue());
   }
   
   public void testTrueOROperator() throws Exception
   {
      log.trace("parse((Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged'))");
      Operator result = (Operator) parser.parse("(Status = 'new') OR (Status = 'cleared') OR (Status= 'acknowledged')", identifierMap);
      Identifier a = (Identifier) identifierMap.get("Status");
      a.setValue("new");
      log.trace("result -> "+result);
      Boolean bool = (Boolean) result.apply();
      assertTrue("is true", bool.booleanValue());
   }
   public void testFalseOROperator() throws Exception
   {
      log.trace("parse((Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged'))");
      Operator result = (Operator) parser.parse("(Status = 'new') OR (Status = 'cleared') OR (Status = 'acknowledged')", identifierMap);
      Identifier a = (Identifier) identifierMap.get("Status");
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
         ((Identifier) identifierMap.get("weight")).setValue(new Integer(3000));
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
         ((Identifier) identifierMap.get("weight")).setValue(new Float(3000));
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
         ((Identifier) identifierMap.get("weight")).setValue(new Double(1.2));
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
         ((Identifier) identifierMap.get("JMSType")).setValue("car");
         ((Identifier) identifierMap.get("color")).setValue("blue");
         ((Identifier) identifierMap.get("weight")).setValue("3000");
         
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
         ((Identifier) identifierMap.get("Cateogry")).setValue("category1");
         ((Identifier) identifierMap.get("Rating")).setValue(new Integer(3));
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
      junit.textui.TestRunner.run(SelectorParserTest.class);
   }
}
