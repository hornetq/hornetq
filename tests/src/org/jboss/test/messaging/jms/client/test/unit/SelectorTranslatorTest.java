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
package org.jboss.test.messaging.jms.client.test.unit;

import org.jboss.jms.client.SelectorTranslator;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A SelectorTranslatorTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SelectorTranslatorTest extends UnitTestCase
{
   public void testParseNull()
   {
      assertNull(SelectorTranslator.convertToJBMFilterString(null));
   }
   
   public void testParseSimple()
   {
      final String selector = "color = 'red'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
   }
   
   public void testParseMoreComplex()
   {
      final String selector = "color = 'red' OR cheese = 'stilton' OR (age = 3 AND shoesize = 12)";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
   }
   
   public void testParseJMSDeliveryMode()
   {
      String selector = "JMSDeliveryMode='NON_PERSISTENT'";
      
      assertEquals("JBMDurable='NON_DURABLE'", SelectorTranslator.convertToJBMFilterString(selector));
            
      selector = "JMSDeliveryMode='PERSISTENT'";
      
      assertEquals("JBMDurable='DURABLE'", SelectorTranslator.convertToJBMFilterString(selector));
            
      selector = "color = 'red' AND 'NON_PERSISTENT' = JMSDeliveryMode";
      
      assertEquals("color = 'red' AND 'NON_DURABLE' = JBMDurable", SelectorTranslator.convertToJBMFilterString(selector));
            
      selector = "color = 'red' AND 'PERSISTENT' = JMSDeliveryMode";
      
      assertEquals("color = 'red' AND 'DURABLE' = JBMDurable", SelectorTranslator.convertToJBMFilterString(selector));
                  
      checkNoSubstitute("JMSDeliveryMode");     
   }
   
   public void testParseJMSPriority()
   {
      String selector = "JMSPriority=5";
      
      assertEquals("JBMPriority=5", SelectorTranslator.convertToJBMFilterString(selector));
            
      selector = " JMSPriority = 7";
      
      assertEquals(" JBMPriority = 7", SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " JMSPriority = 7 OR 1 = JMSPriority AND (JMSPriority= 1 + 4)";
      
      assertEquals(" JBMPriority = 7 OR 1 = JBMPriority AND (JBMPriority= 1 + 4)", SelectorTranslator.convertToJBMFilterString(selector));
                        
      checkNoSubstitute("JMSPriority");      
      
      selector = "animal = 'lion' JMSPriority = 321 OR animal_name = 'xyzJMSPriorityxyz'";
      
      assertEquals("animal = 'lion' JBMPriority = 321 OR animal_name = 'xyzJMSPriorityxyz'", SelectorTranslator.convertToJBMFilterString(selector));
     
   }
   
   public void testParseJMSMessageID()
   {
      String selector = "JMSMessageID='ID:JBM-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " JMSMessageID='ID:JBM-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " JMSMessageID = 'ID:JBM-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " myHeader = JMSMessageID";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " myHeader = JMSMessageID OR (JMSMessageID = 'ID-JBM' + '12345')";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      checkNoSubstitute("JMSMessageID"); 
   }
   
   public void testParseJMSTimestamp()
   {
      String selector = "JMSTimestamp=12345678";
      
      assertEquals("JBMTimestamp=12345678", SelectorTranslator.convertToJBMFilterString(selector));
            
      selector = " JMSTimestamp=12345678";
      
      assertEquals(" JBMTimestamp=12345678", SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " JMSTimestamp=12345678 OR 78766 = JMSTimestamp AND (JMSTimestamp= 1 + 4878787)";
      
      assertEquals(" JBMTimestamp=12345678 OR 78766 = JBMTimestamp AND (JBMTimestamp= 1 + 4878787)", SelectorTranslator.convertToJBMFilterString(selector));
      
                  
      checkNoSubstitute("JMSTimestamp"); 
      
      selector = "animal = 'lion' JMSTimestamp = 321 OR animal_name = 'xyzJMSTimestampxyz'";
      
      assertEquals("animal = 'lion' JBMTimestamp = 321 OR animal_name = 'xyzJMSTimestampxyz'", SelectorTranslator.convertToJBMFilterString(selector));
     
   }
   
   public void testParseJMSCorrelationID()
   {
      String selector = "JMSCorrelationID='ID:JBM-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " JMSCorrelationID='ID:JBM-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " JMSCorrelationID = 'ID:JBM-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " myHeader = JMSCorrelationID";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " myHeader = JMSCorrelationID OR (JMSCorrelationID = 'ID-JBM' + '12345')";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      checkNoSubstitute("JMSCorrelationID"); 
   }
   
   public void testParseJMSType()
   {
      String selector = "JMSType='aardvark'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " JMSType='aardvark'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " JMSType = 'aardvark'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " myHeader = JMSType";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = " myHeader = JMSType OR (JMSType = 'aardvark' + 'sandwich')";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      checkNoSubstitute("JMSType"); 
   }
   
   // Private -------------------------------------------------------------------------------------
   
   private void checkNoSubstitute(String fieldName)
   {
      String selector = "Other" + fieldName + " = 767868";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "cheese = 'cheddar' AND Wrong" + fieldName +" = 54";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "fruit = 'pomegranate' AND " + fieldName + "NotThisOne = 'tuesday'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = '" + fieldName + "'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = ' " + fieldName + "'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = ' " + fieldName + " '";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = 'xyz " + fieldName +"'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = 'xyz" + fieldName + "'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = '" + fieldName + "xyz'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = 'xyz" + fieldName + "xyz'";
      
      assertEquals(selector, SelectorTranslator.convertToJBMFilterString(selector));
   }
   
}
