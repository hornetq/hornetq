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

package org.hornetq.tests.unit.jms.client;

import org.hornetq.jms.client.SelectorTranslator;
import org.hornetq.tests.util.UnitTestCase;

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
      assertNull(SelectorTranslator.convertToHornetQFilterString(null));
   }
   
   public void testParseSimple()
   {
      final String selector = "color = 'red'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
   }
   
   public void testParseMoreComplex()
   {
      final String selector = "color = 'red' OR cheese = 'stilton' OR (age = 3 AND shoesize = 12)";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
   }
   
   public void testParseJMSDeliveryMode()
   {
      String selector = "JMSDeliveryMode='NON_PERSISTENT'";
      
      assertEquals("HQDurable='NON_DURABLE'", SelectorTranslator.convertToHornetQFilterString(selector));
            
      selector = "JMSDeliveryMode='PERSISTENT'";
      
      assertEquals("HQDurable='DURABLE'", SelectorTranslator.convertToHornetQFilterString(selector));
            
      selector = "color = 'red' AND 'NON_PERSISTENT' = JMSDeliveryMode";
      
      assertEquals("color = 'red' AND 'NON_DURABLE' = HQDurable", SelectorTranslator.convertToHornetQFilterString(selector));
            
      selector = "color = 'red' AND 'PERSISTENT' = JMSDeliveryMode";
      
      assertEquals("color = 'red' AND 'DURABLE' = HQDurable", SelectorTranslator.convertToHornetQFilterString(selector));
                  
      checkNoSubstitute("JMSDeliveryMode");     
   }
   
   public void testParseJMSPriority()
   {
      String selector = "JMSPriority=5";
      
      assertEquals("HQPriority=5", SelectorTranslator.convertToHornetQFilterString(selector));
            
      selector = " JMSPriority = 7";
      
      assertEquals(" HQPriority = 7", SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " JMSPriority = 7 OR 1 = JMSPriority AND (JMSPriority= 1 + 4)";
      
      assertEquals(" HQPriority = 7 OR 1 = HQPriority AND (HQPriority= 1 + 4)", SelectorTranslator.convertToHornetQFilterString(selector));
                        
      checkNoSubstitute("JMSPriority");      
      
      selector = "animal = 'lion' JMSPriority = 321 OR animal_name = 'xyzJMSPriorityxyz'";
      
      assertEquals("animal = 'lion' HQPriority = 321 OR animal_name = 'xyzJMSPriorityxyz'", SelectorTranslator.convertToHornetQFilterString(selector));
     
   }
   
   public void testParseJMSMessageID()
   {
      String selector = "JMSMessageID='ID:HQ-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " JMSMessageID='ID:HQ-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " JMSMessageID = 'ID:HQ-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " myHeader = JMSMessageID";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " myHeader = JMSMessageID OR (JMSMessageID = 'ID-HQ' + '12345')";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      checkNoSubstitute("JMSMessageID"); 
   }
   
   public void testParseJMSTimestamp()
   {
      String selector = "JMSTimestamp=12345678";
      
      assertEquals("HQTimestamp=12345678", SelectorTranslator.convertToHornetQFilterString(selector));
            
      selector = " JMSTimestamp=12345678";
      
      assertEquals(" HQTimestamp=12345678", SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " JMSTimestamp=12345678 OR 78766 = JMSTimestamp AND (JMSTimestamp= 1 + 4878787)";
      
      assertEquals(" HQTimestamp=12345678 OR 78766 = HQTimestamp AND (HQTimestamp= 1 + 4878787)", SelectorTranslator.convertToHornetQFilterString(selector));
      
                  
      checkNoSubstitute("JMSTimestamp"); 
      
      selector = "animal = 'lion' JMSTimestamp = 321 OR animal_name = 'xyzJMSTimestampxyz'";
      
      assertEquals("animal = 'lion' HQTimestamp = 321 OR animal_name = 'xyzJMSTimestampxyz'", SelectorTranslator.convertToHornetQFilterString(selector));
     
   }
   
   public void testParseJMSCorrelationID()
   {
      String selector = "JMSCorrelationID='ID:HQ-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " JMSCorrelationID='ID:HQ-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " JMSCorrelationID = 'ID:HQ-12435678";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " myHeader = JMSCorrelationID";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " myHeader = JMSCorrelationID OR (JMSCorrelationID = 'ID-HQ' + '12345')";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      checkNoSubstitute("JMSCorrelationID"); 
   }
   
   public void testParseJMSType()
   {
      String selector = "JMSType='aardvark'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " JMSType='aardvark'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " JMSType = 'aardvark'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " myHeader = JMSType";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = " myHeader = JMSType OR (JMSType = 'aardvark' + 'sandwich')";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      checkNoSubstitute("JMSType"); 
   }
   
   // Private -------------------------------------------------------------------------------------
   
   private void checkNoSubstitute(String fieldName)
   {
      String selector = "Other" + fieldName + " = 767868";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "cheese = 'cheddar' AND Wrong" + fieldName +" = 54";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "fruit = 'pomegranate' AND " + fieldName + "NotThisOne = 'tuesday'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = '" + fieldName + "'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = ' " + fieldName + "'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = ' " + fieldName + " '";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = 'xyz " + fieldName +"'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = 'xyz" + fieldName + "'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = '" + fieldName + "xyz'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
      
      selector = "animal = 'lion' AND animal_name = 'xyz" + fieldName + "xyz'";
      
      assertEquals(selector, SelectorTranslator.convertToHornetQFilterString(selector));
   }
   
}
