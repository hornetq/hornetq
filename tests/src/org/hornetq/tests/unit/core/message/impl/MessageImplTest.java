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

package org.hornetq.tests.unit.core.message.impl;

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomByte;
import static org.hornetq.tests.util.RandomUtil.randomBytes;
import static org.hornetq.tests.util.RandomUtil.randomDouble;
import static org.hornetq.tests.util.RandomUtil.randomFloat;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomShort;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.Set;

import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.Message;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MessageImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(MessageImplTest.class);
  
   public void getSetAttributes()
   {
      for (int j = 0; j < 10; j++)
      {
         byte[] bytes = new byte[1000];
         for (int i = 0; i < bytes.length; i++)
         {
            bytes[i] = randomByte();
         }

         final byte type = randomByte();
         final boolean durable = randomBoolean();
         final long expiration = randomLong();
         final long timestamp = randomLong();
         final byte priority = randomByte();
         Message message1 = new ClientMessageImpl(type, durable, expiration, timestamp, priority, 100);
   
         Message message = message1;
         
         assertEquals(type, message.getType());
         assertEquals(durable, message.isDurable());
         assertEquals(expiration, message.getExpiration());
         assertEquals(timestamp, message.getTimestamp());
         assertEquals(priority, message.getPriority());
         
         final SimpleString destination = new SimpleString(randomString());
         final boolean durable2 = randomBoolean();
         final long expiration2 = randomLong();
         final long timestamp2 = randomLong();
         final byte priority2 = randomByte();
         
         message.setDestination(destination);
         assertEquals(destination, message.getDestination());
         
         message.setDurable(durable2);
         assertEquals(durable2, message.isDurable());
         
         message.setExpiration(expiration2);
         assertEquals(expiration2, message.getExpiration());
         
         message.setTimestamp(timestamp2);
         assertEquals(timestamp2, message.getTimestamp());
         
         message.setPriority(priority2);
         assertEquals(priority2, message.getPriority());

      }      
   }
   
   public void testExpired()
   {
      Message message = new ClientMessageImpl();
      
      assertEquals(0, message.getExpiration());
      assertFalse(message.isExpired());
      
      message.setExpiration(System.currentTimeMillis() + 1000);
      assertFalse(message.isExpired());
      
      message.setExpiration(System.currentTimeMillis() - 1);
      assertTrue(message.isExpired());
      
      message.setExpiration(System.currentTimeMillis() - 1000);
      assertTrue(message.isExpired());
      
      message.setExpiration(0);
      assertFalse(message.isExpired());
   }
   

   
   public void testProperties()
   {
      for (int j = 0; j < 10; j++)
      {
         Message msg = new ClientMessageImpl();
         
         SimpleString prop1 = new SimpleString("prop1");
         boolean val1 = randomBoolean();
         msg.putBooleanProperty(prop1, val1);
         
         SimpleString prop2 = new SimpleString("prop2");
         byte val2 = randomByte();
         msg.putByteProperty(prop2, val2);
         
         SimpleString prop3 = new SimpleString("prop3");
         byte[] val3 = randomBytes();
         msg.putBytesProperty(prop3, val3);
         
         SimpleString prop4 = new SimpleString("prop4");
         double val4 = randomDouble();
         msg.putDoubleProperty(prop4, val4);
         
         SimpleString prop5 = new SimpleString("prop5");
         float val5 = randomFloat();
         msg.putFloatProperty(prop5, val5);
         
         SimpleString prop6 = new SimpleString("prop6");
         int val6 = randomInt();
         msg.putIntProperty(prop6, val6);
         
         SimpleString prop7 = new SimpleString("prop7");
         long val7 = randomLong();
         msg.putLongProperty(prop7, val7);
         
         SimpleString prop8 = new SimpleString("prop8");
         short val8 = randomShort();
         msg.putShortProperty(prop8, val8);
         
         SimpleString prop9 = new SimpleString("prop9");
         SimpleString val9 = new SimpleString(randomString());
         msg.putStringProperty(prop9, val9);
         
         assertEquals(9, msg.getPropertyNames().size());
         assertTrue(msg.getPropertyNames().contains(prop1));
         assertTrue(msg.getPropertyNames().contains(prop2));
         assertTrue(msg.getPropertyNames().contains(prop3));
         assertTrue(msg.getPropertyNames().contains(prop4));
         assertTrue(msg.getPropertyNames().contains(prop5));
         assertTrue(msg.getPropertyNames().contains(prop6));
         assertTrue(msg.getPropertyNames().contains(prop7));
         assertTrue(msg.getPropertyNames().contains(prop8));
         assertTrue(msg.getPropertyNames().contains(prop9));
         
         assertTrue(msg.containsProperty(prop1));
         assertTrue(msg.containsProperty(prop2));
         assertTrue(msg.containsProperty(prop3));
         assertTrue(msg.containsProperty(prop4));
         assertTrue(msg.containsProperty(prop5));
         assertTrue(msg.containsProperty(prop6));
         assertTrue(msg.containsProperty(prop7));
         assertTrue(msg.containsProperty(prop8));
         assertTrue(msg.containsProperty(prop9));
                 
         assertEquals(val1, msg.getObjectProperty(prop1));
         assertEquals(val2, msg.getObjectProperty(prop2));
         assertEquals(val3, msg.getObjectProperty(prop3));
         assertEquals(val4, msg.getObjectProperty(prop4));
         assertEquals(val5, msg.getObjectProperty(prop5));
         assertEquals(val6, msg.getObjectProperty(prop6));
         assertEquals(val7, msg.getObjectProperty(prop7));
         assertEquals(val8, msg.getObjectProperty(prop8));
         assertEquals(val9, msg.getObjectProperty(prop9));
         
         SimpleString val10 = new SimpleString(randomString());
         //test overwrite
         msg.putStringProperty(prop9, val10);
         assertEquals(val10, msg.getObjectProperty(prop9));
         
         int val11 = randomInt();
         msg.putIntProperty(prop9, val11);
         assertEquals(val11, msg.getObjectProperty(prop9));
         
         msg.removeProperty(prop1);
         assertEquals(8, msg.getPropertyNames().size());        
         assertTrue(msg.getPropertyNames().contains(prop2));
         assertTrue(msg.getPropertyNames().contains(prop3));
         assertTrue(msg.getPropertyNames().contains(prop4));
         assertTrue(msg.getPropertyNames().contains(prop5));
         assertTrue(msg.getPropertyNames().contains(prop6));
         assertTrue(msg.getPropertyNames().contains(prop7));
         assertTrue(msg.getPropertyNames().contains(prop8));
         assertTrue(msg.getPropertyNames().contains(prop9));
         
         msg.removeProperty(prop2);
         assertEquals(7, msg.getPropertyNames().size());        
         assertTrue(msg.getPropertyNames().contains(prop3));
         assertTrue(msg.getPropertyNames().contains(prop4));
         assertTrue(msg.getPropertyNames().contains(prop5));
         assertTrue(msg.getPropertyNames().contains(prop6));
         assertTrue(msg.getPropertyNames().contains(prop7));
         assertTrue(msg.getPropertyNames().contains(prop8));
         assertTrue(msg.getPropertyNames().contains(prop9));
         
         msg.removeProperty(prop9);
         assertEquals(6, msg.getPropertyNames().size());        
         assertTrue(msg.getPropertyNames().contains(prop3));
         assertTrue(msg.getPropertyNames().contains(prop4));
         assertTrue(msg.getPropertyNames().contains(prop5));
         assertTrue(msg.getPropertyNames().contains(prop6));
         assertTrue(msg.getPropertyNames().contains(prop7));
         assertTrue(msg.getPropertyNames().contains(prop8));
        
         msg.removeProperty(prop3);
         msg.removeProperty(prop4);
         msg.removeProperty(prop5);
         msg.removeProperty(prop6);
         msg.removeProperty(prop7);
         msg.removeProperty(prop8);   
         assertEquals(0, msg.getPropertyNames().size());               
      }            
   }
   
   // Protected -------------------------------------------------------------------------------
   
   protected void assertMessagesEquivalent(final Message msg1, final Message msg2)
   {
      assertEquals(msg1.isDurable(), msg2.isDurable());

      assertEquals(msg1.getExpiration(), msg2.getExpiration());

      assertEquals(msg1.getTimestamp(), msg2.getTimestamp());

      assertEquals(msg1.getPriority(), msg2.getPriority());

      assertEquals(msg1.getType(), msg2.getType());         

      assertEqualsByteArrays(msg1.getBodyBuffer().toByteBuffer().array(), msg2.getBodyBuffer().toByteBuffer().array());      

      assertEquals(msg1.getDestination(), msg2.getDestination());
      
      Set<SimpleString> props1 = msg1.getPropertyNames();
      
      Set<SimpleString> props2 = msg2.getPropertyNames();
      
      assertEquals(props1.size(), props2.size());
      
      for (SimpleString propname: props1)
      {
         Object val1 = msg1.getObjectProperty(propname);
         
         Object val2 = msg2.getObjectProperty(propname);
         
         assertEquals(val1, val2);
      }
   }
   
   // Private ----------------------------------------------------------------------------------

}
