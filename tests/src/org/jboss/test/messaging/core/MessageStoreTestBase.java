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
package org.jboss.test.messaging.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.container.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;


/**
 * The test strategy is to try as many combination as it makes sense of the following
 * variables:
 * 
 * Note: This test is more or less defunct now since the message store doesn't do much
 * but we'll keep it here anyway
 * 
 * TODO - we should refactor this at some point
 * 
 * 1. The message can be non-reliable or reliable.
 * 2. One or multiple messages can be stored.
 * 3. One or more strong references may be maintain for the same MessageReference.
 * 4. A recoverable message store may be forcibly crashed and tested if it recovers.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class MessageStoreTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_MESSAGES = 100;

   // Static --------------------------------------------------------       

   /**
    * Makes sure the given reference refers the given message.
    */
   public static void assertCorrectReference(MessageReference ref, Message m)
   {
      assertEquals(m.getMessageID(), ref.getMessage().getMessageID());
      assertEquals(m.isReliable(), ref.getMessage().isReliable());
      assertEquals(m.getExpiration(), ref.getMessage().getExpiration());
      assertEquals(m.getTimestamp(), ref.getMessage().getTimestamp());
      assertEquals(m.getPriority(), ref.getMessage().getPriority());
      assertEquals(0, ref.getDeliveryCount());

      Map messageHeaders = m.getHeaders();
      Map refHeaders = ref.getMessage().getHeaders();
      assertEquals(messageHeaders.size(), refHeaders.size());

      for(Iterator i = messageHeaders.keySet().iterator(); i.hasNext(); )
      {
         Object header = i.next();
         Object value = messageHeaders.get(header);
         assertTrue(refHeaders.containsKey(header));
         assertEquals(value, refHeaders.get(header));
      }
   }

   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;
   protected MessageStore ms;

   protected Map headers;

   // Constructors --------------------------------------------------

   public MessageStoreTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all,-remoting,-security");
      sc.start();

      headers = new HashMap();
      headers.put("someKey", "someValue");
   }

   public void tearDown() throws Exception
   {
      headers.clear();
      headers = null;
      sc.stop();
      sc = null;

      super.tearDown();
   }

   ////
   //// Non-reliable message
   ////

   //////
   ////// One message
   //////

   ////////
   //////// One strong reference
   ////////

   public void testMessageStore_1() throws Exception
   {
      Message m = CoreMessageFactory.
      createCoreMessage(0, false, 777l, 888l, (byte)9, headers, "payload");

      // recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, m);
      ref.releaseMemoryReference();

      ref = ms.reference(m.getMessageID());
      assertNull(ref);
   }

   ////////
   //////// Two strong references
   ////////

   public void testMessageStore_1_1() throws Exception
   {

      Message m = CoreMessageFactory.
      createCoreMessage(0, false, 777l, 888l, (byte)9, headers, "payload");

      // recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, m);

      MessageReference ref2 = ms.reference(m);

      assertCorrectReference(ref2, m);
   }

   //////
   ////// Multiple messages
   //////

   public void testMessageStore_2() throws Exception
   {
      Message[] m = new Message[NUMBER_OF_MESSAGES];
      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++ )
      {
         m[i] = CoreMessageFactory.createCoreMessage(i, false, 700 + i, 800 + i,
                                             (byte)(i % 10), headers, "payload" + i);

         // recoverable store, non-reliable message, one message
         refs[i] = ms.reference(m[i]);
         assertCorrectReference(refs[i], m[i]);
      }

      // get reachable reference

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {         
         assertCorrectReference(refs[i], m[i]);
         refs[i].releaseMemoryReference();
      }

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertNull(ms.reference(m[i].getMessageID()));
      }
   }

   ////
   //// Reliable message
   ////

   //////
   ////// One message
   //////

   ////////
   //////// One strong reference
   ////////

   public void testMessageStore_3() throws Exception
   {
      Message m = CoreMessageFactory.
      createCoreMessage(0, true, 777l, 888l, (byte)9, headers, "payload");

      // recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, m);

      ref.releaseMemoryReference();

      // there's no strong reference to the unique message reference anymore, so the message store
      // should be cleaned of everything that pertains to that message

      ref = ms.reference(m.getMessageID());
      assertNull(ref);
   }

   ////////
   //////// Two strong references
   ////////

   public void testMessageStore_3_1() throws Exception
   {
      Message m = CoreMessageFactory.
      createCoreMessage(0, true, 777l, 888l, (byte)9, headers, "payload");

      // recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, m);

      MessageReference ref2 = ms.reference(m);

      assertCorrectReference(ref2, m);
   }

   //////
   ////// Multiple messages
   //////

   public void testMessageStore_4() throws Exception
   {
      Message[] m = new Message[NUMBER_OF_MESSAGES];
      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++ )
      {
         m[i] = CoreMessageFactory.createCoreMessage(i, true, 700 + i, 800 + i,
                                             (byte)(i % 10), headers, "payload" + i);

         // recoverable store, non-reliable message, one message
         refs[i] = ms.reference(m[i]);
         assertCorrectReference(refs[i], m[i]);
      }

      // get reachable reference

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {         
         refs[i].releaseMemoryReference();
      }

      // there are no strong references to the message reference anymore, so the message store
      // should be cleaned of everything that pertains to those message

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertNull(ms.reference(m[i].getMessageID()));
      }
   }
   
   public void testMessageStore_6() throws Exception
   {

      Message m = CoreMessageFactory.
      createCoreMessage(0, true, 777l, 888l, (byte)9, headers, "payload");

      // non-recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, m);
      
      //Create another reference from that reference
      MessageReference ref2 = ms.reference(m);
          
      ref.releaseMemoryReference();
      
      MessageReference ref3 = ms.reference(m.getMessageID());
      assertNotNull(ref3);
      ref3.releaseMemoryReference();
      
      assertCorrectReference(ref3, m);
      
      ref2.releaseMemoryReference();

      // there's no strong reference to the unique message reference anymore, so the message store
      // should be cleaned of everything that pertains to that message

      ref3 = ms.reference(m.getMessageID());
      assertNull(ref3);
   }
   
   public void testMessageStore_7() throws Exception
   {
      Message m = CoreMessageFactory.
      createCoreMessage(0, true, 777l, 888l, (byte)9, headers, "payload");

      // non-recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, m);
      
      MessageReference ref2 = ms.reference(ref.getMessage().getMessageID());
      assertNotNull(ref2);      
      assertCorrectReference(ref2, m);
      
      MessageReference ref3 = ms.reference(ref.getMessage().getMessageID());
      assertNotNull(ref3);      
      assertCorrectReference(ref3, m);
      
      ref.releaseMemoryReference();
      ref2.releaseMemoryReference();
      ref3.releaseMemoryReference();
      
      MessageReference ref4 = ms.reference(ref.getMessage().getMessageID());
            
      assertNull(ref4);                
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
