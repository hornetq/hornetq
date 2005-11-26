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
package org.jboss.test.messaging.core.message.base;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.message.MessageFactory;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.lang.ref.WeakReference;


/**
 * The test strategy is to try as many combination as it makes sense of the following
 * variables:
 *
 * 1. The message store can be can be non-recoverable (does not have access to a
 *    PersistenceManager) or recoverable. A non-recoverable message store can accept reliable
 *    messages or not.
 * 2. The message can be non-reliable or reliable.
 * 3. One or multiple messages can be stored.
 * 4. One or more strong references may be maintain for the same MessageReference.
 * 5. A recoverable message store may be forcibly crashed and tested if it recovers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
   public static void assertCorrectReference(MessageReference ref, Object storeID, Message m)
   {
      assertEquals(storeID, ref.getStoreID());
      assertTrue(ref.isReference());
      assertEquals(m.getMessageID(), ref.getMessageID());
      assertEquals(m.isReliable(), ref.isReliable());
      assertEquals(m.getExpiration(), ref.getExpiration());
      assertEquals(m.getTimestamp(), ref.getTimestamp());
      assertEquals(m.getPriority(), ref.getPriority());
      assertFalse(ref.isRedelivered());

      Map messageHeaders = m.getHeaders();
      Map refHeaders = ref.getHeaders();
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

      sc = new ServiceContainer("all,-aop,-remoting,-security");
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

   //
   // Non-recoverable message store
   //

   ////
   //// Non-reliable message
   ////

   //////
   ////// One message
   //////

   ////////
   //////// One strong reference
   ////////

   public void testNonRecoverableMessageStore_1() throws Exception
   {
      if (ms.isRecoverable())
      {
         // we only test non-recoverable message stores
         return;
      }

      Message m = MessageFactory.createMessage("message0", false, 777l, 888l, 9, headers, "payload");

      // non-recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, ms.getStoreID(), m);

      // get reachable reference
      ref = ms.getReference(m.getMessageID());
      ref.acquireReference();
      assertCorrectReference(ref, ms.getStoreID(), m);

//      // send reference out of scope and request garbage collection
//      WeakReference control = new WeakReference(ref);
//      ref = null;
//      System.gc();
//
//      // make sure the unreachable reference has been garbage collected
//      assertNull(control.get());
      
      ref.releaseReference();

      // there's no strong reference to the unique message reference anymore, so the message store
      // should be cleaned of everything that pertains to that message

      ref = ms.getReference(m.getMessageID());
      assertNull(ref);
   }

   ////////
   //////// Two strong references
   ////////

   public void testNonRecoverableMessageStore_1_1() throws Exception
   {
      if (ms.isRecoverable())
      {
         // we only test non-recoverable message stores
         return;
      }

      Message m = MessageFactory.createMessage("message0", false, 777l, 888l, 9, headers, "payload");

      // non-recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, ms.getStoreID(), m);

      MessageReference ref2 = ms.reference(m);

      assertTrue(ref == ref2);
   }

   //////
   ////// Multiple messages
   //////

   public void testNonRecoverableMessageStore_2() throws Exception
   {
      if (ms.isRecoverable())
      {
         // we only test non-recoverable message stores
         return;
      }

      Message[] m = new Message[NUMBER_OF_MESSAGES];
      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++ )
      {
         m[i] = MessageFactory.
            createMessage("message" + i, false, 700 + i, 800 + i, i % 10, headers, "payload" + i);

         // non-recoverable store, non-reliable message, one message
         refs[i] = ms.reference(m[i]);
         assertCorrectReference(refs[i], ms.getStoreID(), m[i]);
      }

      // get reachable reference

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = ms.getReference(m[i].getMessageID());
         ref.acquireReference();
         assertCorrectReference(ref, ms.getStoreID(), m[i]);
         ref.releaseReference();
      }

//      // send references are out of scope, request garbage collection
//      refs = null;
//      System.gc();
      
      

      // there are no strong references to the message reference anymore, so the message store
      // should be cleaned of everything that pertains to those message

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertNull(ms.getReference(m[i].getMessageID()));
      }
   }

   ////
   //// Reliable message
   ////

   //////
   ////// The non-reliable store does NOT accept reliable messages
   //////

   //////
   ////// One message
   //////

   public void testNonRecoverableMessageStore_3() throws Exception
   {
      if (ms.isRecoverable())
      {
         // we only test non-recoverable message stores
         return;
      }

      if (ms.acceptReliableMessages())
      {
         // we only test message stores that do not accept reliable message
         return;
      }

      Message m = MessageFactory.createMessage("message0", true, 777l, 888l, 9, headers, "payload");

      // non-recoverable store, reliable message, one message
      try
      {
         ms.reference(m);
         fail("should throw IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   //////
   ////// The non-reliable store accepts reliable messages
   //////

   ////////
   //////// One message
   ////////

   public void testNonRecoverableMessageStore_4() throws Exception
   {
      if (ms.isRecoverable())
      {
         // we only test non-recoverable message stores
         return;
      }

      if (!ms.acceptReliableMessages())
      {
         // we only test message stores that accept reliable message
         return;
      }

      Message m = MessageFactory.createMessage("message0", false, 777l, 888l, 9, headers, "payload");

      // non-recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, ms.getStoreID(), m);

      // get reachable reference
      ref = ms.getReference(m.getMessageID());
      assertCorrectReference(ref, ms.getStoreID(), m);

      // send reference out of scope and request garbage collection
      WeakReference control = new WeakReference(ref);
      ref = null;
      System.gc();

      // make sure the unreachable reference has been garbage collected
      assertNull(control.get());

      // there's no strong reference to the unique message reference anymore, so the message store
      // should be cleaned of everything that pertains to that message

      ref = ms.getReference(m.getMessageID());
      assertNull(ref);
   }

   ////////
   //////// Multiple messages
   ////////

   public void testNonRecoverableMessageStore_5() throws Exception
   {
      if (ms.isRecoverable())
      {
         // we only test non-recoverable message stores
         return;
      }

      if (!ms.acceptReliableMessages())
      {
         // we only test message stores that accept reliable message
         return;
      }

      Message[] m = new Message[NUMBER_OF_MESSAGES];
      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++ )
      {
         m[i] = MessageFactory.
            createMessage("message" + i, false, 700 + i, 800 + i, i % 10, headers, "payload" + i);

         // non-recoverable store, non-reliable message, one message
         refs[i] = ms.reference(m[i]);
         assertCorrectReference(refs[i], ms.getStoreID(), m[i]);
      }

      // get reachable reference

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = ms.getReference(m[i].getMessageID());
         assertCorrectReference(ref, ms.getStoreID(), m[i]);
      }

      // send references are out of scope, request garbage collection
      refs = null;
      System.gc();

      // there are no strong references to the message reference anymore, so the message store
      // should be cleaned of everything that pertains to those message

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertNull(ms.getReference(m[i].getMessageID()));
      }
   }

   //
   // Recoverable message store
   //

   ////
   //// Non-reliable message
   ////

   //////
   ////// One message
   //////

   ////////
   //////// One strong reference
   ////////

   public void testRecoverableMessageStore_1() throws Exception
   {
      if (!ms.isRecoverable())
      {
         // we only test recoverable message stores
         return;
      }

      Message m = MessageFactory.createMessage("message0", false, 777l, 888l, 9, headers, "payload");

      // recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, ms.getStoreID(), m);

      // get reachable reference
      ref = ms.getReference(m.getMessageID());
      ref.acquireReference();
      assertCorrectReference(ref, ms.getStoreID(), m);

//      // send reference out of scope and request garbage collection
//      WeakReference control = new WeakReference(ref);
//      ref = null;
//      System.gc();
      ref.releaseReference();

      // make sure the unreachable reference has been garbage collected
      //assertNull(control.get());

      // there's no strong reference to the unique message reference anymore, so the message store
      // should be cleaned of everything that pertains to that message

      ref = ms.getReference(m.getMessageID());
      assertNull(ref);
   }

   ////////
   //////// Two strong references
   ////////

   public void testRecoverableMessageStore_1_1() throws Exception
   {
      if (!ms.isRecoverable())
      {
         // we only test recoverable message stores
         return;
      }

      Message m = MessageFactory.createMessage("message0", false, 777l, 888l, 9, headers, "payload");

      // recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, ms.getStoreID(), m);

      MessageReference ref2 = ms.reference(m);

      assertTrue(ref == ref2);
   }

   //////
   ////// Multiple messages
   //////

   public void testRecoverableMessageStore_2() throws Exception
   {
      if (!ms.isRecoverable())
      {
         // we only test recoverable message stores
         return;
      }

      Message[] m = new Message[NUMBER_OF_MESSAGES];
      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++ )
      {
         m[i] = MessageFactory.
            createMessage("message" + i, false, 700 + i, 800 + i, i % 10, headers, "payload" + i);

         // recoverable store, non-reliable message, one message
         refs[i] = ms.reference(m[i]);
         assertCorrectReference(refs[i], ms.getStoreID(), m[i]);
      }

      // get reachable reference

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = ms.getReference(m[i].getMessageID());
         ref.acquireReference();
         assertCorrectReference(ref, ms.getStoreID(), m[i]);
         ref.releaseReference();
      }

//      // send references are out of scope, request garbage collection
//      refs = null;
//      System.gc();

      // there are no strong references to the message reference anymore, so the message store
      // should be cleaned of everything that pertains to those message

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertNull(ms.getReference(m[i].getMessageID()));
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

   public void testRecoverableMessageStore_3() throws Exception
   {
      if (!ms.isRecoverable())
      {
         // we only test recoverable message stores
         return;
      }

      Message m = MessageFactory.createMessage("message0", true, 777l, 888l, 9, headers, "payload");

      // recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, ms.getStoreID(), m);

      // get reachable reference
      ref = ms.getReference(m.getMessageID());
      ref.acquireReference();
      assertCorrectReference(ref, ms.getStoreID(), m);

//      // send reference out of scope and request garbage collection
//      WeakReference control = new WeakReference(ref);
//      ref = null;
//      System.gc();
      ref.releaseReference();

      // make sure the unreachable reference has been garbage collected
      //assertNull(control.get());

      // there's no strong reference to the unique message reference anymore, so the message store
      // should be cleaned of everything that pertains to that message

      ref = ms.getReference(m.getMessageID());
      assertNull(ref);
   }

   ////////
   //////// Two strong references
   ////////

   public void testRecoverableMessageStore_3_1() throws Exception
   {
      if (!ms.isRecoverable())
      {
         // we only test recoverable message stores
         return;
      }

      Message m = MessageFactory.createMessage("message0", true, 777l, 888l, 9, headers, "payload");

      // recoverable store, non-reliable message, one message
      MessageReference ref = ms.reference(m);
      assertCorrectReference(ref, ms.getStoreID(), m);

      MessageReference ref2 = ms.reference(m);

      assertTrue(ref2 == ref);
   }

   //////
   ////// Multiple messages
   //////

   public void testRecoverableMessageStore_4() throws Exception
   {
      if (!ms.isRecoverable())
      {
         // we only test recoverable message stores
         return;
      }

      Message[] m = new Message[NUMBER_OF_MESSAGES];
      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++ )
      {
         m[i] = MessageFactory.
            createMessage("message" + i, true, 700 + i, 800 + i, i % 10, headers, "payload" + i);

         // recoverable store, non-reliable message, one message
         refs[i] = ms.reference(m[i]);
         assertCorrectReference(refs[i], ms.getStoreID(), m[i]);
      }

      // get reachable reference

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = ms.getReference(m[i].getMessageID());
         ref.acquireReference();
         assertCorrectReference(ref, ms.getStoreID(), m[i]);
         ref.releaseReference();
      }

//      // send references are out of scope, request garbage collection
//      refs = null;
//      System.gc();

      // there are no strong references to the message reference anymore, so the message store
      // should be cleaned of everything that pertains to those message

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertNull(ms.getReference(m[i].getMessageID()));
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
