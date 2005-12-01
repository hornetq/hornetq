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
package org.jboss.test.messaging.core.local.base;

import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.persistence.JDBCPersistenceManager;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.message.PersistentMessageStore;

import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class TopicTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;

   protected CoreDestination topic;

   protected PersistenceManager pm;
   protected MessageStore ms;

   // Constructors --------------------------------------------------
   
   public TopicTestBase(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all,-aop,-remoting,-security");
      sc.start();

      pm = new JDBCPersistenceManager();
      pm.start();
      ms = new PersistentMessageStore("persistent-store", pm);
   }

   public void tearDown() throws Exception
   {
      pm = null;
      ms = null;
      sc.stop();
      sc = null;
      super.tearDown();
   }


   /**
    * Rejecting receivers are ignored.
    */
   public void testRejectingReceiver() throws Exception
   {
      SimpleReceiver rec = new SimpleReceiver("REJECTING", SimpleReceiver.REJECTING);
      topic.add(rec);

      Message m = MessageFactory.createMessage("message0", false, "payload");
      MessageReference ref = ms.reference(m);

      Delivery d = topic.handle(null, ref, null);
      assertTrue(d.isDone());
   }

   /**
    * Broken receivers are ignored.
    */
   public void testBrokenReceiver() throws Exception
   {
      SimpleReceiver rec = new SimpleReceiver("BROKEN", SimpleReceiver.BROKEN);
      topic.add(rec);

      Message m = MessageFactory.createMessage("message0", false, "payload");
      MessageReference ref = ms.reference(m);

      Delivery d = topic.handle(null, ref, null);
      assertTrue(d.isDone());
   }

   /**
    * No NACKING receiver must be allowed by a topic. If this situation occurs, the topic must
    * throw IllegalStateException.
    */
   public void testNACKINGReceiver() throws Exception
   {
      SimpleReceiver rec = new SimpleReceiver("NACKING", SimpleReceiver.NACKING);
      topic.add(rec);

      Message m = MessageFactory.createMessage("message0", false, "payload");
      MessageReference ref = ms.reference(m);

      try
      {
         topic.handle(null, ref, null);
         fail("this should throw exception");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   public void testUnreliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      topic.add(r1);
      topic.add(r2);

      Message m = MessageFactory.createMessage("message0", false, "payload");
      Delivery d = topic.handle(observer, ms.reference(m), null);

      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();

      assertEquals(1, l1.size());
      m = (Message)l1.get(0);
      assertEquals("payload", m.getPayload());

      assertEquals(1, l2.size());
      m = (Message)l2.get(0);
      assertEquals("payload", m.getPayload());
   }


   public void testReliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      assertTrue(topic.add(r1));
      assertTrue(topic.add(r2));

      Message m = MessageFactory.createMessage("message0", true, "payload");
      Delivery d = topic.handle(observer, ms.reference(m), null);

      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();

      assertEquals(1, l1.size());
      m = (Message)l1.get(0);
      assertEquals("payload", m.getPayload());

      assertEquals(1, l2.size());
      m = (Message)l2.get(0);
      assertEquals("payload", m.getPayload());
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
