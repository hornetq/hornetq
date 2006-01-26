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
import org.jboss.messaging.core.plugin.contract.TransactionLog;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.JDBCTransactionLog;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.jms.server.plugin.PersistentMessageStore;
import org.jboss.jms.server.plugin.JDBCMessageStore;
import org.jboss.jms.server.plugin.contract.MessageStore;

import java.util.Iterator;

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

   protected TransactionLog tl;
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

      sc = new ServiceContainer("all,-remoting,-security");
      sc.start();

      tl = new JDBCTransactionLog(sc.getDataSource(), sc.getTransactionManager());
      ((JDBCTransactionLog)tl).start();
      
      ms = new JDBCMessageStore("s33", sc.getDataSource(), sc.getTransactionManager());
      ((JDBCMessageStore)ms).start();

   }

   public void tearDown() throws Exception
   {
      tl = null;
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



   //
   // Zero receivers
   //

   ////
   //// Unreliable message
   ////

   public void testTopic_1() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      assertFalse(topic.iterator().hasNext());

      Message m = MessageFactory.createMessage("message0", false, "payload");
      Delivery d = topic.handle(observer, ms.reference(m), null);

      assertTrue(d.isDone());
   }

   ////
   //// Reliable message
   ////

   // a reliable message is not handled differently

   //
   // One receiver
   //

   ////
   //// Unreliable message
   ////

   public void testTopic_2() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      SimpleReceiver r = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      assertTrue(topic.add(r));

      Iterator i = topic.iterator();
      assertEquals(r, i.next());
      assertFalse(i.hasNext());

      Message m = MessageFactory.createMessage("message0", false, "payload0");
      Delivery d = topic.handle(observer, ms.reference(m), null);

      assertTrue(d.isDone());

      assertEquals(1, r.getMessages().size());
      assertEquals("message0", ((Message)r.getMessages().get(0)).getMessageID());
      assertEquals("payload0", ((Message)r.getMessages().get(0)).getPayload());
   }

   ////
   //// Reliable message
   ////

   // a reliable message is not handled differently

   //
   // Two receivers
   //
   
   public void testTopic_3() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      SimpleReceiver r = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("ACKING2", SimpleReceiver.ACKING);

      assertTrue(topic.add(r));
      assertTrue(topic.add(r2));

      Message m = MessageFactory.createMessage("message0", false, "payload0");
      Delivery d = topic.handle(observer, ms.reference(m), null);

      assertTrue(d.isDone());

      assertEquals(1, r.getMessages().size());
      assertEquals("message0", ((Message)r.getMessages().get(0)).getMessageID());
      assertEquals("payload0", ((Message)r.getMessages().get(0)).getPayload());

      assertEquals(1, r2.getMessages().size());
      assertEquals("message0", ((Message)r2.getMessages().get(0)).getMessageID());
      assertEquals("payload0", ((Message)r2.getMessages().get(0)).getPayload());
   }

   ////
   //// Reliable message
   ////

   // a reliable message is not handled differently

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
