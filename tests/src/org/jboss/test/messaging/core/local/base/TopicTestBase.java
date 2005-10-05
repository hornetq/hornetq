/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.local.base;

import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.message.Factory;

import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public abstract class TopicTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // TODO here I should have a Destination base class so I won't have to cast it to Distributor
   protected Receiver topic;

   // Constructors --------------------------------------------------
   
   public TopicTestBase(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testUnreliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      ((Distributor)topic).add(r1);
      ((Distributor)topic).add(r2);

      Delivery d = topic.handle(observer, Factory.createMessage("message0", false, "payload"));

      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();

      assertEquals(1, l1.size());
      Message m = (Message)l1.get(0);
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
      assertTrue(((Distributor)topic).add(r1));
      assertTrue(((Distributor)topic).add(r2));

      Delivery d = topic.handle(observer, Factory.createMessage("message0", true, "payload"));

      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();

      assertEquals(1, l1.size());
      Message m = (Message)l1.get(0);
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
