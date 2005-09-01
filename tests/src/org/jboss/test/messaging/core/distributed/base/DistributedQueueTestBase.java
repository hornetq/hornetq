/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.distributed.base;


import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.message.Factory;

import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DistributedQueueTestBase extends DistributedChannelTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DistributedQueueTestBase(String name)
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


   public void testSimpleSend() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      channelTwo.add(r);

      ((Peer)channel).join();
      channelTwo.join();

      Delivery d = channel.handle(observer, Factory.createMessage("message0", false, "payload"));

      assertTrue(d.isDone());
      List l = r.getMessages();
      assertEquals(1, l.size());
      Message m = (Message)l.get(0);
      assertEquals("payload", m.getPayload());
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
