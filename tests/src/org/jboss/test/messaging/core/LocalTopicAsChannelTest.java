/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.core.LocalTopic;
import org.jboss.messaging.core.MessageSupport;

import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalTopicAsChannelTest extends ChannelSupportTest
{
   // Constructors --------------------------------------------------

   public LocalTopicAsChannelTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();

      // Create a LocalTopic to be tested by the superclass tests
      channel = new LocalTopic("LocalTopicID");
      receiverOne = new ReceiverImpl("ReceiverOne", ReceiverImpl.HANDLING);
      ((LocalTopic)channel).add(receiverOne);
   }

   public void tearDown()throws Exception
   {
      ((LocalTopic)channel).clear();
      channel = null;
      receiverOne = null;
      super.tearDown();
   }

   //
   // This test also runs all ChannelSupportTest's tests
   //

   public void testDefaultAsynchronous()
   {
      assertTrue(channel.isSynchronous());
   }


   public void testTopic() throws Exception
   {
      LocalTopic topic = new LocalTopic("");

      // send without a receiver

      Routable m = new MessageSupport("");
      assertFalse(topic.handle(m));

      // send with one receiver

      ReceiverImpl rOne = new ReceiverImpl("ReceiverONE", ReceiverImpl.HANDLING);
      assertTrue(topic.add(rOne));

      m = new MessageSupport("");
      assertTrue(topic.handle(m));

      Iterator i = rOne.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl("ReceiverTWO", ReceiverImpl.HANDLING);
      assertTrue(topic.add(rTwo));

      m = new MessageSupport("");
      assertTrue(topic.handle(m));

      Iterator iOne = rOne.iterator();
      assertTrue(m == iOne.next());
      assertFalse(iOne.hasNext());

      Iterator iTwo = rTwo.iterator();
      assertTrue(m == iTwo.next());
      assertFalse(iTwo.hasNext());
   }


   public void testDenyingReceiver() throws Exception
   {
      LocalTopic topic = new LocalTopic("");

      ReceiverImpl denying = new ReceiverImpl("ReceiverONE", ReceiverImpl.DENYING);
      assertTrue(topic.add(denying));

      Routable m = new MessageSupport("");
      assertFalse(topic.handle(m));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(topic.acknowledged(denying.getReceiverID()));

      ReceiverImpl handling = new ReceiverImpl("ReceiverTWO", ReceiverImpl.HANDLING);
      assertTrue(topic.add(handling));
      assertFalse(topic.handle(m));

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(topic.acknowledged(denying.getReceiverID()));
      assertTrue(topic.acknowledged(handling.getReceiverID()));
   }

   public void testBrokenReceiver() throws Exception
   {
      LocalTopic topic = new LocalTopic("");

      ReceiverImpl broken = new ReceiverImpl("ReceiverONE", ReceiverImpl.BROKEN);
      assertTrue(topic.add(broken));

      Routable m = new MessageSupport("");
      assertFalse(topic.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(topic.acknowledged(broken.getReceiverID()));

      ReceiverImpl handling = new ReceiverImpl("ReceiverTWO", ReceiverImpl.HANDLING);
      assertTrue(topic.add(handling));
      assertFalse(topic.handle(m));

      i = broken.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(topic.acknowledged(broken.getReceiverID()));
      assertTrue(topic.acknowledged(handling.getReceiverID()));
   }
}
