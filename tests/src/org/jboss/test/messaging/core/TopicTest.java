/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.ReceiverImpl;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.core.Topic;
import org.jboss.messaging.core.CoreMessage;


import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class TopicTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public TopicTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testTopic() throws Exception
   {
      Topic topic = new Topic();

      // send without a receiver

      Message m = new CoreMessage("");
      assertFalse(topic.handle(m));

      // send with one receiver

      ReceiverImpl rOne = new ReceiverImpl();
      assertTrue(topic.add(rOne));

      m = new CoreMessage("");
      assertTrue(topic.handle(m));

      Iterator i = rOne.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl();
      assertTrue(topic.add(rTwo));

      m = new CoreMessage("");
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
      Topic topic = new Topic();

      ReceiverImpl denying = new ReceiverImpl(ReceiverImpl.DENYING);
      assertTrue(topic.add(denying));

      Message m = new CoreMessage("");
      assertFalse(topic.handle(m));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(topic.acknowledged(denying));

      ReceiverImpl handling = new ReceiverImpl(ReceiverImpl.HANDLING);
      assertTrue(topic.add(handling));
      assertFalse(topic.handle(m));

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(topic.acknowledged(denying));
      assertTrue(topic.acknowledged(handling));
   }

   public void testBrokenReceiver() throws Exception
   {
      Topic topic = new Topic();

      ReceiverImpl broken = new ReceiverImpl(ReceiverImpl.BROKEN);
      assertTrue(topic.add(broken));

      Message m = new CoreMessage("");
      assertFalse(topic.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(topic.acknowledged(broken));

      ReceiverImpl handling = new ReceiverImpl(ReceiverImpl.HANDLING);
      assertTrue(topic.add(handling));
      assertFalse(topic.handle(m));

      i = broken.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(topic.acknowledged(broken));
      assertTrue(topic.acknowledged(handling));
   }
}
