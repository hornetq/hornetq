/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging;

import org.jboss.messaging.channel.plugins.handler.ChannelHandler;
import org.jboss.messaging.channel.plugins.handler.ExclusiveChannelHandler;
import org.jboss.messaging.interfaces.Consumer;
import org.jboss.messaging.memory.MemoryMessageSet;
import org.jboss.test.jms.BaseJMSTest;

/**
 * A basic test
 * 
 * @author <a href="adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class ExclusiveChannelTestCase extends BaseJMSTest
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ExclusiveChannelTestCase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSomething()
      throws Exception
   {
      ChannelHandler handler = getExclusiveChannelHandler();
      Consumer consumer = new AcceptAllConsumer();
      TestMessageReference t1 = new TestMessageReference();
      handler.addMessage(t1);
      TestMessageReference t2 = new TestMessageReference();
      handler.addMessage(t2);
      TestMessageReference r = (TestMessageReference) handler.removeMessage(consumer);
      assertEquals(t1.getMessageID(), r.getMessageID());
      r = (TestMessageReference) handler.removeMessage(consumer);
      assertEquals(t2.getMessageID(), r.getMessageID());
   }

   // Protected ------------------------------------------------------

   protected ChannelHandler getExclusiveChannelHandler()
   {
      MemoryMessageSet mms = new MemoryMessageSet(new TestMessageReference.TestMessageReferenceComparator());
      return new ExclusiveChannelHandler(mms);
   }
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
