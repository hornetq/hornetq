/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.MessagingTestCase;
import org.jboss.messaging.interfaces.Message;


import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PipeTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public PipeTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testAsynchronousPipe() throws Exception
   {
      Pipe p = new Pipe();
      assertFalse(p.isSynchronous());

      // send without a receiver

      Message m = new CoreMessage(new Integer(0));
      assertTrue(p.handle(m));
      assertTrue(p.hasMessages());
      assertFalse(p.deliver());
      assertTrue(p.hasMessages());

      // try to switch the mode to asynchronous while the pipe still holds messages

      assertFalse(p.setSynchronous(true));
      assertFalse(p.isSynchronous());

      // attach a receiver

      ReceiverImpl r = new ReceiverImpl();
      p.setReceiver(r);

      assertFalse(p.hasMessages());

      Iterator i = r.iterator();
      m = (Message)i.next();
      assertFalse(i.hasNext());
      assertEquals(m.getMessageID(), new Integer(0));
   }


   public void testSynchronousPipe() throws Exception
   {
      Pipe p = new Pipe(true);
      assertTrue(p.isSynchronous());

      // send without a receiver

      Message m = new CoreMessage(new Integer(0));
      assertFalse(p.handle(m));
      assertFalse(p.hasMessages());
      assertTrue(p.deliver());

      // send with a receiver

      ReceiverImpl r = new ReceiverImpl();
      p.setReceiver(r);
      m = new CoreMessage(new Integer(1));
      assertTrue(p.handle(m));

      assertFalse(p.hasMessages());

      Iterator i = r.iterator();
      m = (Message)i.next();
      assertFalse(i.hasNext());
      assertEquals(m.getMessageID(), new Integer(1));
   }
}
