/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.util.RpcServer;
import org.jboss.messaging.core.distributed.Queue;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.JChannel;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class InvalidQueueTest extends MessagingTestCase
{


   // Constructors --------------------------------------------------

   public InvalidQueueTest(String name)
   {
      super(name);
   }

   // Protected -----------------------------------------------------

   // Public --------------------------------------------------------

   public void testNoRpcServer() throws Exception
   {
      JChannel channel = new JChannel();
      RpcDispatcher dispatcher = new RpcDispatcher(channel, null, null, null);

      try
      {
         new Queue(dispatcher, "doesntmatter");
         fail("Should have thrown IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   public void testConnectWithTheJChannelNotConnected() throws Exception
   {
      JChannel jChannel = new JChannel();
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, new RpcServer());
      assertFalse(jChannel.isConnected());
      Queue peerOne = new Queue(dispatcher, "doesntmatter");

      try
      {
         peerOne.connect();
         fail("Should have thrown DistributedException");
      }
      catch(DistributedException e)
      {
         // Ok
      }
   }

}
