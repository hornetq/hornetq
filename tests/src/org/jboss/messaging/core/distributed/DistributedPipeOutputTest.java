/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.MessagingTestCase;
import org.jboss.messaging.core.Pipe;
import org.jboss.messaging.core.CoreMessage;
import org.jboss.messaging.core.ReceiverImpl;
import org.jboss.messaging.interfaces.Message;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Channel;
import org.jgroups.MessageListener;
import org.jgroups.MembershipListener;
import org.jgroups.JChannel;


import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributedPipeOutputTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public DistributedPipeOutputTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testNoRpcServer() throws Exception
   {
      JChannel channel = new JChannel();
      RpcDispatcher dispatcher = new RpcDispatcher(channel, null, null, null);

      try
      {
         new DistributedPipeOutput(dispatcher, "", null);
         fail("Should have thrown IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }
}
