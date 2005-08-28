/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.client;

import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Address;
import org.jgroups.MessageListener;
import org.jgroups.Message;
import org.jgroups.MembershipListener;
import org.jgroups.View;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RpcDispatcherClient extends JChannelClient
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected RpcDispatcher rpcDispatcher;

   // Constructors --------------------------------------------------

   public RpcDispatcherClient(Object serverObject) throws Exception
   {
      super();
      rpcDispatcher = new RpcDispatcher(jChannel,
                                     new MessageListenerImpl(),
                                     new MembershipListenerImpl(),
                                     serverObject);
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   private class MessageListenerImpl implements MessageListener
   {
      public void receive(Message message)
      {
         System.out.println("receive("+message+")");
      }

      public byte[] getState()
      {
         return new byte[0];
      }

      public void setState(byte[] bytes)
      {
      }
   }

   private class MembershipListenerImpl implements MembershipListener
   {
      public void viewAccepted(View view)
      {
         System.out.println("viewAccepted("+view+")");
      }

      public void suspect(Address address)
      {
         System.out.println("suspect("+address+")");
      }

      public void block()
      {

      }
   }
}
