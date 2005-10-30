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
