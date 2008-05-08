/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_SEND;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.server.ServerMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ProducerSendMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClientMessage clientMessage;
   
   private final ServerMessage serverMessage;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerSendMessage(final ClientMessage message)
   {
      super(PROD_SEND);

      this.clientMessage = message;
      
      this.serverMessage = null;
   }
   
   public ProducerSendMessage(final ServerMessage message)
   {
      super(PROD_SEND);

      this.serverMessage = message;
      
      this.clientMessage = null;
   }

   // Public --------------------------------------------------------

   public ClientMessage getClientMessage()
   {
      return clientMessage;
   }
   
   public ServerMessage getServerMessage()
   {
      return serverMessage;
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
