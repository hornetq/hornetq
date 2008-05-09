/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ProducerSendMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientMessage clientMessage;
   
   private ServerMessage serverMessage;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerSendMessage(final ClientMessage message)
   {
      super(PROD_SEND);

      this.clientMessage = message;
   }
   
   public ProducerSendMessage(final ServerMessage message)
   {
      super(PROD_SEND);

      this.serverMessage = message;
   }
   
   public ProducerSendMessage()
   {
      super(PROD_SEND);
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      MessagingBuffer buf = clientMessage.encode();
      
      buf.flip();
      
      //TODO - can be optimised
      
      byte[] data = buf.array();
       
      buffer.putBytes(data, 0, buf.limit());
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      //TODO can be optimised
      
      serverMessage = new ServerMessageImpl();
      
      serverMessage.decode(buffer);
      
      serverMessage.getBody().flip();
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
