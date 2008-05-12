/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ReceiveMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientMessage clientMessage;
   
   private ServerMessage serverMessage;
   
   private int deliveryCount;
   
   private long deliveryID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReceiveMessage(final ClientMessage message)
   {
      super(RECEIVE_MSG);

      this.clientMessage = message;
      
      this.serverMessage = null;
      
      this.deliveryCount = -1;
      
      this.deliveryID = -1;
   }
   
   public ReceiveMessage(final ServerMessage message, final int deliveryCount, final long deliveryID)
   {
      super(RECEIVE_MSG);

      this.serverMessage = message;
      
      this.clientMessage = null;
      
      this.deliveryCount = deliveryCount;
      
      this.deliveryID = deliveryID;
   }
   
   public ReceiveMessage()
   {
      super(RECEIVE_MSG);
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

   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public long getDeliveryID()
   {
      return deliveryID;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(deliveryCount);
      buffer.putLong(deliveryID);
      serverMessage.encode(buffer);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      //TODO can be optimised
      
      deliveryCount = buffer.getInt();
      deliveryID = buffer.getLong();
      
      clientMessage = new ClientMessageImpl(deliveryCount, deliveryID);
      
      clientMessage.decode(buffer);
      
      clientMessage.getBody().flip();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
