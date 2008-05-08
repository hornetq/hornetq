/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.RECEIVE_MSG;

import org.jboss.messaging.core.message.ClientMessage;
import org.jboss.messaging.core.message.ServerMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ReceiveMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClientMessage clientMessage;
   
   private final ServerMessage serverMessage;
   
   private final int deliveryCount;
   
   private final long deliveryID;

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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
