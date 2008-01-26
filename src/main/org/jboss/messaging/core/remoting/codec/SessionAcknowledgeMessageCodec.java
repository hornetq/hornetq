/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;

/**
 * 
 * A SessionAcknowledgeMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionAcknowledgeMessageCodec extends AbstractPacketCodec<SessionAcknowledgeMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAcknowledgeMessageCodec()
   {
      super(PacketType.MSG_ACKNOWLEDGE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionAcknowledgeMessage message, RemotingBuffer out) throws Exception
   {
      int bodyLength = LONG_LENGTH + 1;

      out.putInt(bodyLength);
      out.putLong(message.getDeliveryID());
      out.putBoolean(message.isAllUpTo());
   }

   @Override
   protected SessionAcknowledgeMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      long deliveryID = in.getLong();
      boolean isAllUpTo = in.getBoolean();
     
      return new SessionAcknowledgeMessage(deliveryID, isAllUpTo);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

