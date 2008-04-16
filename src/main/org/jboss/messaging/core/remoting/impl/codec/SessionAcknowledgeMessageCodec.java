/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;

/**
 * 
 * A SessionAcknowledgeMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
      super(PacketType.SESS_ACKNOWLEDGE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionAcknowledgeMessage packet) throws Exception
   {
      return LONG_LENGTH + 1;
   }
   
   @Override
   protected void encodeBody(final SessionAcknowledgeMessage message, final RemotingBuffer out) throws Exception
   {
      out.putLong(message.getDeliveryID());
      out.putBoolean(message.isAllUpTo());
   }

   @Override
   protected SessionAcknowledgeMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      long deliveryID = in.getLong();
      boolean isAllUpTo = in.getBoolean();
     
      return new SessionAcknowledgeMessage(deliveryID, isAllUpTo);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

