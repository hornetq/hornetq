/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * 
 * A SessionCancelMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionCancelMessageCodec extends AbstractPacketCodec<SessionCancelMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCancelMessageCodec()
   {
      super(PacketType.SESS_CANCEL);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------
 
   @Override
   protected void encodeBody(final SessionCancelMessage message, final MessagingBuffer out) throws Exception
   {
      out.putLong(message.getDeliveryID());
      out.putBoolean(message.isExpired());
   }

   @Override
   protected SessionCancelMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      long deliveryID = in.getLong();
      boolean expired = in.getBoolean();
     
      return new SessionCancelMessage(deliveryID, expired);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

