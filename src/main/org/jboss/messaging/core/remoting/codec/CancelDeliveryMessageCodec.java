/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERY;

import org.jboss.jms.client.impl.Cancel;
import org.jboss.jms.client.impl.CancelImpl;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveryMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CancelDeliveryMessageCodec extends
      AbstractPacketCodec<CancelDeliveryMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CancelDeliveryMessageCodec()
   {
      super(MSG_CANCELDELIVERY);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CancelDeliveryMessage message, RemotingBuffer out) throws Exception
   {
      long deliveryID = message.getCancel().getDeliveryId();
      int deliveryCount = message.getCancel().getDeliveryCount();
      boolean expired = message.getCancel().isExpired();
      boolean reachedMaxDeliveryAttempts = message.getCancel()
            .isReachedMaxDeliveryAttempts();

      int bodyLength = LONG_LENGTH + INT_LENGTH + 2;

      out.putInt(bodyLength);
      out.putLong(deliveryID);
      out.putInt(deliveryCount);
      out.putBoolean(expired);
      out.putBoolean(reachedMaxDeliveryAttempts);
   }

   @Override
   protected CancelDeliveryMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      long deliveryID = in.getLong();
      int deliveryCount = in.getInt();
      boolean expired = in.getBoolean();
      boolean reachedMaxDeliveryAttempts = in.getBoolean();

      Cancel cancel = new CancelImpl(deliveryID, deliveryCount, expired,
            reachedMaxDeliveryAttempts);
      return new CancelDeliveryMessage(cancel);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
