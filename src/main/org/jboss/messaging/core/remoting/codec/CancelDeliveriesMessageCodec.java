/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERIES;

import java.util.ArrayList;
import java.util.List;

import org.jboss.jms.client.impl.Cancel;
import org.jboss.jms.client.impl.CancelImpl;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveriesMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$F</tt>
 */
public class CancelDeliveriesMessageCodec extends
      AbstractPacketCodec<CancelDeliveriesMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CancelDeliveriesMessageCodec()
   {
      super(MSG_CANCELDELIVERIES);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CancelDeliveriesMessage message,
         RemotingBuffer out) throws Exception
   {
      List<Cancel> cancels = message.getCancels();

      int numOfCancels = cancels.size();
      int cancelLength = LONG_LENGTH + INT_LENGTH + 1 + 1; // 2 booleans
      int bodyLength = INT_LENGTH + cancelLength * numOfCancels;

      out.putInt(bodyLength);
      out.putInt(numOfCancels);
      for (Cancel cancel : cancels)
      {
         out.putLong(cancel.getDeliveryId());
         out.putInt(cancel.getDeliveryCount());
         out.putBoolean(cancel.isExpired());
         out.putBoolean(cancel.isReachedMaxDeliveryAttempts());
      }
   }

   @Override
   protected CancelDeliveriesMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int numOfCancels = in.getInt();
      List<Cancel> cancels = new ArrayList<Cancel>(numOfCancels);
      for (int i = 0; i < numOfCancels; i++)
      {
         long deliveryID = in.getLong();
         int deliveryCount = in.getInt();
         boolean expired = in.getBoolean();
         boolean reachedMaxDeliveryAttempts = in.getBoolean();

         cancels.add(new CancelImpl(deliveryID, deliveryCount, expired,
               reachedMaxDeliveryAttempts));
      }

      return new CancelDeliveriesMessage(cancels);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
