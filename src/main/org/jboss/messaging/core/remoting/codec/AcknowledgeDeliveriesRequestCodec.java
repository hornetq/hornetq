/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ACKDELIVERIES;

import java.util.ArrayList;
import java.util.List;

import org.jboss.jms.client.impl.Ack;
import org.jboss.jms.client.impl.AckImpl;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveriesMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class AcknowledgeDeliveriesRequestCodec extends
      AbstractPacketCodec<AcknowledgeDeliveriesMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static long[] convert(List<Ack> acks)
   {
      long[] deliveryIDs = new long[acks.size()];
      for (int i = 0; i < acks.size(); i++)
      {
         deliveryIDs[i] = acks.get(i).getDeliveryID();
      }
      return deliveryIDs;
   }

   // Constructors --------------------------------------------------

   public AcknowledgeDeliveriesRequestCodec()
   {
      super(MSG_ACKDELIVERIES);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(AcknowledgeDeliveriesMessage request,
         RemotingBuffer out) throws Exception
   {

      long[] deliveryIDs = convert(request.getAcks());

      int bodyLength = LONG_LENGTH * deliveryIDs.length;
      out.putInt(bodyLength);
      for (long id : deliveryIDs)
      {
         out.putLong(id);
      }
   }

   @Override
   protected AcknowledgeDeliveriesMessage decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int numOfDeliveryIds = bodyLength / LONG_LENGTH;
      long[] deliveryIDs = new long[numOfDeliveryIds];
      for (int i = 0; i < numOfDeliveryIds; i++)
      {
         deliveryIDs[i] = in.getLong();
      }
      List<Ack> acks = convert(deliveryIDs);

      return new AcknowledgeDeliveriesMessage(acks);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private List<Ack> convert(long[] deliveryIDs)
   {
      List<Ack> acks = new ArrayList<Ack>(deliveryIDs.length);
      for (long deliveryID : deliveryIDs)
      {
         acks.add(new AckImpl(deliveryID));
      }
      return acks;
   }
   // Inner classes -------------------------------------------------
}
