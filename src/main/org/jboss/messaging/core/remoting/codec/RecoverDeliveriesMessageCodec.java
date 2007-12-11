/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.jboss.jms.delegate.DeliveryRecovery;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.RecoverDeliveriesMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class RecoverDeliveriesMessageCodec extends
      AbstractPacketCodec<RecoverDeliveriesMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RecoverDeliveriesMessageCodec()
   {
      super(PacketType.MSG_RECOVERDELIVERIES);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(RecoverDeliveriesMessage message,
         RemotingBuffer out) throws Exception
   {
      List<DeliveryRecovery> deliveries = message.getDeliveries();
      String sessionID = message.getSessionID();
      byte[] encodedDeliveries = encode(deliveries);

      int bodyLength = INT_LENGTH + INT_LENGTH + encodedDeliveries.length
            + sizeof(sessionID);

      out.putInt(bodyLength);
      out.putInt(deliveries.size());
      out.putInt(encodedDeliveries.length);
      out.put(encodedDeliveries);
      out.putNullableString(sessionID);
   }

   @Override
   protected RecoverDeliveriesMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int deliveriesSize = in.getInt();
      int encodedDeliveriesSize = in.getInt();
      byte[] encodedDeliveries = new byte[encodedDeliveriesSize];
      in.get(encodedDeliveries);
      List<DeliveryRecovery> deliveries = decode(deliveriesSize,
            encodedDeliveries);
      String sessionID = in.getNullableString();

      return new RecoverDeliveriesMessage(deliveries, sessionID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static byte[] encode(List<DeliveryRecovery> deliveries)
         throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      for (DeliveryRecovery delivery : deliveries)
      {
         delivery.write(dos);
      }
      baos.flush();
      return baos.toByteArray();
   }

   private List<DeliveryRecovery> decode(int size, byte[] encodedDeliveries)
         throws Exception
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(encodedDeliveries);
      DataInputStream dis = new DataInputStream(bais);

      List<DeliveryRecovery> deliveries = new ArrayList<DeliveryRecovery>();
      for (int i = 0; i < size; i++)
      {
         DeliveryRecovery delivery = new DeliveryRecovery();
         delivery.read(dis);
         deliveries.add(delivery);
      }
      return deliveries;
   }

   // Inner classes -------------------------------------------------
}
