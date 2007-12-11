/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_ACKDELIVERY;

import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class AcknowledgeDeliveryRequestCodec extends
      AbstractPacketCodec<AcknowledgeDeliveryRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcknowledgeDeliveryRequestCodec()
   {
      super(REQ_ACKDELIVERY);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(AcknowledgeDeliveryRequest request,
         RemotingBuffer out) throws Exception
   {
      long deliveryID = request.getDeliveryID();

      out.putInt(LONG_LENGTH);
      out.putLong(deliveryID);
   }

   @Override
   protected AcknowledgeDeliveryRequest decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      long deliveryID = in.getLong();

      return new AcknowledgeDeliveryRequest(deliveryID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
