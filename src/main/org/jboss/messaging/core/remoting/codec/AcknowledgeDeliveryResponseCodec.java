/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_ACKDELIVERY;

import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class AcknowledgeDeliveryResponseCodec extends
      AbstractPacketCodec<AcknowledgeDeliveryResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcknowledgeDeliveryResponseCodec()
   {
      super(RESP_ACKDELIVERY);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(AcknowledgeDeliveryResponse response,
         RemotingBuffer out) throws Exception
   {
      out.putInt(1); //body length
      out.putBoolean(response.isAcknowledged());
   }

   @Override
   protected AcknowledgeDeliveryResponse decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      boolean acknowledged = in.getBoolean();

      return new AcknowledgeDeliveryResponse(acknowledged);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
