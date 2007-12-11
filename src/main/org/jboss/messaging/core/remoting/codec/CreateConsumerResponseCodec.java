/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATECONSUMER;

import org.jboss.messaging.core.remoting.wireformat.CreateConsumerResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateConsumerResponseCodec extends
      AbstractPacketCodec<CreateConsumerResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConsumerResponseCodec()
   {
      super(RESP_CREATECONSUMER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CreateConsumerResponse response,
         RemotingBuffer out) throws Exception
   {
      String consumerID = response.getConsumerID();
      int bufferSize = response.getBufferSize();
      int maxDeliveries = response.getMaxDeliveries();
      long redeliveryDelay = response.getRedeliveryDelay();

      int bodyLength = sizeof(consumerID) + 2 * INT_LENGTH
            + LONG_LENGTH;

      out.putInt(bodyLength);
      out.putNullableString(consumerID);
      out.putInt(bufferSize);
      out.putInt(maxDeliveries);
      out.putLong(redeliveryDelay);
   }

   @Override
   protected CreateConsumerResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String consumerID = in.getNullableString();
      int bufferSize = in.getInt();
      int maxDeliveries = in.getInt();
      long redeliveryDelay = in.getLong();

      return new CreateConsumerResponse(consumerID, bufferSize, maxDeliveries,
            redeliveryDelay);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
