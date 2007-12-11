/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATEDESTINATION;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateDestinationResponseCodec extends
      AbstractPacketCodec<CreateDestinationResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateDestinationResponseCodec()
   {
      super(RESP_CREATEDESTINATION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CreateDestinationResponse response, RemotingBuffer out) throws Exception
   {
      byte[] destination = encode(response.getDestination());

      int bodyLength = INT_LENGTH + destination.length;

      out.putInt(bodyLength);
      out.putInt(destination.length);
      out.put(destination);
   }

   @Override
   protected CreateDestinationResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int destinationLength = in.getInt();
      byte[] b = new byte[destinationLength];
      in.get(b);
      JBossDestination destination = decode(b);

      return new CreateDestinationResponse(destination);
   }

   // Inner classes -------------------------------------------------
}
