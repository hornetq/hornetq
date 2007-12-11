/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEDESTINATION;

import org.jboss.messaging.core.remoting.wireformat.CreateDestinationRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateDestinationRequestCodec extends
      AbstractPacketCodec<CreateDestinationRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateDestinationRequestCodec()
   {
      super(REQ_CREATEDESTINATION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CreateDestinationRequest request,
         RemotingBuffer out) throws Exception
   {
      String name = request.getName();
      boolean isQueue = request.isQueue();

      int bodyLength = sizeof(name) + 1;

      out.putInt(bodyLength);
      out.putNullableString(name);
      out.putBoolean(isQueue);
   }

   @Override
   protected CreateDestinationRequest decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String name = in.getNullableString();
      boolean isQueue = in.getBoolean();

      return new CreateDestinationRequest(name, isQueue);
   }

   // Inner classes -------------------------------------------------
}
