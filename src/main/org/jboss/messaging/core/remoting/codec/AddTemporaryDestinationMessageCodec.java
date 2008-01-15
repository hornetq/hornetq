/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ADDTEMPORARYDESTINATION;

import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.remoting.wireformat.AddTemporaryDestinationMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class AddTemporaryDestinationMessageCodec extends
      AbstractPacketCodec<AddTemporaryDestinationMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AddTemporaryDestinationMessageCodec()
   {
      super(MSG_ADDTEMPORARYDESTINATION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(AddTemporaryDestinationMessage message, RemotingBuffer out) throws Exception
   {
      byte[] destination = encode(message.getDestination());

      int bodyLength = INT_LENGTH + destination.length;

      out.putInt(bodyLength);
      out.putInt(destination.length);
      out.put(destination);
   }

   @Override
   protected AddTemporaryDestinationMessage decodeBody(RemotingBuffer in)
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
      Destination destination = decode(b);

      return new AddTemporaryDestinationMessage(destination);
   }

   // Inner classes -------------------------------------------------
}
