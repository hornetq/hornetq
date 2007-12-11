/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELETETEMPORARYDESTINATION;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.core.remoting.wireformat.DeleteTemporaryDestinationMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class DeleteTemporaryDestinationMessageCodec extends
      AbstractPacketCodec<DeleteTemporaryDestinationMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DeleteTemporaryDestinationMessageCodec()
   {
      super(MSG_DELETETEMPORARYDESTINATION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(DeleteTemporaryDestinationMessage message,
         RemotingBuffer out) throws Exception
   {
      byte[] destination = encode(message.getDestination());

      int bodyLength = INT_LENGTH + destination.length;

      out.putInt(bodyLength);
      out.putInt(destination.length);
      out.put(destination);
   }

   @Override
   protected DeleteTemporaryDestinationMessage decodeBody(RemotingBuffer in)
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

      return new DeleteTemporaryDestinationMessage(destination);
   }

   // Inner classes -------------------------------------------------
}
