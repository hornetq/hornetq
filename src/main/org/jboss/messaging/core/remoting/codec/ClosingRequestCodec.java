/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CLOSING;

import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class ClosingRequestCodec extends AbstractPacketCodec<ClosingRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClosingRequestCodec()
   {
      super(REQ_CLOSING);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(ClosingRequest request, RemotingBuffer out) throws Exception
   {
      out.putInt(LONG_LENGTH);
      out.putLong(request.getSequence());
   }

   @Override
   protected ClosingRequest decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      long sequence = in.getLong();
      
      return new ClosingRequest(sequence);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
