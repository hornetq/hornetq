/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_IDBLOCK;

import org.jboss.messaging.core.remoting.wireformat.IDBlockRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class IDBlockRequestCodec extends AbstractPacketCodec<IDBlockRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public IDBlockRequestCodec()
   {
      super(REQ_IDBLOCK);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(IDBlockRequest request, RemotingBuffer out) throws Exception
   {
      out.putInt(INT_LENGTH);
      out.putInt(request.getSize());
   }

   @Override
   protected IDBlockRequest decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int size = in.getInt();

      return new IDBlockRequest(size);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
