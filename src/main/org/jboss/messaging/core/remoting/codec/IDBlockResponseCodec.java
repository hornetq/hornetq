/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_IDBLOCK;

import org.jboss.messaging.core.remoting.wireformat.IDBlockResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class IDBlockResponseCodec extends AbstractPacketCodec<IDBlockResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public IDBlockResponseCodec()
   {
      super(RESP_IDBLOCK);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(IDBlockResponse response, RemotingBuffer out) throws Exception
   {
      out.putInt(LONG_LENGTH * 2);
      out.putLong(response.getLow());
      out.putLong(response.getHigh());
   }

   @Override
   protected IDBlockResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      long low = in.getLong();
      long high = in.getLong();

      return new IDBlockResponse(low, high);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
