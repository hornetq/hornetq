/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PING;

import org.jboss.messaging.core.remoting.impl.wireformat.Ping;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class PingCodec extends AbstractPacketCodec<Ping>
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PingCodec()
   {
      super(PING);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(Ping packet, RemotingBuffer out)
         throws Exception
   {
      String clientSessionID = packet.getSessionID();

      out.putInt(sizeof(clientSessionID));
      out.putNullableString(clientSessionID);
   }

   @Override
   protected Ping decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (bodyLength > in.remaining())
      {
         return null;
      }
      String clientSessionID = in.getNullableString();

      return new Ping(clientSessionID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
