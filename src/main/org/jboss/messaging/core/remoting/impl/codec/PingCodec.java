/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PING;

import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
   protected void encodeBody(final Ping packet, final MessagingBuffer out) throws Exception
   {
      long clientSessionID = packet.getSessionID();

      out.putLong(clientSessionID);
   }

   @Override
   protected Ping decodeBody(final MessagingBuffer in) throws Exception
   {
      long clientSessionID = in.getLong();

      return new Ping(clientSessionID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
