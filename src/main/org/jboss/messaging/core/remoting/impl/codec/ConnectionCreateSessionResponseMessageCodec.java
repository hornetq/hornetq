/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConnectionCreateSessionResponseMessageCodec extends
      AbstractPacketCodec<ConnectionCreateSessionResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionCreateSessionResponseMessageCodec()
   {
      super(PacketType.CONN_CREATESESSION_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final ConnectionCreateSessionResponseMessage response, final MessagingBuffer out) throws Exception
   {
      out.putLong(response.getSessionID());
   }

   @Override
   protected ConnectionCreateSessionResponseMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      return new ConnectionCreateSessionResponseMessage(in.getLong());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
