/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
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
   protected void encodeBody(ConnectionCreateSessionResponseMessage response, RemotingBuffer out) throws Exception
   {
      long sessionID = response.getSessionID();

      out.putInt(LONG_LENGTH);
      
      out.putLong(sessionID);
   }

   @Override
   protected ConnectionCreateSessionResponseMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      long sessionID = in.getLong();

      return new ConnectionCreateSessionResponseMessage(sessionID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
