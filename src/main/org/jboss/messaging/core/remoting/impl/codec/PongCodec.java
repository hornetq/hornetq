/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PONG;

import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class PongCodec extends AbstractPacketCodec<Pong>
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PongCodec()
   {
      super(PONG);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(Pong packet, RemotingBuffer out) throws Exception
   {
      String sessionID = packet.getSessionID();
      boolean sessionFailed = packet.isSessionFailed();

      int bodyLength = sizeof(sessionID) + BOOLEAN_LENGTH;

      out.putInt(bodyLength);
      out.putNullableString(sessionID);
      out.putBoolean(sessionFailed);
   }

   @Override
   protected Pong decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (bodyLength > in.remaining())
      {
         return null;
      }
      String sessionID = in.getNullableString();
      boolean sessionFailed = in.getBoolean();
      return new Pong(sessionID, sessionFailed);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
