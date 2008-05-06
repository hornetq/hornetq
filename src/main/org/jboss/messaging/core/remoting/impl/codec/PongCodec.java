/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PONG;
import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;
import static org.jboss.messaging.util.DataConstants.SIZE_LONG;

import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
   
   public int getBodyLength(final Pong packet) throws Exception
   {
   	return SIZE_LONG + SIZE_BOOLEAN;
   }

   @Override
   protected void encodeBody(final Pong packet, final RemotingBuffer out) throws Exception
   {
      long sessionID = packet.getSessionID();
      boolean sessionFailed = packet.isSessionFailed();

      out.putLong(sessionID);
      out.putBoolean(sessionFailed);
   }

   @Override
   protected Pong decodeBody(final RemotingBuffer in) throws Exception
   {
      long sessionID = in.getLong();
      boolean sessionFailed = in.getBoolean();
      return new Pong(sessionID, sessionFailed);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
