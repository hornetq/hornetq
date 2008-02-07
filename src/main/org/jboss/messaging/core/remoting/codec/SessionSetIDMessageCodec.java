/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_SETID;

import org.jboss.messaging.core.remoting.wireformat.SessionSetIDMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class SessionSetIDMessageCodec extends
      AbstractPacketCodec<SessionSetIDMessage>
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSetIDMessageCodec()
   {
      super(SESS_SETID);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionSetIDMessage message, RemotingBuffer out)
         throws Exception
   {
      String sessionID = message.getSessionID();

      out.putInt(sizeof(sessionID));
      out.putNullableString(sessionID);
   }

   @Override
   protected SessionSetIDMessage decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (bodyLength > in.remaining())
      {
         return null;
      }
      String sessionID = in.getNullableString();

      return new SessionSetIDMessage(sessionID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}