/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutResponseMessage;

/**
 * 
 * A SessionXASetTimeoutResponseMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXASetTimeoutResponseMessageCodec extends AbstractPacketCodec<SessionXASetTimeoutResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXASetTimeoutResponseMessageCodec()
   {
      super(PacketType.SESS_XA_SET_TIMEOUT_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXASetTimeoutResponseMessage message, RemotingBuffer out) throws Exception
   {                 
      int bodyLength = 1;
      
      out.putInt(bodyLength);
      
      out.putBoolean(message.isOK());
   }

   @Override
   protected SessionXASetTimeoutResponseMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      boolean ok = in.getBoolean();
      
      return new SessionXASetTimeoutResponseMessage(ok);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


