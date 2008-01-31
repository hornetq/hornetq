/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutResponse;

/**
 * 
 * A SessionXASetTimeoutResponseCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXASetTimeoutResponseCodec extends AbstractPacketCodec<SessionXASetTimeoutResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXASetTimeoutResponseCodec()
   {
      super(PacketType.MSG_XA_SET_TIMEOUT_RESPONSE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXASetTimeoutResponse message, RemotingBuffer out) throws Exception
   {                 
      int bodyLength = 1;
      
      out.putInt(bodyLength);
      
      out.putBoolean(message.isOK());
   }

   @Override
   protected SessionXASetTimeoutResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      boolean ok = in.getBoolean();
      
      return new SessionXASetTimeoutResponse(ok);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


