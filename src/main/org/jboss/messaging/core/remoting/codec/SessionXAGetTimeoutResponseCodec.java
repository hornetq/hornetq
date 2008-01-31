/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetTimeoutResponse;


/**
 * 
 * A SessionXAGetTimeoutResponseCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAGetTimeoutResponseCodec extends AbstractPacketCodec<SessionXAGetTimeoutResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetTimeoutResponseCodec()
   {
      super(PacketType.MSG_XA_GET_TIMEOUT_RESPONSE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXAGetTimeoutResponse message, RemotingBuffer out) throws Exception
   {      
      int bodyLength = INT_LENGTH;
      
      out.putInt(bodyLength);
      
      out.putInt(message.getTimeoutSeconds());
   }

   @Override
   protected SessionXAGetTimeoutResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      int timeout = in.getInt();
      
      return new SessionXAGetTimeoutResponse(timeout);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

