/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutMessage;

/**
 * 
 * A SessionXASetTimeoutMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXASetTimeoutMessageCodec extends AbstractPacketCodec<SessionXASetTimeoutMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXASetTimeoutMessageCodec()
   {
      super(PacketType.SESS_XA_SET_TIMEOUT);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXASetTimeoutMessage message, RemotingBuffer out) throws Exception
   {                 
      int bodyLength = INT_LENGTH;
      
      out.putInt(bodyLength);
      
      out.putInt(message.getTimeoutSeconds());
   }

   @Override
   protected SessionXASetTimeoutMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      int timeout = in.getInt();
      
      return new SessionXASetTimeoutMessage(timeout);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


