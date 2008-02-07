/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResponseMessage;


/**
 * 
 * A SessionXAResponseMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAResponseMessageCodec extends AbstractPacketCodec<SessionXAResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAResponseMessageCodec()
   {
      super(PacketType.SESS_XA_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXAResponseMessage message, RemotingBuffer out) throws Exception
   {      
      int bodyLength = 1 + INT_LENGTH + sizeof(message.getMessage());
      
      out.putInt(bodyLength);
      
      out.putBoolean(message.isError());
      
      out.putInt(message.getResponseCode());
      
      out.putNullableString(message.getMessage());
   }

   @Override
   protected SessionXAResponseMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      boolean isError = in.getBoolean();
      
      int responseCode = in.getInt();
      
      String message = in.getNullableString();
      
      return new SessionXAResponseMessage(isError, responseCode, message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


