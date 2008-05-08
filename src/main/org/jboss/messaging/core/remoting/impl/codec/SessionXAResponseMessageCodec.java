/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.util.MessagingBuffer;


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
   protected void encodeBody(final SessionXAResponseMessage message, final MessagingBuffer out) throws Exception
   {      
      out.putBoolean(message.isError());
      
      out.putInt(message.getResponseCode());
      
      out.putNullableString(message.getMessage());
   }

   @Override
   protected SessionXAResponseMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
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


