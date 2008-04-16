/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;


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

   public int getBodyLength(final SessionXAResponseMessage packet) throws Exception
   {   	
   	int bodyLength = BOOLEAN_LENGTH + INT_LENGTH + sizeof(packet.getMessage());
   	
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionXAResponseMessage message, final RemotingBuffer out) throws Exception
   {      
      out.putBoolean(message.isError());
      
      out.putInt(message.getResponseCode());
      
      out.putNullableString(message.getMessage());
   }

   @Override
   protected SessionXAResponseMessage decodeBody(final RemotingBuffer in)
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


