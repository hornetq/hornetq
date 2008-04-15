/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;


/**
 * 
 * A SessionXAGetTimeoutResponseMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAGetTimeoutResponseMessageCodec extends AbstractPacketCodec<SessionXAGetTimeoutResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetTimeoutResponseMessageCodec()
   {
      super(PacketType.SESS_XA_GET_TIMEOUT_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   protected int getBodyLength(final SessionXAGetTimeoutResponseMessage packet) throws Exception
   {   	
   	return INT_LENGTH;
   }
   
   @Override
   protected void encodeBody(final SessionXAGetTimeoutResponseMessage message, final RemotingBuffer out) throws Exception
   {      
      out.putInt(message.getTimeoutSeconds());
   }

   @Override
   protected SessionXAGetTimeoutResponseMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      int timeout = in.getInt();
      
      return new SessionXAGetTimeoutResponseMessage(timeout);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

