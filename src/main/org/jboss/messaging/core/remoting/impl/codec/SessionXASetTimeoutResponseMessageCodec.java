/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.util.MessagingBuffer;

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
   protected void encodeBody(final SessionXASetTimeoutResponseMessage message, final MessagingBuffer out) throws Exception
   {                 
      out.putBoolean(message.isOK());
   }

   @Override
   protected SessionXASetTimeoutResponseMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      boolean ok = in.getBoolean();
      
      return new SessionXASetTimeoutResponseMessage(ok);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


