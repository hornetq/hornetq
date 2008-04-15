/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;

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

   protected int getBodyLength(final SessionXASetTimeoutMessage packet) throws Exception
   {   	
      return INT_LENGTH;
   }
   
   @Override
   protected void encodeBody(final SessionXASetTimeoutMessage message, final RemotingBuffer out) throws Exception
   {                 
      out.putInt(message.getTimeoutSeconds());
   }

   @Override
   protected SessionXASetTimeoutMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      int timeout = in.getInt();
      
      return new SessionXASetTimeoutMessage(timeout);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


