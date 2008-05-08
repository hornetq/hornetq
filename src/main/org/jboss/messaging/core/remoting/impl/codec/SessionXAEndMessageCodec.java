/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * 
 * A SessionXAEndMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAEndMessageCodec extends AbstractPacketCodec<SessionXAEndMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAEndMessageCodec()
   {
      super(PacketType.SESS_XA_END);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final SessionXAEndMessage message, final MessagingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();      
      
      encodeXid(xid, out);                  
      out.putBoolean(message.isFailed());
   }

   @Override
   protected SessionXAEndMessage decodeBody(final MessagingBuffer in) throws Exception
   {
      Xid xid = decodeXid(in);
      boolean failed = in.getBoolean();
                  
      return new SessionXAEndMessage(xid, failed);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

