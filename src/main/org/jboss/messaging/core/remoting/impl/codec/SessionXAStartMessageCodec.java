/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * 
 * A SessionXAStartMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAStartMessageCodec extends AbstractPacketCodec<SessionXAStartMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAStartMessageCodec()
   {
      super(PacketType.SESS_XA_START);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------


   @Override
   protected void encodeBody(final SessionXAStartMessage message, final MessagingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();      
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAStartMessage decodeBody(final MessagingBuffer in)
         throws Exception
   { 
      Xid xid = decodeXid(in);
      
      return new SessionXAStartMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

