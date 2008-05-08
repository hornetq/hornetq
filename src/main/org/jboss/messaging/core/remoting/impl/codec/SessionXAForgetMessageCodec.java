/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * 
 * A SessionXAEndMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAForgetMessageCodec extends AbstractPacketCodec<SessionXAForgetMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAForgetMessageCodec()
   {
      super(PacketType.SESS_XA_FORGET);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------
 
   @Override
   protected void encodeBody(final SessionXAForgetMessage message, final MessagingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAForgetMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      Xid xid = decodeXid(in);
      
      return new SessionXAForgetMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

