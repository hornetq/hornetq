/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * 
 * A SessionXACommitMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAJoinMessageCodec extends AbstractPacketCodec<SessionXAJoinMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAJoinMessageCodec()
   {
      super(PacketType.SESS_XA_JOIN);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final SessionXAJoinMessage message, final MessagingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAJoinMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      Xid xid = decodeXid(in);
      
      return new SessionXAJoinMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

