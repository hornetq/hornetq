/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.util.MessagingBuffer;

public class SessionXARollbackMessageCodec extends AbstractPacketCodec<SessionXARollbackMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXARollbackMessageCodec()
   {
      super(PacketType.SESS_XA_ROLLBACK);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final SessionXARollbackMessage message, final MessagingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXARollbackMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      Xid xid = decodeXid(in);
      
      return new SessionXARollbackMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


