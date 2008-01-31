/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXARollbackMessage;

public class SessionXARollbackMessageCodec extends AbstractPacketCodec<SessionXARollbackMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXARollbackMessageCodec()
   {
      super(PacketType.MSG_XA_ROLLBACK);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXARollbackMessage message, RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      int bodyLength = getXidLength(xid);
      
      out.putInt(bodyLength);
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXARollbackMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      Xid xid = decodeXid(in);
      
      return new SessionXARollbackMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


