/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXAJoinMessage;

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
      super(PacketType.MSG_XA_JOIN);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXAJoinMessage message, RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      int bodyLength = getXidLength(xid);
      
      out.putInt(bodyLength);
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAJoinMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      Xid xid = decodeXid(in);
      
      return new SessionXAJoinMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

