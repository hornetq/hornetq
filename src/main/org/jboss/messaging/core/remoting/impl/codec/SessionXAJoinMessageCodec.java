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

   protected int getBodyLength(final SessionXAJoinMessage packet) throws Exception
   {   	
   	Xid xid = packet.getXid();
      
      int bodyLength = getXidLength(xid);
      
      return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionXAJoinMessage message, final RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAJoinMessage decodeBody(final RemotingBuffer in)
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

