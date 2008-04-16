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

   public int getBodyLength(final SessionXAForgetMessage packet) throws Exception
   {   	
   	int bodyLength = getXidLength(packet.getXid());
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionXAForgetMessage message, final RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAForgetMessage decodeBody(final RemotingBuffer in)
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

