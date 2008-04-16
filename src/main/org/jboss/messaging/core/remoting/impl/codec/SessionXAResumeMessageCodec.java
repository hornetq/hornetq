/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;

/**
 * 
 * A SessionXAResumeMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAResumeMessageCodec extends AbstractPacketCodec<SessionXAResumeMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAResumeMessageCodec()
   {
      super(PacketType.SESS_XA_RESUME);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionXAResumeMessage packet) throws Exception
   {   	
   	Xid xid = packet.getXid();
      
      int bodyLength = getXidLength(xid);
      
      return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionXAResumeMessage message, final RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAResumeMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {           
      Xid xid = decodeXid(in);
      
      return new SessionXAResumeMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

