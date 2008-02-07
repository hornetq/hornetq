/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResumeMessage;

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

   @Override
   protected void encodeBody(SessionXAResumeMessage message, RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      int bodyLength = getXidLength(xid);
      
      out.putInt(bodyLength);
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAResumeMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      Xid xid = decodeXid(in);
      
      return new SessionXAResumeMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

