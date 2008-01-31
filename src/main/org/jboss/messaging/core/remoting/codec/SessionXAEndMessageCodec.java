/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXAEndMessage;

/**
 * 
 * A SessionXAEndMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAEndMessageCodec extends AbstractPacketCodec<SessionXAEndMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAEndMessageCodec()
   {
      super(PacketType.MSG_XA_END);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXAEndMessage message, RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();      
      
      int bodyLength = 1 + getXidLength(xid);
      
      out.putInt(bodyLength);
      
      out.putBoolean(message.isFailed());
      
      encodeXid(xid, out);            
   }

   @Override
   protected SessionXAEndMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      boolean failed = in.getBoolean();
                  
      Xid xid = decodeXid(in);
            
      return new SessionXAEndMessage(xid, failed);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

