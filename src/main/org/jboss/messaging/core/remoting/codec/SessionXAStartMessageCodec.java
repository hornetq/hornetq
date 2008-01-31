/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXAStartMessage;

/**
 * 
 * A SessionXAStartMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAStartMessageCodec extends AbstractPacketCodec<SessionXAStartMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAStartMessageCodec()
   {
      super(PacketType.MSG_XA_START);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXAStartMessage message, RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();      
      
      int bodyLength = getXidLength(xid);
      
      out.putInt(bodyLength);
      
      encodeXid(xid, out);
   }

   @Override
   protected SessionXAStartMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
            
      Xid xid = decodeXid(in);
      
      return new SessionXAStartMessage(xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

