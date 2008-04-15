/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;

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
      super(PacketType.SESS_XA_END);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   protected int getBodyLength(final SessionXAEndMessage packet) throws Exception
   {   	
   	int bodyLength = BOOLEAN_LENGTH + getXidLength(packet.getXid());
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionXAEndMessage message, final RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();      
      
      out.putBoolean(message.isFailed());
      
      encodeXid(xid, out);            
   }

   @Override
   protected SessionXAEndMessage decodeBody(final RemotingBuffer in) throws Exception
   {
      boolean failed = in.getBoolean();
                  
      Xid xid = decodeXid(in);
            
      return new SessionXAEndMessage(xid, failed);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

