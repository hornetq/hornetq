/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;

/**
 * 
 * A SessionXACommitMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXACommitMessageCodec extends AbstractPacketCodec<SessionXACommitMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXACommitMessageCodec()
   {
      super(PacketType.SESS_XA_COMMIT);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   protected int getBodyLength(final SessionXACommitMessage packet) throws Exception
   {   	
   	int bodyLength = BOOLEAN_LENGTH + getXidLength(packet.getXid());
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionXACommitMessage message, final RemotingBuffer out) throws Exception
   {      
      out.putBoolean(message.isOnePhase());      
      encodeXid(message.getXid(), out);      
   }

   @Override
   protected SessionXACommitMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      boolean onePhase = in.getBoolean();
            
      Xid xid = decodeXid(in);
            
      return new SessionXACommitMessage(xid, onePhase);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

