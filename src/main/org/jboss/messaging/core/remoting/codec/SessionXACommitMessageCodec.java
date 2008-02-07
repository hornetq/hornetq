/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXACommitMessage;

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

   @Override
   protected void encodeBody(SessionXACommitMessage message, RemotingBuffer out) throws Exception
   {      
      Xid xid = message.getXid();
      
      int bodyLength = 1 + getXidLength(xid);
      
      out.putInt(bodyLength);
      
      out.putBoolean(message.isOnePhase());
      
      encodeXid(xid, out);      
   }

   @Override
   protected SessionXACommitMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      boolean onePhase = in.getBoolean();
            
      Xid xid = decodeXid(in);
            
      return new SessionXACommitMessage(xid, onePhase);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

