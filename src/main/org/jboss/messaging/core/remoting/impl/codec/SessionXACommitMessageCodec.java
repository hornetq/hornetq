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
import org.jboss.messaging.util.MessagingBuffer;

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
   protected void encodeBody(final SessionXACommitMessage message, final MessagingBuffer out) throws Exception
   {      
      encodeXid(message.getXid(), out);      
      out.putBoolean(message.isOnePhase());      
   }

   @Override
   protected SessionXACommitMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      Xid xid = decodeXid(in);
      boolean onePhase = in.getBoolean();
                  
      return new SessionXACommitMessage(xid, onePhase);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

