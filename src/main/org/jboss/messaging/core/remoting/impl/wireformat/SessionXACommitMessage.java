/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import javax.transaction.xa.Xid;

import org.jboss.messaging.util.MessagingBuffer;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXACommitMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private boolean onePhase;
   
   private Xid xid;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXACommitMessage(final Xid xid, final boolean onePhase)
   {
      super(SESS_XA_COMMIT);
      
      this.xid = xid;
      this.onePhase = onePhase;
   }
   
   public SessionXACommitMessage()
   {
      super(SESS_XA_COMMIT);
   }

   // Public --------------------------------------------------------
   
   public Xid getXid()
   {
      return xid;
   }
   
   public boolean isOnePhase()
   {
      return onePhase;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
      buffer.putBoolean(onePhase);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
      onePhase = buffer.getBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", xid=" + xid + ", onePhase=" + onePhase + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionXACommitMessage == false)
      {
         return false;
      }
            
      SessionXACommitMessage r = (SessionXACommitMessage)other;
      
      return this.xid.equals(r.xid) &&
             this.onePhase == r.onePhase;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

