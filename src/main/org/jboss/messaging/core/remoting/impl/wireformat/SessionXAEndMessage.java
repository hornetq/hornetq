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
public class SessionXAEndMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private Xid xid;
   
   private boolean failed;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAEndMessage(final Xid xid, final boolean failed)
   {
      super(SESS_XA_END);
      
      this.xid = xid;
      
      this.failed = failed;
   }
   
   public SessionXAEndMessage()
   {
      super(SESS_XA_END);
   }

   // Public --------------------------------------------------------
   
   public boolean isFailed()
   {
      return failed;
   }
   
   public Xid getXid()
   {
      return xid;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
      buffer.putBoolean(failed);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
      failed = buffer.getBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", xid=" + xid + ", failed=" + failed + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionXAEndMessage == false)
      {
         return false;
      }
            
      SessionXAEndMessage r = (SessionXAEndMessage)other;
      
      return this.xid.equals(r.xid) &&
             this.failed == r.failed;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

