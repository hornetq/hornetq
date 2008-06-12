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
public class SessionXAForgetMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private Xid xid;
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAForgetMessage(final Xid xid)
   {
      super(SESS_XA_FORGET);
      
      this.xid = xid;
   }
   
   public SessionXAForgetMessage()
   {
      super(SESS_XA_FORGET);
   }

   // Public --------------------------------------------------------
   
   public Xid getXid()
   {
      return xid;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionXAForgetMessage == false)
      {
         return false;
      }
            
      SessionXAForgetMessage r = (SessionXAForgetMessage)other;
      
      return this.xid.equals(r.xid);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


