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
public class SessionXARollbackMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private Xid xid;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXARollbackMessage(final Xid xid)
   {
      super(SESS_XA_ROLLBACK);
      
      this.xid = xid;
   }
   
   public SessionXARollbackMessage()
   {
      super(SESS_XA_ROLLBACK);
   }

   // Public --------------------------------------------------------
   
   public Xid getXid()
   {
      return xid;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      encodeXid(xid, buffer);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      xid = decodeXid(buffer);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

