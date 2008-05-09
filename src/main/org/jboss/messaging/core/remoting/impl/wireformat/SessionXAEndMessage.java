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
public class SessionXAEndMessage extends EmptyPacket
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
      buffer.putBoolean(failed);
      encodeXid(xid, buffer);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      failed = buffer.getBoolean();
      xid = decodeXid(buffer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

