/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import javax.transaction.xa.Xid;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXAEndMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final Xid xid;
   
   private final boolean failed;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAEndMessage(final Xid xid, final boolean failed)
   {
      super(PacketType.SESS_XA_END);
      
      this.xid = xid;
      
      this.failed = failed;
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

