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
public class SessionXAResumeMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final Xid xid;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAResumeMessage(final Xid xid)
   {
      super(PacketType.SESS_XA_RESUME);
      
      this.xid = xid;
   }

   // Public --------------------------------------------------------
   
   public Xid getXid()
   {
      return xid;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

