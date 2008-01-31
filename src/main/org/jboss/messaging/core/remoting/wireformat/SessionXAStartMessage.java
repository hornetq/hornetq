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
public class SessionXAStartMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private Xid xid;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAStartMessage(Xid xid)
   {
      super(PacketType.MSG_XA_START);
      
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

