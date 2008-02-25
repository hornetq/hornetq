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
public class SessionXACommitMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final boolean onePhase;
   
   private final Xid xid;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXACommitMessage(final Xid xid, final boolean onePhase)
   {
      super(PacketType.SESS_XA_COMMIT);
      
      this.xid = xid;
      
      this.onePhase = onePhase;
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
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

