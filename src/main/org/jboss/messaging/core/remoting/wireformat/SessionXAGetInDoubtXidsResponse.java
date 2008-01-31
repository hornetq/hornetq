/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import java.util.List;

import javax.transaction.xa.Xid;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXAGetInDoubtXidsResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private List<Xid> xids;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetInDoubtXidsResponse(List<Xid> xids)
   {
      super(PacketType.RESP_XA_INDOUBT_XIDS);
      
      this.xids = xids;
   }

   // Public --------------------------------------------------------
   
   public List<Xid> getXids()
   {
      return xids;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


