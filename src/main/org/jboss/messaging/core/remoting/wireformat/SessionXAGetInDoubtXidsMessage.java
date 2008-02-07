/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXAGetInDoubtXidsMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetInDoubtXidsMessage()
   {
      super(PacketType.SESS_XA_INDOUBT_XIDS);
   }

   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


