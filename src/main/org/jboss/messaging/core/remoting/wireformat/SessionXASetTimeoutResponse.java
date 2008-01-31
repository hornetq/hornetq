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
public class SessionXASetTimeoutResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private boolean ok;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXASetTimeoutResponse(boolean ok)
   {
      super(PacketType.MSG_XA_SET_TIMEOUT_RESPONSE);
      
      this.ok = ok;
   }
   
   // Public --------------------------------------------------------
   
   public boolean isOK()
   {
      return ok;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
