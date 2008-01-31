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
public class SessionXASetTimeoutMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private int timeoutSeconds;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXASetTimeoutMessage(int timeoutSeconds)
   {
      super(PacketType.MSG_XA_SET_TIMEOUT);
      
      this.timeoutSeconds = timeoutSeconds;
   }
   

   // Public --------------------------------------------------------
   
   public int getTimeoutSeconds()
   {
      return this.timeoutSeconds;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

