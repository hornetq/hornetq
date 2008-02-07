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
public class SessionXAResponseMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private boolean error;
   
   private int responseCode;
   
   private String message;
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAResponseMessage(boolean isError, int responseCode, String message)
   {
      super(PacketType.SESS_XA_RESP);
      
      this.error = isError;
      
      this.responseCode = responseCode;
      
      this.message = message;
   }

   // Public --------------------------------------------------------
   
   public boolean isError()
   {
      return error;
   }
   
   public int getResponseCode()
   {
      return this.responseCode;
   }
   
   public String getMessage()
   {
      return message;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

