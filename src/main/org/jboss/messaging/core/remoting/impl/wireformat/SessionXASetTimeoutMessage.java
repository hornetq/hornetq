/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;



/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXASetTimeoutMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private int timeoutSeconds;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXASetTimeoutMessage(final int timeoutSeconds)
   {
      super(SESS_XA_SET_TIMEOUT);
      
      this.timeoutSeconds = timeoutSeconds;
   }
   
   public SessionXASetTimeoutMessage()
   {
      super(SESS_XA_SET_TIMEOUT);
   }
   

   // Public --------------------------------------------------------
   
   public int getTimeoutSeconds()
   {
      return this.timeoutSeconds;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(timeoutSeconds);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      timeoutSeconds = buffer.getInt();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

