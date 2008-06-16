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
public class SessionXAGetTimeoutResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private int timeoutSeconds;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetTimeoutResponseMessage(final int timeoutSeconds)
   {
      super(SESS_XA_GET_TIMEOUT_RESP);
      
      this.timeoutSeconds = timeoutSeconds;
   }
   
   public SessionXAGetTimeoutResponseMessage()
   {
      super(SESS_XA_GET_TIMEOUT_RESP);
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

