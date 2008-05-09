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
public class SessionXAResponseMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private boolean error;
   
   private int responseCode;
   
   private String message;
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAResponseMessage(final boolean isError, final int responseCode, final String message)
   {
      super(SESS_XA_RESP);
      
      this.error = isError;
      
      this.responseCode = responseCode;
      
      this.message = message;
   }
   
   public SessionXAResponseMessage()
   {
      super(SESS_XA_RESP);
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putBoolean(error);      
      buffer.putInt(responseCode);      
      buffer.putNullableString(message);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      error = buffer.getBoolean();      
      responseCode = buffer.getInt();      
      message = buffer.getNullableString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

