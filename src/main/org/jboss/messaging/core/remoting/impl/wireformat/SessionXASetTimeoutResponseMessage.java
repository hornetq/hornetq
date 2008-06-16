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
public class SessionXASetTimeoutResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private boolean ok;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXASetTimeoutResponseMessage(final boolean ok)
   {
      super(SESS_XA_SET_TIMEOUT_RESP);
      
      this.ok = ok;
   }
   
   public SessionXASetTimeoutResponseMessage()
   {
      super(SESS_XA_SET_TIMEOUT_RESP);
   }
   
   // Public --------------------------------------------------------
   
   public boolean isOK()
   {
      return ok;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putBoolean(ok);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      ok = buffer.getBoolean();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
