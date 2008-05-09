/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessagingExceptionMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingException exception;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MessagingExceptionMessage(final MessagingException exception)
   {
      super(EXCEPTION);

      this.exception = exception;
   }
   
   public MessagingExceptionMessage()
   {
      super(EXCEPTION);
   }

   // Public --------------------------------------------------------

   public MessagingException getException()
   {
      return exception;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(exception.getCode());
      buffer.putNullableString(exception.getMessage());
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      int code = buffer.getInt();
      String msg = buffer.getNullableString();
      exception = new MessagingException(code, msg);
   }
   
   @Override
   public String toString()
   {
      return getParentString() + ", exception= " + exception + "]";
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
