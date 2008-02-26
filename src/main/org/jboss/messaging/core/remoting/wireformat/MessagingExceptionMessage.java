/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.EXCEPTION;

import org.jboss.messaging.core.MessagingException;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessagingExceptionMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final MessagingException exception;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MessagingExceptionMessage(final MessagingException exception)
   {
      super(EXCEPTION);

      assert exception != null;

      this.exception = exception;
   }

   // Public --------------------------------------------------------

   public MessagingException getException()
   {
      return exception;
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
