/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_JMSEXCEPTION;

import javax.jms.JMSException;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSExceptionMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JMSException exception;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSExceptionMessage(JMSException exception)
   {
      super(MSG_JMSEXCEPTION);

      assert exception != null;

      this.exception = exception;
   }

   // Public --------------------------------------------------------

   public JMSException getException()
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
