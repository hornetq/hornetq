/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERY;

import org.jboss.jms.delegate.Cancel;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class CancelDeliveryMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Cancel cancel;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CancelDeliveryMessage(Cancel cancel)
   {
      super(MSG_CANCELDELIVERY);

      assert cancel != null;

      this.cancel = cancel;
   }

   // Public --------------------------------------------------------

   public Cancel getCancel()
   {
      return cancel;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", cancel=" + cancel + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
