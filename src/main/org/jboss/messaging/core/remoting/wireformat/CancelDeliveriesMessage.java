/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERIES;

import java.util.List;

import org.jboss.jms.client.impl.Cancel;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class CancelDeliveriesMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final List<Cancel> cancels;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CancelDeliveriesMessage(List<Cancel> cancels)
   {
      super(MSG_CANCELDELIVERIES);

      assert cancels != null;
      assert cancels.size() > 0;

      this.cancels = cancels;
   }

   // Public --------------------------------------------------------

   public List<Cancel> getCancels()
   {
      return cancels;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", cancels=" + cancels + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
