/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CHANGERATE;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ConsumerChangeRateMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final float rate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerChangeRateMessage(float rate)
   {
      super(MSG_CHANGERATE);

      this.rate = rate;
   }

   // Public --------------------------------------------------------

   public float getRate()
   {
      return rate;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", rate=" + rate + "]";
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
