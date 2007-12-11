/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATECONSUMER;

import org.jboss.messaging.core.remoting.Assert;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConsumerResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String consumerID;
   private final int bufferSize;
   private final int maxDeliveries;
   private final long redeliveryDelay;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConsumerResponse(String consumerID, int bufferSize,
         int maxDeliveries, long redeliveryDelay)
   {
      super(RESP_CREATECONSUMER);

      Assert.assertValidID(consumerID);

      this.consumerID = consumerID;
      this.bufferSize = bufferSize;
      this.maxDeliveries = maxDeliveries;
      this.redeliveryDelay = redeliveryDelay;
   }

   // Public --------------------------------------------------------

   public String getConsumerID()
   {
      return consumerID;
   }

   public int getBufferSize()
   {
      return bufferSize;
   }

   public int getMaxDeliveries()
   {
      return maxDeliveries;
   }

   public long getRedeliveryDelay()
   {
      return redeliveryDelay;
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", consumerID=" + consumerID);
      buf.append(", bufferSize=" + bufferSize);
      buf.append(", maxDeliveries=" + maxDeliveries);
      buf.append(", redeliveryDelay=" + redeliveryDelay);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
