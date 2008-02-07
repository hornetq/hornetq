/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATECONSUMER_RESP;

import org.jboss.messaging.core.remoting.Assert;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateConsumerResponseMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String consumerID;
   private final int bufferSize;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerResponseMessage(String consumerID, int bufferSize)
   {
      super(SESS_CREATECONSUMER_RESP);

      Assert.assertValidID(consumerID);

      this.consumerID = consumerID;
      this.bufferSize = bufferSize;
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

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", consumerID=" + consumerID);
      buf.append(", bufferSize=" + bufferSize);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
