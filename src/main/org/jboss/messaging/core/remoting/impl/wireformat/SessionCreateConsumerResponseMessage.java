/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATECONSUMER_RESP;

import org.jboss.messaging.core.remoting.impl.Assert;

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
   
   private final int windowSize;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerResponseMessage(final String consumerID, final int windowSize)
   {
      super(SESS_CREATECONSUMER_RESP);

      Assert.assertValidID(consumerID);

      this.consumerID = consumerID;
      
      this.windowSize = windowSize;
   }

   // Public --------------------------------------------------------

   public String getConsumerID()
   {
      return consumerID;
   }
   
   public int getWindowSize()
   {
   	return windowSize;
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", consumerID=" + consumerID);
      buf.append(", windowSize=" + windowSize);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
