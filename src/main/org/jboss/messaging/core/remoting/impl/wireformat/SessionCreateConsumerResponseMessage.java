/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATECONSUMER_RESP;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateConsumerResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long consumerTargetID;
   
   private final int windowSize;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerResponseMessage(final long consumerID, final int windowSize)
   {
      super(SESS_CREATECONSUMER_RESP);

      this.consumerTargetID = consumerID;
      
      this.windowSize = windowSize;
   }

   // Public --------------------------------------------------------

   public long getConsumerTargetID()
   {
      return consumerTargetID;
   }
   
   public int getWindowSize()
   {
   	return windowSize;
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", consumerTargetID=" + consumerTargetID);
      buf.append(", windowSize=" + windowSize);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
