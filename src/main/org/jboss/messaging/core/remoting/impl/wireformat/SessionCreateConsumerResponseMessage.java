/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;

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

   private long consumerTargetID;
   
   private int windowSize;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerResponseMessage(final long consumerID, final int windowSize)
   {
      super(SESS_CREATECONSUMER_RESP);

      this.consumerTargetID = consumerID;
      
      this.windowSize = windowSize;
   }
   
   public SessionCreateConsumerResponseMessage()
   {
      super(SESS_CREATECONSUMER_RESP);
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(consumerTargetID);
      buffer.putInt(windowSize);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      consumerTargetID = buffer.getLong();
      windowSize = buffer.getInt();
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
