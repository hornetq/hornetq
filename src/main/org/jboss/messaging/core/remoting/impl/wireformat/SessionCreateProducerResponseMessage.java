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
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateProducerResponseMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long producerTargetID;
   
   private int windowSize;
   
   private int maxRate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerResponseMessage(final long producerTargetID, final int windowSize, final int maxRate)
   {
      super(SESS_CREATEPRODUCER_RESP);

      this.producerTargetID = producerTargetID;
      
      this.windowSize = windowSize;
      
      this.maxRate = maxRate;
   }
   
   public SessionCreateProducerResponseMessage()
   {
      super(SESS_CREATEPRODUCER_RESP);
   }

   // Public --------------------------------------------------------

   public long getProducerTargetID()
   {
      return producerTargetID;
   }
   
   public int getWindowSize()
   {
   	return windowSize;
   }
   
   public int getMaxRate()
   {
   	return maxRate;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(producerTargetID);
      buffer.putInt(windowSize);
      buffer.putInt(maxRate);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      producerTargetID = buffer.getLong();      
      windowSize = buffer.getInt();
      maxRate = buffer.getInt();
   }
   

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", producerTargetID=" + producerTargetID);
      buf.append(", windowSize=" + windowSize);
      buf.append(", maxRate=" + maxRate);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
