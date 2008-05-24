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
   
   private int initialCredits;
   
   private int maxRate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerResponseMessage(final long producerTargetID, final int initialCredits, final int maxRate)
   {
      super(SESS_CREATEPRODUCER_RESP);

      this.producerTargetID = producerTargetID;
      
      this.initialCredits = initialCredits;
      
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
   
   public int getInitialCredits()
   {
   	return initialCredits;
   }
   
   public int getMaxRate()
   {
   	return maxRate;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(producerTargetID);
      buffer.putInt(initialCredits);
      buffer.putInt(maxRate);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      producerTargetID = buffer.getLong();      
      initialCredits = buffer.getInt();
      maxRate = buffer.getInt();
   }
   

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", producerTargetID=" + producerTargetID);
      buf.append(", initialCredits=" + initialCredits);
      buf.append(", maxRate=" + maxRate);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
