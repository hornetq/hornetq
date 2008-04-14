/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEPRODUCER_RESP;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateProducerResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long producerTargetID;
   
   private final int windowSize;
   
   private final int maxRate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerResponseMessage(final long producerTargetID, final int windowSize, final int maxRate)
   {
      super(SESS_CREATEPRODUCER_RESP);

      this.producerTargetID = producerTargetID;
      
      this.windowSize = windowSize;
      
      this.maxRate = maxRate;
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
