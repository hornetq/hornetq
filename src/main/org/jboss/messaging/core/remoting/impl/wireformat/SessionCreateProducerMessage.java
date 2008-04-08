/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateProducerMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String address;
   
   private final int windowSize;
   
   private final int maxRate;
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerMessage(final String address, final int windowSize, final int maxRate)
   {
      super(PacketType.SESS_CREATEPRODUCER);

      this.address = address;
      
      this.windowSize = windowSize;
      
      this.maxRate = maxRate;
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", address=" + address);
      buff.append(", windowSize=" + windowSize);
      buff.append(", maxrate=" + maxRate);
      buff.append("]");
      return buff.toString();
   }

   public String getAddress()
   {
      return address;
   }
   
   public int getWindowSize()
   {
   	return windowSize;
   }
   
   public int getMaxRate()
   {
   	return maxRate;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

