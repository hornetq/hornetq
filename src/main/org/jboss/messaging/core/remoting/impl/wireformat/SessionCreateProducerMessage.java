/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateProducerMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long clientTargetID;
   
   private SimpleString address;
   
   private int windowSize;
   
   private int maxRate;
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerMessage(final long clientTargetID, final SimpleString address, final int windowSize, final int maxRate)
   {
      super(SESS_CREATEPRODUCER);

      this.clientTargetID = clientTargetID;
      
      this.address = address;
      
      this.windowSize = windowSize;
      
      this.maxRate = maxRate;
   }
   
   public SessionCreateProducerMessage()
   {
      super(SESS_CREATEPRODUCER);
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
   
   public long getClientTargetID()
   {
      return clientTargetID;
   }

   public SimpleString getAddress()
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(clientTargetID);
      buffer.putNullableSimpleString(address);
      buffer.putInt(windowSize);
      buffer.putInt(maxRate);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      clientTargetID = buffer.getLong();      
      address = buffer.getNullableSimpleString();      
      windowSize = buffer.getInt();      
      maxRate = buffer.getInt();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionCreateProducerMessage == false)
      {
         return false;
      }
            
      SessionCreateProducerMessage r = (SessionCreateProducerMessage)other;
      
      return this.clientTargetID == r.clientTargetID &&
             this.address == null ? r.address == null : this.address.equals(r.address) &&
             this.windowSize == r.windowSize &&
             this.maxRate == r.maxRate;                  
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

