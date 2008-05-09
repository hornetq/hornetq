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
public class SessionCreateConsumerMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long clientTargetID;
   
   private SimpleString queueName;
   
   private SimpleString filterString;
   
   private boolean noLocal;
   
   private boolean autoDeleteQueue;
   
   private int windowSize;
   
   private int maxRate;
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerMessage(final long clientTargetID, final SimpleString queueName, final SimpleString filterString,
   		                              final boolean noLocal, final boolean autoDeleteQueue,
   		                              final int windowSize, final int maxRate)
   {
      super(SESS_CREATECONSUMER);

      this.clientTargetID = clientTargetID;
      this.queueName = queueName;
      this.filterString = filterString;
      this.noLocal = noLocal;
      this.autoDeleteQueue = autoDeleteQueue;
      this.windowSize = windowSize;
      this.maxRate = maxRate;
   }
   
   public SessionCreateConsumerMessage()
   {
      super(SESS_CREATECONSUMER);   
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append(", noLocal=" + noLocal);
      buff.append(", autoDeleteQueue=" + autoDeleteQueue);
      buff.append(", windowSize=" + windowSize);
      buff.append(", maxRate=" + maxRate);
      buff.append("]");
      return buff.toString();
   }

   public long getClientTargetID()
   {
      return clientTargetID;
   }
   
   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public boolean isNoLocal()
   {
      return noLocal;
   }
   
   public boolean isAutoDeleteQueue()
   {
      return autoDeleteQueue;
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
      buffer.putSimpleString(queueName);
      buffer.putNullableSimpleString(filterString);
      buffer.putBoolean(noLocal);
      buffer.putBoolean(autoDeleteQueue);
      buffer.putInt(windowSize);
      buffer.putInt(maxRate);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      clientTargetID = buffer.getLong();
      queueName = buffer.getSimpleString();
      filterString = buffer.getNullableSimpleString();
      noLocal = buffer.getBoolean();
      autoDeleteQueue = buffer.getBoolean();
      windowSize = buffer.getInt();
      maxRate = buffer.getInt();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
