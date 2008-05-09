/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.remoting;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * 
 * A Packet
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Packet
{
   // Public --------------------------------------------------------

   public static final long NO_ID_SET = -1L;

   void setResponseTargetID(long responseTargetID);

   long getResponseTargetID();

   byte getType();

   long getTargetID();

   void setTargetID(long targetID);

   long getExecutorID();

   void setExecutorID(long executorID);
   
   void normalize(Packet other);

   void encode(MessagingBuffer buffer) throws Exception;
      
   void decode(MessagingBuffer buffer) throws Exception;
}
