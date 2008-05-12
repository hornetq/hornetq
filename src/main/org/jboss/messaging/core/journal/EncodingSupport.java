package org.jboss.messaging.core.journal;

import org.jboss.messaging.util.MessagingBuffer;

/** 
 * This class provides encoding support for the Journal.
 * */
public interface EncodingSupport
{
   int encodeSize();
   void encode(MessagingBuffer buffer);
}
