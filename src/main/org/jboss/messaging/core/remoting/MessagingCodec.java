package org.jboss.messaging.core.remoting;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * Used to encode/decode messages.
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface MessagingCodec
{
   void encode(MessagingBuffer buffer, Object message) throws Exception;

   Packet decode(MessagingBuffer in) throws Exception;
}
