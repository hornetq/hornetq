package org.jboss.messaging.util;

import org.jboss.messaging.core.remoting.TransportType;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface MessagingBufferFactory
{
   MessagingBuffer createMessagingBuffer(TransportType transportType, int len);
}
