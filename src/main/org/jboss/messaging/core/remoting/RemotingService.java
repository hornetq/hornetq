/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.server.MessagingComponent;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public interface RemotingService extends MessagingComponent
{
   PacketDispatcher getDispatcher();

   Configuration getConfiguration();
   
   void addInterceptor(Interceptor interceptor);

   void removeInterceptor(Interceptor interceptor);

   void addRemotingSessionListener(RemotingSessionListener listener);

   void removeRemotingSessionListener(RemotingSessionListener listener);
}
