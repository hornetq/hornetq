/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.impl.mina.ServerKeepAliveFactory;
import org.jboss.messaging.core.server.MessagingComponent;

import java.util.List;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision$</tt>
 */
public interface RemotingService extends MessagingComponent
{
   PacketDispatcher getDispatcher();

   Configuration getConfiguration();

   ServerKeepAliveFactory getKeepAliveFactory();

   List<Acceptor> getAcceptors();

   void setAcceptorFactory(AcceptorFactory acceptorFactory);

   void addInterceptor(Interceptor interceptor);

   void removeInterceptor(Interceptor interceptor);

   void addRemotingSessionListener(RemotingSessionListener listener);

   void removeRemotingSessionListener(RemotingSessionListener listener);

   void registerPinger(NIOSession session);

   void unregisterPinger(Long id);
}
