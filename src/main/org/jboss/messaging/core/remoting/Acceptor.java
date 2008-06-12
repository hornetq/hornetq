package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.remoting.impl.mina.CleanUpNotifier;

/**
 * An Acceptor is used tby the Remoting Service to allow clients to connect. It should take care of dispatchin client requests
 * to the Remoting Service's Dispatcher.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface Acceptor
{
   void startAccepting(RemotingService remotingService, CleanUpNotifier cleanupNotifier) throws Exception;

   void stopAccepting();
}
