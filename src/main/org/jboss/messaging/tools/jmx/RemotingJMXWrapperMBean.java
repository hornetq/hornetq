/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.tools.jmx;

import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.transport.Connector;

public interface RemotingJMXWrapperMBean
{

   public void start() throws Exception;
   public void stop() throws Exception;

   public RemotingJMXWrapper getInstance();

   public Connector getConnector() throws Exception;

   public String getInvokerLocator() throws Exception;

   public ServerInvocationHandler addInvocationHandler(String s, ServerInvocationHandler h)
         throws Exception;
}
