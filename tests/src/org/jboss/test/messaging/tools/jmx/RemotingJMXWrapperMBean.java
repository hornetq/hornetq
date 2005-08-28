/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.tools.jmx;

import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.transport.Connector;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface RemotingJMXWrapperMBean
{
   void start() throws Exception;
   void stop() throws Exception;

   RemotingJMXWrapper getInstance();

   Connector getConnector() throws Exception;

   String getInvokerLocator() throws Exception;

   ServerInvocationHandler addInvocationHandler(String s, ServerInvocationHandler h)
         throws Exception;
}
