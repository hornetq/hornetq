/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.ClientManager;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.client.container.InvokerInterceptor;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.lang.reflect.Proxy;

/**
 * Creates ConnectionFactoryDelegate instances. Instances of this class are constructed only on the
 * server.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerConnectionFactoryDelegate implements ConnectionFactoryDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConnectionFactoryDelegate.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;

   // Constructors --------------------------------------------------

   public ServerConnectionFactoryDelegate(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
   }

   // ConnectionFactoryDelegate implementation ----------------------

   public ConnectionDelegate createConnectionDelegate()
   {
      return createConnectionDelegate(null, null);
   }

   public ConnectionDelegate createConnectionDelegate(String username, String password)
   {

      // create the ConnectionDelegate dynamic proxy
      ConnectionDelegate cd = null;
      Serializable oid = serverPeer.getConnectionAdvisor().getName();
      String stackName = "ConnectionStack";
      AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);

      // TODO why do I need to the advisor to create the interceptor stack?
      Interceptor[] interceptors = stack.createInterceptors(serverPeer.getConnectionAdvisor(), null);

      JMSInvocationHandler h = new JMSInvocationHandler(interceptors);

      ClientManager clientManager = serverPeer.getClientManager();
      String clientID = clientManager.generateClientID();

      SimpleMetaData metadata = new SimpleMetaData();
      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, oid, PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
                           InvokerInterceptor.INVOKER_LOCATOR,
                           serverPeer.getLocator(),
                           PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
                           InvokerInterceptor.SUBSYSTEM,
                           "JMS",
                           PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID, clientID, PayloadKey.AS_IS);

      h.getMetaData().mergeIn(metadata);

      // TODO
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ConnectionDelegate.class };
      cd = (ConnectionDelegate)Proxy.newProxyInstance(loader, interfaces, h);

      // create the corresponding "server-side" ConnectionDelegate and register it with the
      // server peer's ClientManager
      ServerConnectionDelegate scd = new ServerConnectionDelegate(clientID, serverPeer);
      clientManager.putConnectionDelegate(clientID, scd);

      log.debug("created connection delegate (clientID=" + clientID + ")");

      return cd;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
