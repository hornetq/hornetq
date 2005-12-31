/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.jboss.test.messaging.tools.jmx.rmi;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.tools.jmx.MockJBossSecurityManager;
import org.jboss.test.messaging.tools.jmx.RemotingJMXWrapper;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.StateManager;
import org.jboss.messaging.core.MessageStore;
import org.jboss.remoting.transport.Connector;
import org.w3c.dom.Element;

import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

/**
 * An RMI wrapper to access the ServiceContainer from a different address space. The same RMI
 * container can be used to access the ServiceContainer in-VM, without activation of the RMI
 * features.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RMIServer extends UnicastRemoteObject implements Server
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RMIServer.class);

   public static final int RMI_REGISTRY_PORT = 25989;
   public static final String RMI_SERVER_NAME = "messaging-rmi-server";
   public static final String NAMING_SERVER_NAME = "naming-rmi-server";

   private static Registry registry;

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      log.debug("initializing RMI runtime");

      String host = System.getProperty("test.bind.address");
      if (host == null)
      {
         host = "localhost";
      }

      log.info("bind address: " + host);

      // let RMI know the bind address
      System.setProperty("java.rmi.server.hostname", host);

      registry = LocateRegistry.createRegistry(RMI_REGISTRY_PORT);
      log.debug("registry created");

      RMIServer rmiServer = new RMIServer(true);
      log.debug("RMI server created");

      registry.bind(RMI_SERVER_NAME, rmiServer);
      registry.bind(NAMING_SERVER_NAME, rmiServer.getNamingDelegate());

      log.info("RMI server bound");
   }

   // Attributes ----------------------------------------------------

   private ServiceContainer sc;
   private ServerPeer serverPeer;
   private boolean remote;
   private RMINamingDelegate namingDelegate;

   // Constructors --------------------------------------------------

   public RMIServer(boolean remote) throws Exception
   {
      super();
      this.remote = remote;

      if (remote)
      {
         namingDelegate = new RMINamingDelegate();
      }
   }

   // Server implementation -----------------------------------------

   public synchronized void start(String containerConfig) throws Exception
   {
      if (isStarted())
      {
         return;
      }

      log.debug("starting service container");

      sc = new ServiceContainer(containerConfig, null);
      sc.start();

      log.debug("starting JMS server");

      serverPeer = new ServerPeer("ServerPeer0");
      serverPeer.setSecurityDomain(MockJBossSecurityManager.TEST_SECURITY_DOMAIN);
      final String defaultSecurityConfig =
         "<security><role name=\"guest\" read=\"true\" write=\"true\" create=\"true\"/></security>";
      serverPeer.setDefaultSecurityConfig(ServerManagement.toElement(defaultSecurityConfig));
      serverPeer.start();

      log.info("server started");
   }

   public synchronized void stop() throws Exception
   {
      if (!isStarted())
      {
         return;
      }

      log.debug("stopping service container");

      if (serverPeer != null)
      {
         serverPeer.stop();
         serverPeer = null;
      }

      sc.stop();
      sc = null;

      if (remote)
      {
         namingDelegate.reset();
      }

      log.info("server stopped");
   }

   public synchronized void destroy() throws Exception
   {
      stop();

      if (remote)
      {
         registry.unbind(RMI_SERVER_NAME);
         registry.unbind(NAMING_SERVER_NAME);
      }
   }

   public void exit() throws Exception
   {
      destroy();
      new Thread(new VMKiller(), "VM Killer").start();
   }

   public synchronized boolean isStarted() throws Exception
   {
      return sc != null;
   }

   public void startServerPeer() throws Exception
   {
      serverPeer.start();
   }

   public void stopServerPeer() throws Exception
   {
      serverPeer.stop();
   }

   /**
    * Only for in-VM use!
    */
   public ServerPeer getServerPeer() throws Exception
   {
      if (isRemote())
      {
         throw new IllegalStateException("This method shouldn't be invoked on a remote server");
      }
      return serverPeer;
   }

   /**
    * Only for in-VM use!
    */
   public Connector getConnector() throws Exception
   {
      if (isRemote())
      {
         throw new IllegalStateException("This method shouldn't be invoked on a remote server");
      }

      RemotingJMXWrapper remoting =
         (RemotingJMXWrapper)sc.getService(ServiceContainer.REMOTING_OBJECT_NAME);
      return remoting.getConnector();
   }

   public MessageStore getMessageStore() throws Exception
   {
      //TODO
      return serverPeer.getMessageStore();
   }

   public StateManager getStateManager() throws Exception
   {
      // TODO
      return serverPeer.getStateManager();
   }

   public void deployTopic(String name, String jndiName) throws Exception
   {
      serverPeer.getDestinationManager().createTopic(name, jndiName);
   }

   public void undeployTopic(String name) throws Exception
   {
      serverPeer.getDestinationManager().destroyTopic(name);
   }

   public void deployQueue(String name, String jndiName) throws Exception
   {
      serverPeer.getDestinationManager().createQueue(name, jndiName);
   }

   public void undeployQueue(String name) throws Exception
   {
      serverPeer.getDestinationManager().destroyQueue(name);
   }

   public void setSecurityConfig(String destName, String config) throws Exception
   {
      Element element = ServerManagement.toElement(config);
      serverPeer.setSecurityConfig(destName, element);
   }

   public void setDefaultSecurityConfig(String config) throws Exception
   {
      Element element = ServerManagement.toElement(config);
      serverPeer.setDefaultSecurityConfig(element);
   }

   public String getDefaultSecurityConfig() throws Exception
   {
      return serverPeer.getDefaultSecurityConfig().toString();
   }

   // Public --------------------------------------------------------

   public boolean isRemote()
   {
      return remote;
   }

   // Package protected ---------------------------------------------

   ServiceContainer getServiceContainer()
   {
      return sc;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private RMINamingDelegate getNamingDelegate()
   {
      return namingDelegate;
   }

   // Inner classes -------------------------------------------------

   public class VMKiller implements Runnable
   {
      public void run()
      {
         log.info("shutting down the VM");

         try
         {
            Thread.sleep(250);
         }
         catch(Exception e)
         {
            log.warn("interrupted while sleeping", e);
         }

         System.exit(0);
      }
   }
}
