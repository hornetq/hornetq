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
package org.jboss.test.messaging.jms.crash;

import javax.jms.ConnectionFactory;
import javax.jms.Topic;
import javax.naming.InitialContext;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.tools.jmx.rmi.LocalTestServer;
import org.jboss.test.messaging.tools.jmx.rmi.Server;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;

/**
 * 
 * A ClientCrashTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientCrashTwoConnectionsTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ClientCrashTwoConnectionsTest.class);
   
   // Attributes ----------------------------------------------------
   
   protected Server localServer;
   
   protected Server remoteServer;

   // Constructors --------------------------------------------------

   public ClientCrashTwoConnectionsTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      
      // Start the local server
      localServer = new LocalTestServer();
      
      // Start all the services locally
      localServer.start("all", true);

      // This crash test is relying on a precise value of LeaseInterval, so we don't rely on
      // the default, whatever that is ...

      localServer.setAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "LeasePeriod", "3000");
       
      localServer.deployQueue("Queue", null, false);
      localServer.deployTopic("Topic", null, false);
          
      // Connect to the remote server, but don't start a servicecontainer on it. We are only using
      // the remote server to open a client connection to the local server.
      ServerManagement.create();
          
      remoteServer = ServerManagement.getServer();

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {       
      localServer.stop();
   }
      
   /**
    * Test that when a remote jms client crashes, server side resources for connections are
    * cleaned-up.
    */
   public void testClientCrashWithTwoConnections() throws Exception
   {
      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      Topic topic = (Topic)ic.lookup("/topic/Topic");
      
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      CreateTwoClientOnServerCommand command = new CreateTwoClientOnServerCommand(cf, topic, true);
      
      String remotingSessionId[] = (String[])remoteServer.executeCommand(command);
      
      ConnectionManager cm = localServer.getServerPeer().getConnectionManager();
            
      log.info("server(0) = " + remotingSessionId[0]);
      log.info("server(1) = " + remotingSessionId[1]);
      log.info("we have = " + ((SimpleConnectionManager)cm).getClients().size() + " clients registered on SimpleconnectionManager");
      
      assertFalse(cm.containsSession(remotingSessionId[0]));            
      assertTrue(cm.containsSession(remotingSessionId[1]));          
      
      // Now we should have a client connection from the remote server to the local server
      remoteServer.kill();
      log.info("killed remote server");
        
      // Wait for connection resources to be cleared up
      Thread.sleep(25000);
           
      // See if we still have a connection with this id
      
      //Connection state should have been cleared up by now
      assertFalse(cm.containsSession(remotingSessionId[0]));            
      assertFalse(cm.containsSession(remotingSessionId[1]));            
      
      log.info("Servers = " + ((SimpleConnectionManager)cm).getClients().size());
      
      assertEquals(0,((SimpleConnectionManager)cm).getClients().size());
   }
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
  
   // Inner classes -------------------------------------------------

}
