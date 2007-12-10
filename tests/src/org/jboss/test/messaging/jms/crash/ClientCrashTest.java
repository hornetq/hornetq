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

import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.LocalTestServer;
import org.jboss.test.messaging.tools.container.Server;
import org.jboss.test.messaging.tools.container.ServiceContainer;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * 
 * A ClientCrashTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientCrashTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ClientCrashTest.class);
   
   // Attributes ----------------------------------------------------
   
   protected Server localServer;
   
   protected Server remoteServer;

   // Constructors --------------------------------------------------

   public ClientCrashTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
   	//Server might have been left around by other tests
   	kill(1);
   	
      //stop();
      
      // Start the local server
      localServer = new LocalTestServer(1);
      localServer.start(getContainerConfig(), getConfiguration(), false);
      
      // Start all the services locally
      //localServer.start("all", true);

      // This crash test is relying on a precise value of LeaseInterval, so we don't rely on
      // the default, whatever that is ...      
       
      localServer.deployQueue("Queue", null, false);
      
      localServer.deployTopic("Topic", null, false);
       
      // Connect to the remote server, but don't start a servicecontainer on it. We are only using
      // the remote server to open a client connection to the local server.
      //ServerManagement.create();
          
      remoteServer = servers.get(0);

      super.setUp();
      
   }

   public void tearDown() throws Exception
   {       
      localServer.stop();
      
      super.tearDown();
   }
   
   private void performCrash(long wait, boolean contains) throws Exception
   {
   	InitialContext theIC = getInitialContext();
      
      ConnectionFactory cf = (ConnectionFactory)theIC.lookup("/ConnectionFactory");
      
      Queue queue = (Queue)theIC.lookup("/queue/Queue");
      
      CreateClientOnServerCommand command = new CreateClientOnServerCommand(cf, queue, true);
      
      String remotingSessionId = (String)remoteServer.executeCommand(command);
      
      ConnectionManager cm = localServer.getServerPeer().getConnectionManager();
            
      assertTrue(cm.containsRemotingSession(remotingSessionId));
      
      // Now we should have a client connection from the remote server to the local server
      
      ServerManagement.kill(0);

      log.trace("killed remote server");
      
      //Wait for connection resources to be cleared up
      Thread.sleep(wait);
           
      // See if we still have a connection with this id
      assertEquals(contains, cm.containsRemotingSession(remotingSessionId));
   }
      
   /**
    * Test that when a remote jms client crashes, server side resources for connections are
    * cleaned-up.
    */
   public void testClientCrash() throws Exception
   {
   	localServer.setAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "LeasePeriod", "2000");
   	
      performCrash(8000, false);
   }
   
   public void testClientCrashLargeLease() throws Exception
   {
      localServer.setAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "LeasePeriod", "30000");      
   	
      performCrash(8000, true);
   }

   public void testClientCrashNegativeLease() throws Exception
   {
      //Set lease period to -1 --> this should disable leasing so the state won't be cleared up
      
      localServer.setAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "LeasePeriod", "-1");       
   	
      performCrash(8000, true);
   }
   
   public void testClientCrashZeroLease() throws Exception
   {
      //Set lease period to 0 --> this should disable leasing so the state won't be cleared up
      
      localServer.setAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "LeasePeriod", "0");     
   	
      performCrash(8000, true);
   }
   
   public void testClientCrashWithTwoConnections() throws Exception
   {
      localServer.setAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "LeasePeriod", "2000");      
   	
      InitialContext ic = getInitialContext();
      Topic topic = (Topic)ic.lookup("/topic/Topic");
      
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      CreateTwoClientOnServerCommand command = new CreateTwoClientOnServerCommand(cf, topic, true);
      
      String remotingSessionId[] = (String[])remoteServer.executeCommand(command);
      
      ConnectionManager cm = localServer.getServerPeer().getConnectionManager();
            
      log.info("server(0) = " + remotingSessionId[0]);
      log.info("server(1) = " + remotingSessionId[1]);
      log.info("we have = " + ((SimpleConnectionManager)cm).getClients().size() + " clients registered on SimpleconnectionManager");
      
      assertFalse(cm.containsRemotingSession(remotingSessionId[0]));
      assertTrue(cm.containsRemotingSession(remotingSessionId[1]));
      
      ServerManagement.kill(0);
      
      // Now we should have a client connection from the remote server to the local server
      
      log.info("killed remote server");
        
      // Wait for connection resources to be cleared up
      Thread.sleep(8000);
           
      // See if we still have a connection with this id
      
      //Connection state should have been cleared up by now
      assertFalse(cm.containsRemotingSession(remotingSessionId[0]));
      assertFalse(cm.containsRemotingSession(remotingSessionId[1]));
      
      log.info("Servers = " + ((SimpleConnectionManager)cm).getClients().size());
      
      assertEquals(0,((SimpleConnectionManager)cm).getClients().size());
   }
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
  
   // Inner classes -------------------------------------------------

}
