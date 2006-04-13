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
import javax.jms.Queue;
import javax.naming.InitialContext;

import org.jboss.jms.server.ConnectionManager;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.jms.CreateClientOnServerCommand;
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
 * @version 1.1
 *
 * ClientCrashTest.java,v 1.1 2006/02/21 08:22:28 timfox Exp
 */
public class ClientCrashTest extends MessagingTestCase
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
      super.setUp();
      
      // Start the local server
      localServer = new LocalTestServer();
      
      // Start all the services locally
      localServer.start("all");


      // This crash test is relying on a precise value of LeaseInterval, so we don't rely on
      // the default, whatever that is ...

      localServer.setAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "LeasePeriod", "3000");
       
      localServer.deployQueue("Queue", null);
          
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
   public void testClientCrash() throws Exception
   {
      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      Queue queue = (Queue)ic.lookup("/queue/Queue");
      
      CreateClientOnServerCommand command = new CreateClientOnServerCommand(cf, queue, true);
      
      String remotingSessionId = (String)remoteServer.executeCommand(command);
      
      ConnectionManager cm = localServer.getServerPeer().getConnectionManager();
            
      assertTrue(cm.containsSession(remotingSessionId));
      
      // Now we should have a client connection from the remote server to the local server
      
      remoteServer.exit();
      log.info("killed remote server");
        
      // Wait for connection resources to be cleared up
      Thread.sleep(15000);
           
      // See if we still have a connection with this id
      assertFalse(cm.containsSession(remotingSessionId));            
   }
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
  
   // Inner classes -------------------------------------------------

}
