/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.remoting;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import junit.framework.TestCase;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.ConnectionRegistryLocator;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.mina.MinaAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class PingTest extends TestCase
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(PingTest.class);

   private static final long PING_INTERVAL = 500;

   // Attributes ----------------------------------------------------

   private MessagingService messagingService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.getConnectionParams().setPingInterval(PING_INTERVAL);
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(config);
      messagingService.getServer().getRemotingService().registerAcceptorFactory(new MinaAcceptorFactory());
      messagingService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      messagingService.stop();
   }
   
   class Listener implements FailureListener
   {
      volatile MessagingException me;
      
      public void connectionFailed(MessagingException me)
      {
         log.info("** connection failed");
         this.me = me;
      }
      
      public MessagingException getException()
      {
         return me;
      }
   };

   /*
    * Test that no failure listeners are triggered in a non failure case with pinging going on
    */
   public void testNoFailureWithPinging() throws Exception
   {           
      Location location = new LocationImpl(TCP, "localhost", ConfigurationImpl.DEFAULT_PORT);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setPingInterval(PING_INTERVAL);
      
      ConnectionRegistry registry = ConnectionRegistryLocator.getRegistry();
      
      RemotingConnection conn = null;
      
      try
      {         
         conn = registry.getConnection(location, connectionParams);
         assertNotNull(conn);
         assertEquals(1, registry.getCount(location));
         
         Listener clientListener = new Listener();
         
         conn.addFailureListener(clientListener);
         
         //It's async so need to wait a while
         Thread.sleep(1000);
         
         RemotingConnection serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         
         Listener serverListener = new Listener();
         
         serverConn.addFailureListener(serverListener);
         
         Thread.sleep(PING_INTERVAL * 3);
         
         assertNull(clientListener.getException());
         
         assertNull(serverListener.getException());
         
         RemotingConnection serverConn2 = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         
         assertTrue(serverConn == serverConn2);      
      }
      finally
      {
         try
         {
            registry.returnConnection(location);
         }
         catch (Exception ignore)
         {            
         }
      }
   }
   
   /*
    * Test that no failure listeners are triggered in a non failure case with no pinging going on
    */
   public void testNoFailureNoPinging() throws Exception
   {           
      ConfigurationImpl config = new ConfigurationImpl();
      config.getConnectionParams().setPingInterval(-1);
      messagingService.stop();
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(config);
      messagingService.getServer().getRemotingService().registerAcceptorFactory(new MinaAcceptorFactory());
      messagingService.start();
            
      Location location = new LocationImpl(TCP, "localhost", ConfigurationImpl.DEFAULT_PORT);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setPingInterval(-1);
      
      ConnectionRegistry registry = ConnectionRegistryLocator.getRegistry();
      
      RemotingConnection conn = null;
      
      try
      {        
         conn = registry.getConnection(location, connectionParams);
         assertNotNull(conn);
         assertEquals(1, registry.getCount(location));
         
         Listener clientListener = new Listener();
         
         conn.addFailureListener(clientListener);
         
         //It's async so need to wait a while
         Thread.sleep(1000);
         
         RemotingConnection serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         
         Listener serverListener = new Listener();
         
         serverConn.addFailureListener(serverListener);
         
         Thread.sleep(PING_INTERVAL * 3);
         
         assertNull(clientListener.getException());
         
         assertNull(serverListener.getException());
         
         RemotingConnection serverConn2 = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         
         assertTrue(serverConn == serverConn2);      
      }
      finally
      {
         try
         {
            registry.returnConnection(location);
         }
         catch (Exception ignore)
         {            
         }
      }
   }
   
   /*
    * Test the server timing out a connection since it doesn't receive a ping in time
    */
   public void testServerFailureNoPing() throws Exception
   {
      Location location = new LocationImpl(TCP, "localhost", ConfigurationImpl.DEFAULT_PORT);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setPingInterval(PING_INTERVAL * 2);
      
      ConnectionRegistry registry = ConnectionRegistryLocator.getRegistry();
      
      RemotingConnection conn = null;
      
      try
      {         
         conn = registry.getConnection(location, connectionParams);
         assertEquals(1, registry.getCount(location));
         assertNotNull(conn);
                
         Listener clientListener = new Listener();
         
         conn.addFailureListener(clientListener);
         
         //It's async so need to wait a while
         Thread.sleep(1000);
               
         RemotingConnection serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         
         Listener serverListener = new Listener();
         
         serverConn.addFailureListener(serverListener);
         
         Thread.sleep(PING_INTERVAL * 4);
         
         //The client listener should be called too since the server will close it from the server side which will result in the
         //MINA detecting closure on the client side and then calling failure listener
         assertNotNull(clientListener.getException());
         
         assertNotNull(serverListener.getException());
         
         assertTrue(messagingService.getServer().getRemotingService().getConnections().isEmpty());
         
         //Make sure we don't get the same connection back - it should have been removed from the registry
         
         RemotingConnection conn2 = registry.getConnection(location, connectionParams);
         assertNotNull(conn2);
         
         assertFalse(conn == conn2);
         
      }
      finally
      {
         try
         {
            registry.returnConnection(location);
         }
         catch (Exception ignore)
         {            
         }
      }
   }
   
   /*
   * Test the client triggering failure due to no pong received in time
   */
  public void testClientFailureNoPong() throws Exception
  {
     Interceptor noPongInterceptor = new Interceptor()
     {
        public boolean intercept(Packet packet) throws MessagingException
        {
           if (packet.getType() == PacketImpl.PING)
           {
              log.info("Not sending pong");
              return false;
           }
           else
           {
              return true;
           }
        }
     };
     
     messagingService.getServer().getRemotingService().getDispatcher().addInterceptor(noPongInterceptor);
          
     Location location = new LocationImpl(TCP, "localhost", ConfigurationImpl.DEFAULT_PORT);
     ConnectionParams connectionParams = new ConnectionParamsImpl();
     connectionParams.setPingInterval(PING_INTERVAL);
     
     ConnectionRegistry registry = ConnectionRegistryLocator.getRegistry();
     
     RemotingConnection conn = null;
     
     try
     {         
        conn = registry.getConnection(location, connectionParams);
        assertNotNull(conn);
        assertEquals(1, registry.getCount(location));
               
        Listener clientListener = new Listener();
        
        conn.addFailureListener(clientListener);
        
        //It's async so need to wait a while
        Thread.sleep(1000);
              
        RemotingConnection serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
        
        Listener serverListener = new Listener();
        
        serverConn.addFailureListener(serverListener);
        
        Thread.sleep(PING_INTERVAL * 2);
        
        assertNotNull(clientListener.getException());
        
        //We don't receive an exception on the server in this case
        assertNull(serverListener.getException());
        
        assertTrue(messagingService.getServer().getRemotingService().getConnections().isEmpty());
        
        //Make sure we don't get the same connection back - it should have been removed from the registry
        
        RemotingConnection conn2 = registry.getConnection(location, connectionParams);
        assertNotNull(conn2);        
     }
     finally
     {
        messagingService.getServer().getRemotingService().getDispatcher().removeInterceptor(noPongInterceptor);
        try
        {
           registry.returnConnection(location);
        }
        catch (Exception ignore)
        {            
        }
     }
  }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}