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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionManager;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.ConnectionManagerImpl;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.netty.NettyConnectorFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
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
      config.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.netty.NettyAcceptorFactory"));
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(config);
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
      ConnectorFactory cf = new NettyConnectorFactory();
      Map<String, Object> params = new HashMap<String, Object>();
      
      ConnectionManager cm = new ConnectionManagerImpl(cf, params, PING_INTERVAL, 5000, 1);
      
      RemotingConnection conn = cm.getConnection();
      assertNotNull(conn);
      assertEquals(1, cm.numConnections());
      
      Listener clientListener = new Listener();
      
      conn.addFailureListener(clientListener);
      
      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = messagingService.getServer().getRemotingService().getConnections();
         
         if (!conns.isEmpty())
         {            
            serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         }
         else
         {
            //It's async so need to wait a while
            Thread.sleep(10);
         }
      }
      
      Listener serverListener = new Listener();
      
      serverConn.addFailureListener(serverListener);
      
      Thread.sleep(PING_INTERVAL * 3);
      
      assertNull(clientListener.getException());
      
      assertNull(serverListener.getException());
      
      RemotingConnection serverConn2 = messagingService.getServer().getRemotingService().getConnections().iterator().next();
      
      assertTrue(serverConn == serverConn2);    
        
      cm.returnConnection(conn.getID());
   }
   
   /*
    * Test that no failure listeners are triggered in a non failure case with no pinging going on
    */
   public void testNoFailureNoPinging() throws Exception
   {           
      ConnectorFactory cf = new NettyConnectorFactory();
      Map<String, Object> params = new HashMap<String, Object>();
      
      ConnectionManager cm = new ConnectionManagerImpl(cf, params, PING_INTERVAL, 5000, 1);
      
      RemotingConnection conn = cm.getConnection();
      assertNotNull(conn);
      assertEquals(1, cm.numConnections());
      
      Listener clientListener = new Listener();
      
      conn.addFailureListener(clientListener);
      
      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = messagingService.getServer().getRemotingService().getConnections();
         
         if (!conns.isEmpty())
         {            
            serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         }
         else
         {
            //It's async so need to wait a while
            Thread.sleep(10);
         }
      }
      
      Listener serverListener = new Listener();
      
      serverConn.addFailureListener(serverListener);
      
      Thread.sleep(PING_INTERVAL * 3);
      
      assertNull(clientListener.getException());
      
      assertNull(serverListener.getException());
      
      RemotingConnection serverConn2 = messagingService.getServer().getRemotingService().getConnections().iterator().next();
      
      assertTrue(serverConn == serverConn2); 
      
      cm.returnConnection(conn.getID());
   }
   
   /*
    * Test the server timing out a connection since it doesn't receive a ping in time
    */
   public void testServerFailureNoPing() throws Exception
   {            
      ConnectorFactory cf = new NettyConnectorFactory();
      Map<String, Object> params = new HashMap<String, Object>();
      
      ConnectionManager cm = new ConnectionManagerImpl(cf, params, PING_INTERVAL, 5000, 1);
      
      RemotingConnectionImpl conn = (RemotingConnectionImpl)cm.getConnection();
      assertEquals(1, cm.numConnections());
      assertNotNull(conn);
        
      //We need to get it to send one ping then stop         
      conn.stopPingingAfterOne();
             
      Listener clientListener = new Listener();
      
      conn.addFailureListener(clientListener);
                      
      RemotingConnection serverConn = null;
      
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = messagingService.getServer().getRemotingService().getConnections();
         
         if (!conns.isEmpty())
         {            
            serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         }
         else
         {
            //It's async so need to wait a while
            Thread.sleep(10);
         }
      }
                  
      Listener serverListener = new Listener();
      
      serverConn.addFailureListener(serverListener);
      
      Thread.sleep(PING_INTERVAL * 10);
      
      //The client listener should be called too since the server will close it from the server side which will result in the
      //MINA detecting closure on the client side and then calling failure listener
      assertNotNull(clientListener.getException());
      
      assertNotNull(serverListener.getException());
      
      assertTrue(messagingService.getServer().getRemotingService().getConnections().isEmpty());
      
      //Make sure we don't get the same connection back - it should have been removed from the registry
      
      RemotingConnection conn2 = cm.getConnection();
      assertNotNull(conn2);
      
      cm.returnConnection(conn2.getID());    
   }

   /*
   * Test the client triggering failure due to no pong received in time
   */
  public void testClientFailureNoPong() throws Exception
  {
     Interceptor noPongInterceptor = new Interceptor()
     {
        public boolean intercept(Packet packet, RemotingConnection conn) throws MessagingException
        {
           log.info("In interceptor, packet is " + packet.getType());
           if (packet.getType() == PacketImpl.PING)
           {
              return false;
           }
           else
           {
              return true;
           }
        }
     };
     
     messagingService.getServer().getRemotingService().addInterceptor(noPongInterceptor);
          
     ConnectorFactory cf = new NettyConnectorFactory();
     Map<String, Object> params = new HashMap<String, Object>();
     
     ConnectionManager cm = new ConnectionManagerImpl(cf, params, PING_INTERVAL, 5000, 1);
     
     RemotingConnection conn = cm.getConnection();
     assertNotNull(conn);
     assertEquals(1, cm.numConnections());
            
     Listener clientListener = new Listener();
     
     conn.addFailureListener(clientListener);
     
     RemotingConnection serverConn = null;
     while (serverConn == null)
     {
        Set<RemotingConnection> conns = messagingService.getServer().getRemotingService().getConnections();
        
        if (!conns.isEmpty())
        {            
           serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
        }
        else
        {
           //It's async so need to wait a while
           Thread.sleep(10);
        }
     }
     
     Listener serverListener = new Listener();
     
     serverConn.addFailureListener(serverListener);
     
     Thread.sleep(PING_INTERVAL * 2);
     
     assertNotNull(clientListener.getException());
     
     //Sleep a bit more since it's async
     Thread.sleep(PING_INTERVAL);
     
     //We don't receive an exception on the server in this case
     assertNull(serverListener.getException());
     
     assertTrue(messagingService.getServer().getRemotingService().getConnections().isEmpty());
     
     //Make sure we don't get the same connection back - it should have been removed from the registry
     
     RemotingConnection conn2 = cm.getConnection();
     assertNotNull(conn2);        

     messagingService.getServer().getRemotingService().removeInterceptor(noPongInterceptor);
     
     cm.returnConnection(conn2.getID());
     
  }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}