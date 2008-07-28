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

package org.jboss.messaging.tests.unit.core.client.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientConnectionImpl;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.core.version.impl.VersionImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.VersionLoader;

/**
 * 
 * A ClientConnectionFactoryImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientConnectionFactoryImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientConnectionFactoryImplTest.class);
   
   public void testWideConstructor() throws Exception
   {
      final Location location = new LocationImpl(TransportType.TCP, "aardvarks");
      
      final ConnectionParams params = new ConnectionParamsImpl();
      
      testCreateClientConnectionFactoryImpl(location, params,
            32342, 1254, 152454, 15454, false, false, false);
      
      testCreateClientConnectionFactoryImpl(location, params,
            65465, 5454, 6544, 654654, true, true, true);      
   }
   
   public void testLocationOnlyConstructor() throws Exception
   {
      final Location location = new LocationImpl(TransportType.TCP, "bullfrog");
      
      final ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      
      assertTrue(cf.getLocation() == location);
      
      ConnectionParams params = new ConnectionParamsImpl();
      assertEquals(params, cf.getConnectionParams());
      
      checkDefaults(cf);
   }
    
   public void testLocationAndParamsOnlyConstructor() throws Exception
   {
      final Location location = new LocationImpl(TransportType.TCP, "bullfrog");
      
      final ConnectionParams params = new ConnectionParamsImpl();
      
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location, params);
      
      assertTrue(cf.getLocation() == location);
            
      assertTrue(params == cf.getConnectionParams());
      
      checkDefaults(cf);
   }   
   
   public void testGetSetAttributes() throws Exception
   {
      final Location location = new LocationImpl(TransportType.TCP, "echidna");
      
      final ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      
      checkGetSetAttributes(cf, new ConnectionParamsImpl(), 12312, 1231, 23424, 123213, false, false, false);
      checkGetSetAttributes(cf, new ConnectionParamsImpl(), 656, 3453, 4343, 6556, true, true, true);      
   }   
   
   public void testCreateConnection() throws Throwable
   {
      testCreateConnectionWithUsernameAndPassword(null, null);
   }
   
   public void testCreateConnectionWithUsernameAndPassword() throws Throwable
   {
      testCreateConnectionWithUsernameAndPassword("bob", "wibble");
   }
   
   public void testMessagingExceptionOnCreateConnection() throws Throwable
   {
      final Location location = new LocationImpl(TransportType.TCP, "baked beans");
      
      final ConnectionParams params = new ConnectionParamsImpl();
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
      
      ConnectionRegistry cr = EasyMock.createStrictMock(ConnectionRegistry.class);
      
      ClientConnectionFactoryImpl cf =
         new ClientConnectionFactoryImpl(location, params,
               32432, 4323,
               453453, 54543, false,
               false, false);
      
      cf.setConnectionRegistry(cr);
      
      MessagingException me = new MessagingException(1234, "blahblah");
      
      EasyMock.expect(cr.getConnection(location, params)).andReturn(rc);
      
      Version clientVersion = VersionLoader.load();
      
      CreateConnectionRequest request =
         new CreateConnectionRequest(clientVersion.getIncrementingVersion(), null, null);
        
      EasyMock.expect(rc.sendBlocking(0, 0, request)).andThrow(me);
      
      cr.returnConnection(location);
      
      EasyMock.replay(cr, rc);
      
      try
      {
         cf.createConnection();
         
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertTrue(e == me);
      }
      
      EasyMock.verify(cr, rc);
   }
   
   // Private -----------------------------------------------------------------------------------------------------------
      
   private void testCreateConnectionWithUsernameAndPassword(final String username, final String password) throws Throwable
   {
      final Location location = new LocationImpl(TransportType.TCP, "cheesecake");
      
      final ConnectionParams params = new ConnectionParamsImpl();
      
      ConnectionRegistry cr = EasyMock.createStrictMock(ConnectionRegistry.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      
      ClientConnectionFactoryImpl cf =
         new ClientConnectionFactoryImpl(location, params,
               32432, 4323,
               453453, 54543, false,
               false, false);
      
      cf.setConnectionRegistry(cr);
            
      EasyMock.expect(cr.getConnection(location, params)).andReturn(rc);
      
      Version clientVersion = VersionLoader.load();
      
      CreateConnectionRequest request =
         new CreateConnectionRequest(clientVersion.getIncrementingVersion(), username, password);
      
      final long connTargetID = 5425142;
      
      Version serverVersion = new VersionImpl("blah", 1, 1, 1, 12, "blah");
      
      CreateConnectionResponse response = 
         new CreateConnectionResponse(connTargetID, serverVersion);
      
      EasyMock.expect(rc.sendBlocking(0, 0, request)).andReturn(response);

      EasyMock.expect(rc.getPacketDispatcher()).andReturn(dispatcher);
      rc.addFailureListener((FailureListener) EasyMock.anyObject());
      EasyMock.replay(cr, rc, dispatcher);
      
      ClientConnection conn;
      
      if (username == null)         
      {
         conn = cf.createConnection();
      }
      else
      {
         conn = cf.createConnection(username, password);
      }
         
      EasyMock.verify(cr, rc, dispatcher);
      
      assertTrue(conn instanceof ClientConnectionImpl);
      
      assertEquals(serverVersion.getFullVersion(), conn.getServerVersion().getFullVersion());
   }
   
   private void testCreateClientConnectionFactoryImpl(final Location location, final ConnectionParams params,
         final int defaultConsumerWindowSize, final int defaultConsumerMaxRate,
         final int defaultProducerWindowSize, final int defaultProducerMaxRate,
         final boolean defaultBlockOnAcknowledge,
         final boolean defaultSendNonPersistentMessagesBlocking,
         final boolean defaultSendPersistentMessagesBlocking) throws Exception
   {
      ClientConnectionFactory cf =
         new ClientConnectionFactoryImpl(location, params, defaultConsumerWindowSize, defaultConsumerMaxRate,
               defaultProducerWindowSize, defaultProducerMaxRate, defaultBlockOnAcknowledge,
               defaultSendNonPersistentMessagesBlocking, defaultSendPersistentMessagesBlocking);
      
      assertTrue(location == cf.getLocation());
      assertTrue(params == cf.getConnectionParams());
      assertEquals(defaultConsumerWindowSize, cf.getDefaultConsumerWindowSize());
      assertEquals(defaultConsumerMaxRate, cf.getDefaultConsumerMaxRate());
      assertEquals(defaultProducerWindowSize, cf.getDefaultProducerWindowSize());
      assertEquals(defaultProducerMaxRate, cf.getDefaultProducerMaxRate());
      assertEquals(defaultBlockOnAcknowledge, cf.isDefaultBlockOnAcknowledge());
      assertEquals(defaultSendNonPersistentMessagesBlocking, cf.isDefaultBlockOnNonPersistentSend());
      assertEquals(defaultSendPersistentMessagesBlocking, cf.isDefaultBlockOnPersistentSend());
   }
   
   private void checkDefaults(final ClientConnectionFactory cf) throws Exception
   {
      assertEquals(ClientConnectionFactoryImpl.DEFAULT_DEFAULT_CONSUMER_WINDOW_SIZE, cf.getDefaultConsumerWindowSize());
      assertEquals(ClientConnectionFactoryImpl.DEFAULT_DEFAULT_CONSUMER_MAX_RATE, cf.getDefaultConsumerMaxRate());
      assertEquals(ClientConnectionFactoryImpl.DEFAULT_DEFAULT_PRODUCER_WINDOW_SIZE, cf.getDefaultProducerWindowSize());
      assertEquals(ClientConnectionFactoryImpl.DEFAULT_DEFAULT_PRODUCER_MAX_RATE, cf.getDefaultProducerMaxRate());
      assertEquals(ClientConnectionFactoryImpl.DEFAULT_DEFAULT_BLOCK_ON_ACKNOWLEDGE, cf.isDefaultBlockOnAcknowledge());
      assertEquals(ClientConnectionFactoryImpl.DEFAULT_DEFAULT_BLOCK_ON_PERSISTENT_SEND, cf.isDefaultBlockOnNonPersistentSend());
      assertEquals(ClientConnectionFactoryImpl.DEFAULT_DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND, cf.isDefaultBlockOnPersistentSend());      
   }
   
   private void checkGetSetAttributes(ClientConnectionFactory cf,
         final ConnectionParams params,
         final int defaultConsumerWindowSize, final int defaultConsumerMaxRate,
         final int defaultProducerWindowSize, final int defaultProducerMaxRate,
         final boolean defaultBlockOnAcknowledge,
         final boolean defaultBlockOnPersistentSend,
         final boolean defaultBlockOnNonPersistentSend)
   {
      cf.setConnectionParams(params);
      assertTrue(params == cf.getConnectionParams());
      cf.setDefaultConsumerWindowSize(defaultConsumerWindowSize);
      assertEquals(defaultConsumerWindowSize, cf.getDefaultConsumerWindowSize());
      cf.setDefaultConsumerMaxRate(defaultConsumerMaxRate);
      assertEquals(defaultConsumerMaxRate, cf.getDefaultConsumerMaxRate());
      cf.setDefaultProducerWindowSize(defaultProducerWindowSize);
      assertEquals(defaultProducerWindowSize, cf.getDefaultProducerWindowSize());
      cf.setDefaultProducerMaxRate(defaultProducerMaxRate);
      assertEquals(defaultProducerMaxRate, cf.getDefaultProducerMaxRate());
      cf.setDefaultBlockOnAcknowledge(defaultBlockOnAcknowledge);
      assertEquals(defaultBlockOnAcknowledge, cf.isDefaultBlockOnAcknowledge());
      cf.setDefaultBlockOnPersistentSend(defaultBlockOnPersistentSend);
      assertEquals(defaultBlockOnPersistentSend, cf.isDefaultBlockOnPersistentSend());
      cf.setDefaultBlockOnNonPersistentSend(defaultBlockOnNonPersistentSend);
      assertEquals(defaultBlockOnNonPersistentSend, cf.isDefaultBlockOnNonPersistentSend());
   }
   
}
