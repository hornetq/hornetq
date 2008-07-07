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

package org.jboss.messaging.tests.unit.core.remoting.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class RemotingConnectionImplTest extends UnitTestCase
{
   protected void tearDown() throws Exception
   {
      super.tearDown();
      ConnectorRegistryFactory.setRegisteryLocator(null);
   }

   public void testNullLocationThrowsException()
   {
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      try
      {
         new RemotingConnectionImpl(null, connectionParams);
         fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
   }

   public void testNullConnectionParamsThrowsException()
   {
      Location location = EasyMock.createNiceMock(Location.class);
      try
      {
         new RemotingConnectionImpl(location, null);
         fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
   }

   public void testConnectionStarted() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      EasyMock.replay(connector);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
   }

   public void testConnectionStartedAndStopped() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.expect(connectorRegistry.removeConnector(location)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      EasyMock.expect(connector.disconnect()).andReturn(true);
      EasyMock.replay(connector);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      remotingConnection.stop();
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      assertEquals(-1, remotingConnection.getSessionID());

   }

   public void testConnectionListenerRemovedOnStop() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      RemotingSessionListener listener = EasyMock.createNiceMock(RemotingSessionListener.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.expect(connectorRegistry.removeConnector(location)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      connector.addSessionListener(listener);
      connector.removeSessionListener(listener);
      EasyMock.expect(connector.disconnect()).andReturn(true);
      EasyMock.replay(connector);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      remotingConnection.addRemotingSessionListener(listener);
      remotingConnection.stop();
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      assertEquals(-1, remotingConnection.getSessionID());

   }

   public void testConnectionGetSessionId() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      EasyMock.replay(connector);
      EasyMock.expect(remotingSession.isConnected()).andReturn(true);
      EasyMock.expect(remotingSession.getID()).andReturn((123l));
      EasyMock.replay(remotingSession);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      assertEquals(123l, remotingConnection.getSessionID());

      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(remotingSession);
   }

   public void testConnectionGetSessionIdDisconnected() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      EasyMock.replay(connector);
      EasyMock.expect(remotingSession.isConnected()).andReturn(false);
      //EasyMock.expect(nioSession.getID()).andReturn((123l));
      EasyMock.replay(remotingSession);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      assertEquals(-1, remotingConnection.getSessionID());

      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(remotingSession);
   }

   public void testConnectionGetSessionIdStopped() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      EasyMock.replay(connector);
      EasyMock.expect(remotingSession.isConnected()).andReturn(true);
      EasyMock.expect(remotingSession.getID()).andReturn((123l));
      EasyMock.replay(remotingSession);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      remotingConnection.stop();
      assertEquals(123l, remotingConnection.getSessionID());

      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(remotingSession);
   }

   public void testConnectionSendBlocking() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      DummySession nioSession = new DummySession(dispatcher, 0, null, false);
      PacketHandler handler = null;

      Packet packet = EasyMock.createStrictMock(Packet.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(nioSession);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.replay(connector);
      packet.setTargetID(1);
      packet.setExecutorID(2);
      packet.setResponseTargetID(0);
      EasyMock.replay(packet);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      remotingConnection.sendBlocking(1, 2, packet);
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(packet);
      assertNotNull(nioSession.getPacketDispatched());
   }

   public void testConnectionSendBlockingThrowsExceptionIfSessionNull() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      DummySession nioSession = new DummySession(dispatcher, 0, null, false);
      PacketHandler handler = null;

      Packet packet = EasyMock.createStrictMock(Packet.class);

      //EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher).anyTimes();
      EasyMock.replay(connector);
      //packet.setTargetID(1);
      //packet.setExecutorID(2);
      // packet.setResponseTargetID(0);
      EasyMock.replay(packet);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      try
      {
         remotingConnection.sendBlocking(1, 2, packet);
         fail("should throw exception");
      }
      catch (IllegalStateException e)
      {
         //pass
      }
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(packet);
      assertNull(nioSession.getPacketDispatched());
   }

   public void testConnectionSendBlockingThrowsExceptionWhenSessionNotConnected() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      RemotingSession nioSession = EasyMock.createStrictMock(RemotingSession.class);
      PacketHandler handler = null;

      Packet packet = EasyMock.createStrictMock(Packet.class);
      EasyMock.expect(nioSession.isConnected()).andReturn(false);
      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(nioSession);
      EasyMock.replay(connector, nioSession);
      EasyMock.replay(packet);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      try
      {
         remotingConnection.sendBlocking(1, 2, packet);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         //pass
      }
      EasyMock.verify(connector, connectorRegistry, packet, nioSession);
   }

   public void testConnectionSendBlockingThrowsMessagingException() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      DummySession nioSession = new DummySession(dispatcher, 0, null, false);
      PacketHandler handler = null;

      MessagingExceptionMessage packet = new MessagingExceptionMessage(new MessagingException());

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(nioSession);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.replay(connector);
      packet.setTargetID(1);
      packet.setExecutorID(2);
      packet.setResponseTargetID(0);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      try
      {
         remotingConnection.sendBlocking(1, 2, packet);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         //pass
      }
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      assertNotNull(nioSession.getPacketDispatched());
   }


   public void testConnectionSendBlockingErrorOnWrite() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      DummySession nioSession = new DummySession(dispatcher, 0, new Exception(), false);
      PacketHandler handler = null;

      Packet packet = EasyMock.createStrictMock(Packet.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(nioSession);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.expect(connector.getDispatcher()).andReturn(dispatcher);
      EasyMock.replay(connector);
      packet.setTargetID(1);
      packet.setExecutorID(2);
      packet.setResponseTargetID(0);
      EasyMock.replay(packet);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      try
      {
         remotingConnection.sendBlocking(1, 2, packet);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         //pass
      }
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(packet);
      assertNull(nioSession.getPacketDispatched());
   }

   public void testConnectionSendOneWay() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      DummySession nioSession = new DummySession(dispatcher, 0, null, true);
      PacketHandler handler = null;

      Packet packet = EasyMock.createStrictMock(Packet.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(nioSession);
      EasyMock.replay(connector);
      packet.setTargetID(1);
      packet.setExecutorID(2);
      EasyMock.replay(packet);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      remotingConnection.sendOneWay(1, 2, packet);
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(packet);
      assertNull(nioSession.getPacketDispatched());
   }

   public void testConnectionSendOneWayThrowsExceptionOnNullSession() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      DummySession nioSession = new DummySession(dispatcher, 0, null, true);
      PacketHandler handler = null;

      Packet packet = EasyMock.createStrictMock(Packet.class);

      EasyMock.replay(connectorRegistry);
      EasyMock.replay(connector);
      EasyMock.replay(packet);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      try
      {
         remotingConnection.sendOneWay(1, 2, packet);
         fail("should throw exception");
      }
      catch (IllegalStateException e)
      {
         //pass
      }
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(packet);
      assertNull(nioSession.getPacketDispatched());
   }

   public void testConnectionSendOneWayThrowsExceptionOnSessionNoConnected() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      RemotingSession nioSession = EasyMock.createStrictMock(RemotingSession.class);
      PacketHandler handler = null;

      Packet packet = EasyMock.createStrictMock(Packet.class);
      EasyMock.expect(nioSession.isConnected()).andReturn(false);
      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(nioSession);
      EasyMock.replay(connector);
      EasyMock.replay(packet);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      try
      {
         remotingConnection.sendOneWay(1, 2, packet);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         //pass
      }
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(packet);
   }

   public void testConnectionSendOneWayErrorOnWrite() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(1000);
      DummyDispatcher dispatcher = new DummyDispatcher();
      DummySession nioSession = new DummySession(dispatcher, 0, new Exception(), true);
      PacketHandler handler = null;

      Packet packet = EasyMock.createStrictMock(Packet.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(nioSession);
      EasyMock.replay(connector);
      packet.setTargetID(1);
      packet.setExecutorID(2);
      EasyMock.replay(packet);


      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      try
      {
         remotingConnection.sendOneWay(1, 2, packet);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         //pass
      }
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      EasyMock.verify(packet);
      assertNull(nioSession.getPacketDispatched());
   }

   public void testConnectionSetListener() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);
      RemotingSessionListener listener = EasyMock.createNiceMock(RemotingSessionListener.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      connector.addSessionListener(listener);
      EasyMock.replay(connector);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      remotingConnection.addRemotingSessionListener(listener);
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
   }

   public void testConnectionReSetListener() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);
      RemotingSessionListener listener = EasyMock.createNiceMock(RemotingSessionListener.class);
      RemotingSessionListener listener2 = EasyMock.createNiceMock(RemotingSessionListener.class);
      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      connector.addSessionListener(listener);
      connector.removeSessionListener(listener);
      connector.addSessionListener(listener2);
      EasyMock.replay(connector);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      remotingConnection.addRemotingSessionListener(listener);
      remotingConnection.removeRemotingSessionListener(listener);
      remotingConnection.addRemotingSessionListener(listener2);
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
   }


   public void testGetDispatcher() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      PacketDispatcher packetDispatcher = EasyMock.createNiceMock(PacketDispatcher.class);
      EasyMock.expect(connector.getDispatcher()).andReturn(packetDispatcher);
      EasyMock.replay(connector);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      assertEquals(remotingConnection.getPacketDispatcher(), packetDispatcher);
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
   }

   public void testGetLocation() throws Throwable
   {
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      EasyMock.replay(location, connectionParams);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      assertEquals(remotingConnection.getLocation(), location);
      EasyMock.verify(location, connectionParams);
   }
   
   public void testCreateBuffer() throws Throwable
   {
      final ConnectorRegistry connectorRegistry = EasyMock.createStrictMock(ConnectorRegistry.class);
      RemotingConnector connector = EasyMock.createStrictMock(RemotingConnector.class);
      ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
      {
         public ConnectorRegistry locate()
         {
            return connectorRegistry;
         }
      });
      Location location = EasyMock.createNiceMock(Location.class);
      ConnectionParams connectionParams = EasyMock.createNiceMock(ConnectionParams.class);
      RemotingSession remotingSession = EasyMock.createStrictMock(RemotingSession.class);

      EasyMock.expect(connectorRegistry.getConnector(location, connectionParams)).andReturn(connector);
      EasyMock.replay(connectorRegistry);
      EasyMock.expect(connector.connect()).andReturn(remotingSession);
      
      final int size = 120912;      
      MessagingBuffer buff = EasyMock.createMock(MessagingBuffer.class);
      EasyMock.expect(connector.createBuffer(size)).andReturn(buff);
      
      EasyMock.replay(connector);

      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(location, connectionParams);
      remotingConnection.start();
      MessagingBuffer buff2 = remotingConnection.createBuffer(size);
            
      EasyMock.verify(connector);
      EasyMock.verify(connectorRegistry);
      assertTrue(buff == buff2);
   }


   class DummyDispatcher implements PacketDispatcher
   {
      PacketHandler handler = null;

      public void register(PacketHandler handler)
      {
         this.handler = handler;
      }

      public void unregister(long handlerID)
      {
         //todo
      }

      public void setListener(PacketHandlerRegistrationListener listener)
      {
         //todo
      }

      public void dispatch(Packet packet, PacketReturner sender) throws Exception
      {
         handler.handle(packet, sender);
      }

      public void callFilters(Packet packet) throws Exception
      {
         //todo
      }

      public void addInterceptor(Interceptor filter)
      {
         //todo
      }

      public void removeInterceptor(Interceptor filter)
      {
         //todo
      }

      public long generateID()
      {
         return 0;
      }
   }

   class DummySession implements RemotingSession
   {
      PacketDispatcher dispatcher;
      Packet packetDispatched = null;
      long timeToReply = 0;
      Exception exceptionToThrow = null;
      boolean oneWay = false;

      public DummySession(PacketDispatcher dispatcher, long timeToReply, Exception toThrow, boolean oneWay)
      {
         this.dispatcher = dispatcher;
         this.timeToReply = timeToReply;
         exceptionToThrow = toThrow;
         this.oneWay = oneWay;
      }

      public Packet getPacketDispatched()
      {
         return packetDispatched;
      }

      public long getID()
      {
         return 0;
      }

      public void write(final Packet packet) throws Exception
      {
         if (exceptionToThrow != null)
         {
            throw exceptionToThrow;
         }
         else if (!oneWay)
         {
            new Thread(new Runnable()
            {
               public void run()
               {
                  try
                  {
                     Thread.sleep(timeToReply);
                  }
                  catch (InterruptedException e)
                  {
                     e.printStackTrace();
                  }
                  packetDispatched = packet;
                  try
                  {
                     dispatcher.dispatch(packet, null);
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
               }
            }).start();
         }
      }

      public boolean isConnected()
      {
         return true;
      }
   }
}
