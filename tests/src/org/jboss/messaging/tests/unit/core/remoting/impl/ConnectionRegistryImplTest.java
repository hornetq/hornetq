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
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.ConnectionRegistryImpl;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A ConnectionRegistryImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConnectionRegistryImplTest extends UnitTestCase
{
   public static final Logger log = Logger.getLogger(ConnectionRegistryImplTest.class);
   
   class FakeConnectorFactory implements ConnectorFactory
   {
      FakeConnectorFactory(final Connector connector)
      {
         this.connector = connector;
      }
      
      private final Connector connector;

      public Connector createConnector(final Location location,
            final ConnectionParams params, final RemotingHandler handler,
            final ConnectionLifeCycleListener listener)
      {
         return connector;
      }
      
   }
   
   public void testGetAndReturnConnection()
   {
      ConnectionRegistry registry = new ConnectionRegistryImpl();
      
      //First one 
      
      Connector connector1 = EasyMock.createStrictMock(Connector.class);
      
      registry.registerConnectorFactory(TransportType.TCP, new FakeConnectorFactory(connector1));
      
      connector1.start();
      
      Connection tc1 = EasyMock.createStrictMock(Connection.class);
      
      final long id1 = 102912;
      
      EasyMock.expect(tc1.getID()).andReturn(id1);
      
      EasyMock.expect(connector1.createConnection()).andReturn(tc1);
      
      // Second one
      
      Connector connector2 = EasyMock.createStrictMock(Connector.class);
      
      registry.registerConnectorFactory(TransportType.HTTP, new FakeConnectorFactory(connector2));
      
      connector2.start();
      
      Connection tc2 = EasyMock.createStrictMock(Connection.class);
      
      final long id2 = 81273;
      
      EasyMock.expect(tc2.getID()).andReturn(id2);
      
      EasyMock.expect(connector2.createConnection()).andReturn(tc2);
      
      
      
      EasyMock.replay(connector1, connector2, tc1, tc2);
      
      Location location1 = new LocationImpl(TransportType.TCP, "blahhost", 12345);
      
      ConnectionParams params = new ConnectionParamsImpl();
      
      assertEquals(0, registry.size());
      
      RemotingConnection conn1 = registry.getConnection(location1, params);
      
      assertEquals(1, registry.size());
      
      RemotingConnection conn2 = registry.getConnection(location1, params);
      
      assertEquals(1, registry.size());
      
      RemotingConnection conn3 = registry.getConnection(location1, params);
      
      assertEquals(1, registry.size());
      
      assertTrue(conn1 == conn2);
      
      assertTrue(conn2 == conn3);
      
      
      Location location2 = new LocationImpl(TransportType.HTTP, "blahhost2", 65125);
      
      RemotingConnection conn4 = registry.getConnection(location2, params);
      
      assertEquals(2, registry.size());

      RemotingConnection conn5 = registry.getConnection(location2, params);
      
      assertEquals(2, registry.size());
      
      RemotingConnection conn6 = registry.getConnection(location2, params);
      
      assertEquals(2, registry.size());
      
      assertTrue(conn4 == conn5);
      
      assertTrue(conn5 == conn6);
      
      assertFalse(conn1 == conn4);
      
      EasyMock.verify(connector1, connector2, tc1, tc2);
      
      EasyMock.reset(connector1, connector2, tc1, tc2);
      
      EasyMock.replay(connector1, connector2, tc1, tc2);
      
      registry.returnConnection(location2);
      
      assertEquals(2, registry.size());
      
      registry.returnConnection(location2);
      
      assertEquals(2, registry.size());
      
      RemotingConnection conn7 = registry.getConnection(location2, params);
      
      assertTrue(conn7 == conn4);
      
      EasyMock.verify(connector1, connector2, tc1, tc2);
      
      EasyMock.reset(connector1, connector2, tc1, tc2);
      
      EasyMock.expect(tc2.getID()).andReturn(id2);
      
      tc2.close();
          
      connector2.close();
      
      connector2.start();
      
      EasyMock.expect(tc2.getID()).andReturn(id2);
      
      EasyMock.expect(connector2.createConnection()).andReturn(tc2);
      
      EasyMock.replay(connector1, connector2, tc1, tc2);
      
      registry.returnConnection(location2);
      
      assertEquals(2, registry.size());
      
      registry.returnConnection(location2);
      
      assertEquals(1, registry.size());
      
      RemotingConnection conn8 = registry.getConnection(location2, params);
      
      assertEquals(2, registry.size());
      
      assertFalse(conn8 == conn4);
      
      EasyMock.verify(connector1, connector2, tc1, tc2);
      
      EasyMock.reset(connector1, connector2, tc1, tc2);
      
      EasyMock.expect(tc1.getID()).andReturn(id1);
      
      tc1.close();
          
      connector1.close();
      
      EasyMock.replay(connector1, connector2, tc1, tc2);
      
      registry.returnConnection(location1);
      
      registry.returnConnection(location1);
      
      registry.returnConnection(location1);
      
      assertEquals(1, registry.size());
      
      EasyMock.verify(connector1, connector2, tc1, tc2);
      
   }
   
   public void testConnectionDestroyed()
   {
      ConnectionRegistryImpl registry = new ConnectionRegistryImpl();
      
      Connector connector1 = EasyMock.createStrictMock(Connector.class);
      
      registry.registerConnectorFactory(TransportType.TCP, new FakeConnectorFactory(connector1));
      
      connector1.start();
      
      Connection tc1 = EasyMock.createStrictMock(Connection.class);
      
      final long id1 = 102912;
      
      EasyMock.expect(tc1.getID()).andStubReturn(id1);
      
      EasyMock.expect(connector1.createConnection()).andReturn(tc1);
      
    
      EasyMock.replay(connector1, tc1);
      
      Location location1 = new LocationImpl(TransportType.TCP, "blahhost", 12345);
      
      ConnectionParams params = new ConnectionParamsImpl();
      
      RemotingConnection conn1 = registry.getConnection(location1, params);
      
      Listener listener = new Listener();
      
      conn1.addFailureListener(listener);
      
      assertEquals(id1, conn1.getID());
                
      EasyMock.verify(connector1, tc1);
      
      assertEquals(1, registry.size());
      
      EasyMock.reset(connector1, tc1);
      
      tc1.close();
      
      connector1.close();
      
      EasyMock.replay(connector1, tc1);
            
      registry.connectionDestroyed(id1);
      
      EasyMock.verify(connector1, tc1);
      
      assertEquals(0, registry.size());
      
      assertNotNull(listener.me);
      
      assertEquals(MessagingException.OBJECT_CLOSED, listener.me.getCode());
            
   }
   
   public void testConnectionException()
   {
      ConnectionRegistryImpl registry = new ConnectionRegistryImpl();
      
      Connector connector1 = EasyMock.createStrictMock(Connector.class);
      
      registry.registerConnectorFactory(TransportType.TCP, new FakeConnectorFactory(connector1));
      
      connector1.start();
      
      Connection tc1 = EasyMock.createStrictMock(Connection.class);
      
      final long id1 = 102912;
      
      EasyMock.expect(tc1.getID()).andStubReturn(id1);
      
      EasyMock.expect(connector1.createConnection()).andReturn(tc1);
      
    
      EasyMock.replay(connector1, tc1);
      
      Location location1 = new LocationImpl(TransportType.TCP, "blahhost", 12345);
      
      ConnectionParams params = new ConnectionParamsImpl();
      
      RemotingConnection conn1 = registry.getConnection(location1, params);
      
      Listener listener = new Listener();
      
      conn1.addFailureListener(listener);
      
      assertEquals(id1, conn1.getID());
                
      EasyMock.verify(connector1, tc1);
      
      assertEquals(1, registry.size());
      
      EasyMock.reset(connector1, tc1);
      
      tc1.close();
      
      connector1.close();
      
      EasyMock.replay(connector1, tc1);
      
      MessagingException me = new MessagingException(1212, "blah123");
            
      registry.connectionException(id1, me);
      
      EasyMock.verify(connector1, tc1);
      
      assertEquals(0, registry.size());
      
      assertTrue(me == listener.me);
            
   }
   
   class Listener implements FailureListener
   {
      volatile MessagingException me;

      public void connectionFailed(MessagingException me)
      {
         this.me = me;
      }
      
   }
}
