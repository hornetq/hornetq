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
package org.jboss.messaging.tests.unit.core.server.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.impl.ConnectionManagerImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConnectionManagerImplTest extends UnitTestCase
{
   public void testRegisterSingleConnection()
   {
      ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
      ServerConnection connection = EasyMock.createStrictMock(ServerConnection.class);

      EasyMock.replay(connection);
      connectionManager.registerConnection(0, connection);
      EasyMock.verify(connection);
      assertEquals(connectionManager.size(), 1);
      checkActiveConnections(connectionManager.getActiveConnections(), connection);
   }

   public void testRegisterMultipleConnectionsSameSession()
   {
      ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
      ServerConnection connection = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection2 = EasyMock.createStrictMock(ServerConnection.class);
      EasyMock.replay(connection, connection2);
      connectionManager.registerConnection(0, connection);
      connectionManager.registerConnection(0, connection2);
      EasyMock.verify(connection, connection2 );
      assertEquals(connectionManager.size(), 2);
      checkActiveConnections(connectionManager.getActiveConnections(), connection, connection2);
   }

   public void testRegisterMultipleConnectionsDifferentSession()
   {
      ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
      ServerConnection connection = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection2 = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection3 = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection4 = EasyMock.createStrictMock(ServerConnection.class);
      EasyMock.replay(connection, connection2, connection3, connection4);
      connectionManager.registerConnection(0, connection);
      connectionManager.registerConnection(0, connection2);
      connectionManager.registerConnection(1, connection3);
      connectionManager.registerConnection(1, connection4);
      EasyMock.verify(connection, connection2, connection3, connection4);
      assertEquals(connectionManager.size(), 4);
      checkActiveConnections(connectionManager.getActiveConnections(), connection, connection2, connection3, connection4);
   }

   public void testUnRegisterConnections()
   {
      ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
      ServerConnection connection = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection2 = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection3 = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection4 = EasyMock.createStrictMock(ServerConnection.class);
      EasyMock.replay(connection, connection2, connection3, connection4);
      connectionManager.registerConnection(0, connection);
      connectionManager.registerConnection(0, connection2);
      connectionManager.registerConnection(1, connection3);
      connectionManager.registerConnection(1, connection4);
      checkActiveConnections(connectionManager.getActiveConnections(), connection, connection2, connection3, connection4);
      assertEquals(connectionManager.unregisterConnection(0,connection), connection);
      checkActiveConnections(connectionManager.getActiveConnections(), connection2, connection3, connection4);
      EasyMock.verify(connection, connection2, connection3, connection4);
      assertEquals(connectionManager.size(), 3);
      assertEquals(connectionManager.unregisterConnection(0,connection2), connection2);
      checkActiveConnections(connectionManager.getActiveConnections(),connection3, connection4);
      assertEquals(connectionManager.size(), 2);
   }

   public void testUnRegisterNonExistentConnection()
      {
         ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
         ServerConnection connection = EasyMock.createStrictMock(ServerConnection.class);
         EasyMock.replay(connection);
         assertNull(connectionManager.unregisterConnection(0,connection));
         EasyMock.verify(connection);
         assertEquals(connectionManager.size(), 0);
      }


   public void testSessionClosed() throws Exception
   {
      ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
      ServerConnection connection = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection2 = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection3 = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection4 = EasyMock.createStrictMock(ServerConnection.class);
      connection.close();
      connection2.close();
      EasyMock.replay(connection, connection2, connection3, connection4);
      connectionManager.registerConnection(0, connection);
      connectionManager.registerConnection(0, connection2);
      connectionManager.registerConnection(1, connection3);
      connectionManager.registerConnection(1, connection4);
      checkActiveConnections(connectionManager.getActiveConnections(), connection, connection2, connection3, connection4);
      connectionManager.sessionDestroyed(0, null);
      checkActiveConnections(connectionManager.getActiveConnections(), connection3, connection4);
      EasyMock.verify(connection, connection2, connection3, connection4);
      assertEquals(connectionManager.size(), 2);
   }

    public void testSessionClosedNonExistentConnection() throws Exception
   {
      ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
      connectionManager.sessionDestroyed(0, null);
      assertEquals(connectionManager.size(), 0);
   }

   public void testSessionClosedHandlesCloseException() throws Exception
   {
      ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
      ServerConnection connection = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection2 = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection3 = EasyMock.createStrictMock(ServerConnection.class);
      ServerConnection connection4 = EasyMock.createStrictMock(ServerConnection.class);
      connection.close();
      EasyMock.expectLastCall().andThrow(new RuntimeException());
      connection2.close();
      EasyMock.replay(connection, connection2, connection3, connection4);
      connectionManager.registerConnection(0, connection);
      connectionManager.registerConnection(0, connection2);
      connectionManager.registerConnection(1, connection3);
      connectionManager.registerConnection(1, connection4);
      checkActiveConnections(connectionManager.getActiveConnections(), connection, connection2, connection3, connection4);
      connectionManager.sessionDestroyed(0, null);
      checkActiveConnections(connectionManager.getActiveConnections(), connection3, connection4);
      EasyMock.verify(connection, connection2, connection3, connection4);
      assertEquals(connectionManager.size(), 2);
   }

   private void checkActiveConnections(List<ServerConnection> connectionList, ServerConnection... serverConnections)
   {
      List<ServerConnection> connections = new ArrayList<ServerConnection>(connectionList);
      for (ServerConnection serverConnection : serverConnections)
      {
         assertTrue(connections.remove(serverConnection));
      }
      assertEquals(0, connections.size());
   }
}
