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
package org.jboss.messaging.tests.timing.core.remoting.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class RemotingConnectionImplTest extends UnitTestCase
{
   public void testConnectionSendBlockingWithTimeout() throws Throwable
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
      DummySession nioSession = new DummySession(dispatcher, 2000, null, false);
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
         fail("should have timed out");
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
