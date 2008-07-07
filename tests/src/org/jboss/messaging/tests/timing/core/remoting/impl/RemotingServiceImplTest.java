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

import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingSession;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class RemotingServiceImplTest extends UnitTestCase
{
    public void testPingerAddedAndCalled()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      config.getConnectionParams().setPingInterval(100);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      DummySession dummySession = new DummySession(remotingService.getDispatcher());
      remotingService.registerPinger(dummySession);
      try
      {
         Thread.sleep(1100);
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();
      }
      assertTrue(remotingService.isSession(1l));
      remotingService.unregisterPinger(1l);
      assertTrue(dummySession.count > 10);
   }

   public void testPingerAddedAndRemoved()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      config.getConnectionParams().setPingInterval(100);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      DummySession dummySession = new DummySession(remotingService.getDispatcher());
      remotingService.registerPinger(dummySession);
      try
      {
         Thread.sleep(1100);
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();
      }
      remotingService.unregisterPinger(1l);
      int count = dummySession.count;
      try
      {
         Thread.sleep(config.getConnectionParams().getPingInterval() + 2);
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();
      }
      assertEquals(count, dummySession.count);
   }

   class DummySession implements RemotingSession
   {
      PacketDispatcher dispatcher;
      int count = 0;

      public DummySession(PacketDispatcher dispatcher)
      {
         this.dispatcher = dispatcher;
      }

      public long getID()
      {
         return 1;
      }

      public void write(Packet packet) throws Exception
      {
         count++;
         Ping ping = (Ping) packet;

         Pong pong = new Pong(ping.getSessionID(), false);
         pong.setTargetID(1);
         dispatcher.dispatch(pong, null);

      }

      public boolean isConnected()
      {
         return true;
      }
   }
}
