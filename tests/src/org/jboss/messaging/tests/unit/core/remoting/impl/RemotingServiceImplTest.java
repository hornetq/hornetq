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
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class RemotingServiceImplTest extends UnitTestCase
{
   public void testSingleAcceptorStarted() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.setAcceptorFactory(new AcceptorFactory()
      {
         public List<Acceptor> createAcceptors(Configuration configuration)
         {
            List<Acceptor> acceptors = new ArrayList<Acceptor>();
            acceptors.add(acceptor);
            return acceptors;
         }
      });
      acceptor.startAccepting(remotingService, remotingService);
      EasyMock.replay(acceptor);
      remotingService.start();
      EasyMock.verify(acceptor);
      assertEquals(1, remotingService.getAcceptors().size());
      assertEquals(config, remotingService.getConfiguration());
   }

   public void testSingleAcceptorStartedTwiceIsIgnored() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.setAcceptorFactory(new AcceptorFactory()
      {
         public List<Acceptor> createAcceptors(Configuration configuration)
         {
            List<Acceptor> acceptors = new ArrayList<Acceptor>();
            acceptors.add(acceptor);
            return acceptors;
         }
      });
      acceptor.startAccepting(remotingService, remotingService);
      EasyMock.replay(acceptor);
      remotingService.start();
      remotingService.start();
      EasyMock.verify(acceptor);
      assertEquals(1, remotingService.getAcceptors().size());
      assertEquals(config, remotingService.getConfiguration());
   }

   public void testSingleAcceptorStartedAndStopped() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.setAcceptorFactory(new AcceptorFactory()
      {
         public List<Acceptor> createAcceptors(Configuration configuration)
         {
            List<Acceptor> acceptors = new ArrayList<Acceptor>();
            acceptors.add(acceptor);
            return acceptors;
         }
      });
      acceptor.startAccepting(remotingService, remotingService);
      acceptor.stopAccepting();
      EasyMock.replay(acceptor);
      remotingService.start();
      remotingService.stop();
      EasyMock.verify(acceptor);
      assertEquals(1, remotingService.getAcceptors().size());
      assertEquals(config, remotingService.getConfiguration());
   }

   public void testMultipleAcceptorsStarted() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor2 = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor3 = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.setAcceptorFactory(new AcceptorFactory()
      {
         public List<Acceptor> createAcceptors(Configuration configuration)
         {
            List<Acceptor> acceptors = new ArrayList<Acceptor>();
            acceptors.add(acceptor);
            acceptors.add(acceptor2);
            acceptors.add(acceptor3);
            return acceptors;
         }
      });
      acceptor.startAccepting(remotingService, remotingService);
      acceptor2.startAccepting(remotingService, remotingService);
      acceptor3.startAccepting(remotingService, remotingService);
      EasyMock.replay(acceptor, acceptor2, acceptor3);
      remotingService.start();
      EasyMock.verify(acceptor, acceptor2, acceptor3);
      assertEquals(3, remotingService.getAcceptors().size());
      assertEquals(config, remotingService.getConfiguration());
   }

   public void testMultipleAcceptorsStartedAndStopped() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor2 = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor3 = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.setAcceptorFactory(new AcceptorFactory()
      {
         public List<Acceptor> createAcceptors(Configuration configuration)
         {
            List<Acceptor> acceptors = new ArrayList<Acceptor>();
            acceptors.add(acceptor);
            acceptors.add(acceptor2);
            acceptors.add(acceptor3);
            return acceptors;
         }
      });
      acceptor.startAccepting(remotingService, remotingService);
      acceptor.stopAccepting();
      acceptor2.startAccepting(remotingService, remotingService);
      acceptor2.stopAccepting();
      acceptor3.startAccepting(remotingService, remotingService);
      acceptor3.stopAccepting();
      EasyMock.replay(acceptor, acceptor2, acceptor3);
      remotingService.start();
      remotingService.stop();
      EasyMock.verify(acceptor, acceptor2, acceptor3);
      assertEquals(3, remotingService.getAcceptors().size());
      assertEquals(config, remotingService.getConfiguration());
   }

   public void testDispatcherNotNull() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());
      remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());
   }

   public void testInterceptorsAddedToDispatcher() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());
      remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());

      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createNiceMock(Packet.class);
      interceptor.intercept(packet);
      EasyMock.replay(interceptor);
      remotingService.addInterceptor(interceptor);
      remotingService.getDispatcher().callFilters(packet);
      EasyMock.verify(interceptor);
   }

   public void testMultipleInterceptorsAddedToDispatcher() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());
      remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());

      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor2 = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor3 = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createNiceMock(Packet.class);
      interceptor.intercept(packet);
      interceptor2.intercept(packet);
      interceptor3.intercept(packet);
      EasyMock.replay(interceptor, interceptor2, interceptor3);
      remotingService.addInterceptor(interceptor);
      remotingService.addInterceptor(interceptor2);
      remotingService.addInterceptor(interceptor3);
      remotingService.getDispatcher().callFilters(packet);
      EasyMock.verify(interceptor, interceptor2, interceptor3);
   }

   public void testInterceptorsAddedToAndRemovedFromDispatcher() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());
      remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());

      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createNiceMock(Packet.class);
      EasyMock.replay(interceptor);
      remotingService.addInterceptor(interceptor);
      remotingService.removeInterceptor(interceptor);
      remotingService.getDispatcher().callFilters(packet);
      EasyMock.verify(interceptor);
   }

   public void testMultipleInterceptorsAddedToAndRemovedFromDispatcher() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());
      remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());

      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor2 = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor3 = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createNiceMock(Packet.class);
      interceptor3.intercept(packet);
      EasyMock.replay(interceptor, interceptor2, interceptor3);
      remotingService.addInterceptor(interceptor);
      remotingService.addInterceptor(interceptor2);
      remotingService.addInterceptor(interceptor3);
      remotingService.removeInterceptor(interceptor);
      remotingService.removeInterceptor(interceptor2);
      remotingService.getDispatcher().callFilters(packet);
      EasyMock.verify(interceptor, interceptor2, interceptor3);
   }

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

   public void testListenerAdded()
   {
      RemotingSessionListener listener = EasyMock.createStrictMock(RemotingSessionListener.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      config.getConnectionParams().setPingInterval(100);

      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.getSessions().add(1l);
      MessagingException me = new MessagingException();
      listener.sessionDestroyed(1l, me);
      EasyMock.replay(listener);
      remotingService.addRemotingSessionListener(listener);
      remotingService.fireCleanup(1l, me);
      EasyMock.verify(listener);
   }

   public void testMultipleListenerAdded()
   {
      RemotingSessionListener listener = EasyMock.createStrictMock(RemotingSessionListener.class);
      RemotingSessionListener listener2 = EasyMock.createStrictMock(RemotingSessionListener.class);
      RemotingSessionListener listener3 = EasyMock.createStrictMock(RemotingSessionListener.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      config.getConnectionParams().setPingInterval(100);

      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.getSessions().add(1l);
      remotingService.getSessions().add(2l);
      remotingService.getSessions().add(3l);
      MessagingException me = new MessagingException();
      listener.sessionDestroyed(1l, me);
      listener2.sessionDestroyed(1l, me);
      listener3.sessionDestroyed(1l, me);
      EasyMock.replay(listener, listener2, listener3);
      remotingService.addRemotingSessionListener(listener);
      remotingService.addRemotingSessionListener(listener2);
      remotingService.addRemotingSessionListener(listener3);
      remotingService.fireCleanup(1l, me);
      EasyMock.verify(listener, listener2, listener3);
   }

   public void testListenerAddedAndRemoved()
   {
      RemotingSessionListener listener = EasyMock.createStrictMock(RemotingSessionListener.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      config.getConnectionParams().setPingInterval(100);

      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.getSessions().add(1l);
      MessagingException me = new MessagingException();
      EasyMock.replay(listener);
      remotingService.addRemotingSessionListener(listener);
      remotingService.removeRemotingSessionListener(listener);
      remotingService.fireCleanup(1l, me);
      EasyMock.verify(listener);
   }

   public void testMultipleListenerAddedAndRemoved()
   {
      RemotingSessionListener listener = EasyMock.createStrictMock(RemotingSessionListener.class);
      RemotingSessionListener listener2 = EasyMock.createStrictMock(RemotingSessionListener.class);
      RemotingSessionListener listener3 = EasyMock.createStrictMock(RemotingSessionListener.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      config.getConnectionParams().setPingInterval(100);

      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.getSessions().add(1l);
      remotingService.getSessions().add(1l);
      remotingService.getSessions().add(1l);
      MessagingException me = new MessagingException();
      listener3.sessionDestroyed(1l, me);
      EasyMock.replay(listener, listener2, listener3);
      remotingService.addRemotingSessionListener(listener);
      remotingService.addRemotingSessionListener(listener2);
      remotingService.addRemotingSessionListener(listener3);
      remotingService.removeRemotingSessionListener(listener);
      remotingService.removeRemotingSessionListener(listener2);
      remotingService.fireCleanup(1l, me);
      EasyMock.verify(listener, listener2, listener3);
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
