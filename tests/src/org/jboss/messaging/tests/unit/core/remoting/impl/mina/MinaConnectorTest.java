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


package org.jboss.messaging.tests.unit.core.remoting.impl.mina;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.apache.mina.common.AbstractIoSession;
import org.apache.mina.common.CloseFuture;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChain;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoFilter;
import org.apache.mina.common.IoFilterChain;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoProcessor;
import org.apache.mina.common.IoService;
import org.apache.mina.common.IoServiceListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.IoSessionConfig;
import org.apache.mina.common.IoSessionDataStructureFactory;
import org.apache.mina.common.TrafficMask;
import org.apache.mina.common.TransportMetadata;
import org.apache.mina.common.WriteFuture;
import org.apache.mina.common.WriteRequest;
import org.apache.mina.common.IoFilter.NextFilter;
import org.apache.mina.filter.ssl.SslFilter;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IArgumentMatcher;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.RemotingSession;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.ResponseHandlerImpl;
import org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaHandler;
import org.jboss.messaging.core.remoting.impl.mina.MinaSession;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.tests.integration.core.remoting.mina.TestSupport;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class MinaConnectorTest extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   
   private LocationImpl location;
   
   private ConnectionParams connectionParams;
   
   private PacketDispatcher dispatcher;
   
   private SocketConnector connector;
   
   private DefaultIoFilterChainBuilder delegateBuilder;
   
   private SocketSessionConfig sessionConfig;
   
   private IoSession ioSession;
   
   private FilterChainSupport filterChainSupport;
   
   private SetHandlerAnswer setHandlerAnswer;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   
   public void setUp() throws Exception
   {
      super.setUp();
      
      location = null;
      
      connectionParams = null;
      
      dispatcher = null;
      
      connector = null;
      
      delegateBuilder = null;
      
      sessionConfig = null;
      
      ioSession = null;
      
      filterChainSupport = null;
      
      setHandlerAnswer = null;

   }
   
   public void testConstructor() throws Exception
   {
      construct(false);
      construct(true);
      
      try
      {
         new MinaConnector(null, connectionParams, connector, filterChainSupport, dispatcher);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException ignored)
      {
      }

      try
      {
         new MinaConnector(location, null, connector, filterChainSupport, dispatcher);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException ignored)
      {
      }

      try
      {
         new MinaConnector(location, connectionParams, null, filterChainSupport, dispatcher);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException ignored)
      {
      }

      try
      {
         new MinaConnector(location, connectionParams, connector, null, dispatcher);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException ignored)
      {
      }

      try
      {
         new MinaConnector(location, connectionParams, connector, filterChainSupport, null);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException ignored)
      {
      }

      try
      {
         new MinaConnector(null, dispatcher);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException ignored)
      {
      }

      try
      {
         new MinaConnector(location, null);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException ignored)
      {
      }
   }
   
   public void testSSLError() throws Exception
   {
      location = new LocationImpl(TransportType.TCP, TestSupport.HOST, TestSupport.PORT);
      
      connectionParams = EasyMock.createMock(ConnectionParams.class);
      
      dispatcher = EasyMock.createMock(PacketDispatcher.class);
      
      connector = EasyMock.createMock(SocketConnector.class);
      
      connector.setSessionDataStructureFactory((IoSessionDataStructureFactory)EasyMock.anyObject());
      
      EasyMock.expect(connectionParams.isSSLEnabled()).andReturn(true);
      
      FilterChainSupport filterChainSupport = EasyMock.createMock(FilterChainSupport.class);
      
      delegateBuilder = new DefaultIoFilterChainBuilder();

      sessionConfig = EasyMock.createMock(SocketSessionConfig.class);
      
      EasyMock.expect(connector.getSessionConfig()).andReturn(sessionConfig);
      
      EasyMock.expect(connector.getFilterChain()).andReturn(delegateBuilder);
      
      String password = RandomUtil.randomString();
 
      String storePath = RandomUtil.randomString();
      
      EasyMock.expect(connectionParams.getKeyStorePassword()).andReturn(password);
      
      EasyMock.expect(connectionParams.getKeyStorePath()).andReturn(storePath);

      filterChainSupport.addSSLFilter(delegateBuilder, true, storePath, password, null, null);
      
      EasyMock.expectLastCall().andThrow(new IllegalStateException("mock exception"));

      EasyMock.replay(connectionParams, dispatcher, connector, filterChainSupport);
      
      
      try
      {
         new MinaConnector(location, connectionParams, connector, filterChainSupport, dispatcher);
         fail("supposed to throw exception");
      }
      catch (IllegalStateException e)
      {
      }
      
      EasyMock.verify(connectionParams, dispatcher, connector, filterChainSupport);
      
   }

   public void testExceptionOnConnect() throws Exception
   {
      MinaConnector minaConnector = construct(false);
      
      connector.setHandler(EasyMock.isA(MinaHandler.class));
      
      dispatcher.setListener(EasyMock.isA(MinaHandler.class));
      
      ConnectFuture futureConnect = EasyMock.createMock(ConnectFuture.class);
      
      InetSocketAddress address = new InetSocketAddress(location.getHost(), location.getPort());
      
      EasyMock.expect(connector.connect(address)).andReturn(futureConnect);
      
      connector.setDefaultRemoteAddress(address);
      
      connector.addListener(EasyMock.isA(IoServiceListener.class));
      
      EasyMock.expect(futureConnect.awaitUninterruptibly()).andReturn(futureConnect);
      
      EasyMock.expect(futureConnect.isConnected()).andReturn(false);

      EasyMock.replay(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport, futureConnect);
      
      try
      {
         minaConnector.connect();
         fail("Suppsoed to throw IOException");
      }
      catch (IOException ignored)
      {
      }
      
      EasyMock.verify(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport, futureConnect);
   }

   
   public void testConnect() throws Exception
   {
      MinaConnector minaConnector = construct(false);
      
      connect(minaConnector);
      
      // A reconnect should return the same cached Session (using a new MinaSession)
      
      EasyMock.expect(ioSession.isConnected()).andStubReturn(true);
      
      EasyMock.replay(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport, ioSession);

      RemotingSession minaSession = minaConnector.connect();
      
      assertEquals(minaSession, new MinaSession(ioSession));
      
      EasyMock.verify(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport, ioSession);
   }

   public void testDisconnect() throws Exception
   {
      MinaConnector minaConnector = construct(true);
      
      assertFalse(minaConnector.disconnect());
      
      connect(minaConnector);
      
      disconnect(minaConnector);
      
   }

   public void testPinger() throws Exception
   {
      MinaConnector minaConnector = construct(true);
      
      connect(minaConnector, true);
      
      Ping ping = new Ping(33);
      
      
      PacketReturner retPack = EasyMock.createStrictMock(PacketReturner.class);
      retPack.send(pongMatch(33, false));
      
      EasyMock.replay(retPack);
      
      this.setHandlerAnswer.handler.handle(ping, retPack);

      EasyMock.verify(retPack);
      
      disconnect(minaConnector);
      
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private Pong pongMatch(final int sessionId, final boolean sessionFailed)
   {
      EasyMock.reportMatcher(new IArgumentMatcher()
      {

         public void appendTo(StringBuffer buffer)
         {
         }

         public boolean matches(Object argument)
         {
            Pong pong = (Pong) argument;
            
            return pong.getSessionID() == sessionId &&
                   pong.isSessionFailed() == sessionFailed;
         }
         
      });
      
      return null;
   }
      
   private void connect(MinaConnector minaConnector) throws IOException
   {
      connect(minaConnector, false);
   }
   
   private void connect(final MinaConnector minaConnector, final boolean ping) throws IOException
   {
      connector.setHandler(EasyMock.isA(MinaHandler.class));
      
      dispatcher.setListener(EasyMock.isA(MinaHandler.class));
      
      ConnectFuture futureConnect = EasyMock.createMock(ConnectFuture.class);
      
      InetSocketAddress address = new InetSocketAddress(location.getHost(), location.getPort());
      
      EasyMock.expect(connector.connect(address)).andReturn(futureConnect);
      
      connector.setDefaultRemoteAddress(address);
      
      connector.addListener(EasyMock.isA(IoServiceListener.class));
      
      EasyMock.expect(futureConnect.awaitUninterruptibly()).andReturn(futureConnect);
      
      EasyMock.expect(futureConnect.isConnected()).andReturn(true);
      
      ioSession = EasyMock.createMock(IoSession.class);
      
      EasyMock.expect(futureConnect.getSession()).andReturn(ioSession);
      
      dispatcher.register(EasyMock.isA(PacketHandler.class));
      
      this.setHandlerAnswer = new SetHandlerAnswer();
      
      EasyMock.expectLastCall().andAnswer(setHandlerAnswer);
      
      if (ping)
      {
         EasyMock.expect(connectionParams.getPingInterval()).andReturn(60000l);
         EasyMock.expectLastCall().atLeastOnce();
         EasyMock.expect(dispatcher.generateID()).andReturn(30l);
         EasyMock.expect(connectionParams.getPingTimeout()).andReturn(60000l);
         dispatcher.register(EasyMock.isA(ResponseHandlerImpl.class));
      }
      else
      {
         EasyMock.expect(connectionParams.getPingInterval()).andReturn(0l);
      }
      
      EasyMock.replay(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport, futureConnect, ioSession);
      
      minaConnector.connect();
      
      EasyMock.verify(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport, futureConnect, ioSession);
      
      EasyMock.reset(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport, futureConnect, ioSession);
   }
   
   private void disconnect(MinaConnector minaConnector) throws Exception,
         NoSuchAlgorithmException
   {
      connector.removeListener(EasyMock.isA(IoServiceListener.class));
      
      CloseFuture futureClose = EasyMock.createMock(CloseFuture.class);
      
      EasyMock.expect(ioSession.close()).andReturn(futureClose);
      
      EasyMock.expect(futureClose.awaitUninterruptibly())
            .andReturn(futureClose);
      
      EasyMock.expect(futureClose.isClosed()).andReturn(true);
      
      connector.dispose();
      
      IoFilterChain easyChain = EasyMock.createNiceMock(IoFilterChain.class);
      
      WriteFuture futureSSLStop = EasyMock.createStrictMock(WriteFuture.class);
      EasyMock.expect(futureSSLStop.awaitUninterruptibly()).andReturn(
            futureSSLStop);
      
      EasyMock.expect(easyChain.get("ssl")).andReturn(
            new ProxySSLFilter(futureSSLStop));
      
      ProxyFilterChain chain = new ProxyFilterChain(new IoSessionProxy(
            easyChain), easyChain);
      
      new SslFilter(SSLContext.getInstance("TLS"));
      
      EasyMock.expect(ioSession.getFilterChain()).andReturn(chain);
      
      // This is a hack currently being done on disconnect for SSL, maybe it
      // will start to fail when MINA is fixed
      
      EasyMock.replay(connectionParams, dispatcher, connector, sessionConfig,
            filterChainSupport, futureClose, futureSSLStop, ioSession,
            easyChain);
      
      minaConnector.disconnect();
      
      EasyMock.verify(connectionParams, dispatcher, connector, sessionConfig,
            filterChainSupport, futureClose, futureSSLStop, ioSession,
            easyChain);
      
      EasyMock.reset(connectionParams, dispatcher, connector, sessionConfig,
            filterChainSupport, futureClose, futureSSLStop, ioSession,
            easyChain);
   }


   
   private MinaConnector construct(boolean ssl) throws Exception
   {
      location = new LocationImpl(TransportType.TCP, TestSupport.HOST, TestSupport.PORT);
      
      connectionParams = EasyMock.createMock(ConnectionParams.class);
      
      dispatcher = EasyMock.createMock(PacketDispatcher.class);
      
      connector = EasyMock.createMock(SocketConnector.class);
      
      connector.setSessionDataStructureFactory((IoSessionDataStructureFactory)EasyMock.anyObject());
      
      EasyMock.expect(connectionParams.isSSLEnabled()).andReturn(ssl);
      
      filterChainSupport = EasyMock.createMock(FilterChainSupport.class);
      
      delegateBuilder = new DefaultIoFilterChainBuilder();

      filterChainSupport.addCodecFilter(delegateBuilder);
      
      sessionConfig = EasyMock.createMock(SocketSessionConfig.class);
      
      EasyMock.expect(connector.getSessionConfig()).andReturn(sessionConfig);
      
      EasyMock.expect(connector.getFilterChain()).andReturn(delegateBuilder);
      
      if (ssl)
      {
         String password = RandomUtil.randomString();
         String storePath = RandomUtil.randomString();
         EasyMock.expect(connectionParams.getKeyStorePassword()).andReturn(password);
         EasyMock.expect(connectionParams.getKeyStorePath()).andReturn(storePath);

         filterChainSupport.addSSLFilter(delegateBuilder, true, storePath, password, null, null);
      }
      
      EasyMock.expect(connectionParams.isTcpNoDelay()).andReturn(true);
      
      sessionConfig.setTcpNoDelay(true);

      EasyMock.expect(connectionParams.getTcpReceiveBufferSize()).andReturn(1024);
      
      sessionConfig.setReceiveBufferSize(1024);
      
      EasyMock.expect(connectionParams.getTcpSendBufferSize()).andReturn(1024);
      
      sessionConfig.setSendBufferSize(1024);
      
      sessionConfig.setKeepAlive(true);
      
      sessionConfig.setReuseAddress(true);
      
      EasyMock.replay(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport);
      
      MinaConnector mina = new MinaConnector(location, connectionParams, connector, filterChainSupport, dispatcher);
      
      EasyMock.verify(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport);
      
      EasyMock.reset(connectionParams, dispatcher, connector, sessionConfig, filterChainSupport);
      
      return mina;
   }
   
   
   // Inner classes -------------------------------------------------
   

   class SetHandlerAnswer implements IAnswer<Object>
   {
      PacketHandler handler;

      public Object answer() throws Throwable
      {
         handler = (PacketHandler)EasyMock.getCurrentArguments()[0];
         System.out.println("handler = " + handler);
         return null;
      }
   }
   
   
   /** Mina is not 100% "interfacable", so I'm mocking this class */
  class ProxySSLFilter extends SslFilter
   {
      
      private final WriteFuture futureStop;
      
      public ProxySSLFilter(WriteFuture futureStop) throws Exception
      {
         super(SSLContext.getInstance("TLS"));
         this.futureStop = futureStop;
      }
      
      public WriteFuture stopSsl(IoSession session) throws SSLException
      {
         return futureStop;
      }
      
   }
   
   
   /** Mina is not 100% "interfacable", so I'm mocking this class */
   class ProxyFilterChain extends DefaultIoFilterChain
   {

      private final IoFilterChain delegateMock;
      
      public ProxyFilterChain(AbstractIoSession session, IoFilterChain delegateMock)
      {
         super(session);
         this.delegateMock = delegateMock;
      }

      public void addAfter(String baseName, String name, IoFilter filter)
      {
         delegateMock.addAfter(baseName, name, filter);
      }

      public void addBefore(String baseName, String name, IoFilter filter)
      {
         delegateMock.addBefore(baseName, name, filter);
      }

      public void addFirst(String name, IoFilter filter)
      {
         delegateMock.addFirst(name, filter);
      }

      public void addLast(String name, IoFilter filter)
      {
         delegateMock.addLast(name, filter);
      }

      public void clear() throws Exception
      {
         delegateMock.clear();
      }

      public boolean contains(Class<? extends IoFilter> filterType)
      {
         return delegateMock.contains(filterType);
      }

      public boolean contains(IoFilter filter)
      {
         return delegateMock.contains(filter);
      }

      public boolean contains(String name)
      {
         return delegateMock.contains(name);
      }

      public void fireExceptionCaught(Throwable cause)
      {
         delegateMock.fireExceptionCaught(cause);
      }

      public void fireFilterClose()
      {
         delegateMock.fireFilterClose();
      }

      public void fireFilterSetTrafficMask(TrafficMask trafficMask)
      {
         delegateMock.fireFilterSetTrafficMask(trafficMask);
      }

      public void fireFilterWrite(WriteRequest writeRequest)
      {
         delegateMock.fireFilterWrite(writeRequest);
      }

      public void fireMessageReceived(Object message)
      {
         delegateMock.fireMessageReceived(message);
      }

      public void fireMessageSent(WriteRequest request)
      {
         delegateMock.fireMessageSent(request);
      }

      public void fireSessionClosed()
      {
         delegateMock.fireSessionClosed();
      }

      public void fireSessionCreated()
      {
         delegateMock.fireSessionCreated();
      }

      public void fireSessionIdle(IdleStatus status)
      {
         delegateMock.fireSessionIdle(status);
      }

      public void fireSessionOpened()
      {
         delegateMock.fireSessionOpened();
      }

      public IoFilter get(Class<? extends IoFilter> filterType)
      {
         return delegateMock.get(filterType);
      }

      public IoFilter get(String name)
      {
         return delegateMock.get(name);
      }

      public List<Entry> getAll()
      {
         return delegateMock.getAll();
      }

      public List<Entry> getAllReversed()
      {
         return delegateMock.getAllReversed();
      }

      public Entry getEntry(Class<? extends IoFilter> filterType)
      {
         return delegateMock.getEntry(filterType);
      }

      public Entry getEntry(IoFilter filter)
      {
         return delegateMock.getEntry(filter);
      }

      public Entry getEntry(String name)
      {
         return delegateMock.getEntry(name);
      }

      public NextFilter getNextFilter(Class<? extends IoFilter> filterType)
      {
         return delegateMock.getNextFilter(filterType);
      }

      public NextFilter getNextFilter(IoFilter filter)
      {
         return delegateMock.getNextFilter(filter);
      }

      public NextFilter getNextFilter(String name)
      {
         return delegateMock.getNextFilter(name);
      }

      public IoSession getSession()
      {
         return delegateMock.getSession();
      }

      public IoFilter remove(Class<? extends IoFilter> filterType)
      {
         return delegateMock.remove(filterType);
      }

      public void remove(IoFilter filter)
      {
         delegateMock.remove(filter);
      }

      public IoFilter remove(String name)
      {
         return delegateMock.remove(name);
      }

      public IoFilter replace(Class<? extends IoFilter> oldFilterType,
            IoFilter newFilter)
      {
         return delegateMock.replace(oldFilterType, newFilter);
      }

      public void replace(IoFilter oldFilter, IoFilter newFilter)
      {
         delegateMock.replace(oldFilter, newFilter);
      }

      public IoFilter replace(String name, IoFilter newFilter)
      {
         return delegateMock.replace(name, newFilter);
      }

   }
   
   
   /** Mina is not 100% "interfacable", so I'm mocking this class */
   class IoSessionProxy extends AbstractIoSession
   {

      private final IoFilterChain delegateMock;
      
      public IoSessionProxy(IoFilterChain delegateMock)
      {
         this.delegateMock = delegateMock;
      }
      
      protected IoProcessor getProcessor()
      {
         return null;
      }

      public IoSessionConfig getConfig()
      {
         return null;
      }

      public IoFilterChain getFilterChain()
      {
         return new ProxyFilterChain(this, delegateMock);
      }

      public IoHandler getHandler()
      {
         return null;
      }

      public SocketAddress getLocalAddress()
      {
         return null;
      }

      public SocketAddress getRemoteAddress()
      {
         return null;
      }

      public IoService getService()
      {
         return null;
      }

      public TransportMetadata getTransportMetadata()
      {
         return null;
      }
      
   }
   
}
