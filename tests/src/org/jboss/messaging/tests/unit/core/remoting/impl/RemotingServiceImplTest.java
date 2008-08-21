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

import java.util.List;

import org.easymock.EasyMock;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.AcceptorFactory;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class RemotingServiceImplTest extends UnitTestCase
{
   public void testSingleAcceptorStarted() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor;
         }
      });
      acceptor.start();
      EasyMock.replay(acceptor);
      remotingService.start();
      EasyMock.verify(acceptor);
      assertEquals(1, remotingService.getAcceptors().size());      
      assertTrue(remotingService.getAcceptors().contains(acceptor));   
      assertTrue(remotingService.isStarted());
   }

   public void testSingleAcceptorStartedTwiceIsIgnored() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor;
         }
      });
      acceptor.start();
      EasyMock.replay(acceptor);
      remotingService.start();
      remotingService.start();
      EasyMock.verify(acceptor);
      assertEquals(1, remotingService.getAcceptors().size());      
      assertTrue(remotingService.getAcceptors().contains(acceptor));
      assertTrue(remotingService.isStarted());
   }

   public void testSingleAcceptorStartedAndStopped() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor;
         }
      });
      acceptor.start();
      acceptor.stop();
      EasyMock.replay(acceptor);
      remotingService.start();
      remotingService.stop();
      EasyMock.verify(acceptor);
      assertEquals(1, remotingService.getAcceptors().size());      
      assertTrue(remotingService.getAcceptors().contains(acceptor)); 
      assertFalse(remotingService.isStarted());
   }

   public void testMultipleAcceptorsStarted() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor2 = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor3 = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor;
         }
      });
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor2;
         }
      });
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor3;
         }
      });
      acceptor.start();
      acceptor2.start();
      acceptor3.start();
      EasyMock.replay(acceptor, acceptor2, acceptor3);
      remotingService.start();
      EasyMock.verify(acceptor, acceptor2, acceptor3);
      assertEquals(3, remotingService.getAcceptors().size());   
      assertTrue(remotingService.getAcceptors().contains(acceptor));
      assertTrue(remotingService.getAcceptors().contains(acceptor2));
      assertTrue(remotingService.getAcceptors().contains(acceptor3));
      assertTrue(remotingService.isStarted());
   }

   public void testMultipleAcceptorsStartedAndStopped() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor2 = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor3 = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor;
         }
      });
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor2;
         }
      });
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor3;
         }
      });
      acceptor.start();
      acceptor2.start();
      acceptor3.start();
      acceptor.stop();
      acceptor2.stop();
      acceptor3.stop();
      EasyMock.replay(acceptor, acceptor2, acceptor3);
      remotingService.start();
      remotingService.stop();
      EasyMock.verify(acceptor, acceptor2, acceptor3);
      assertEquals(3, remotingService.getAcceptors().size());   
      assertTrue(remotingService.getAcceptors().contains(acceptor));
      assertTrue(remotingService.getAcceptors().contains(acceptor2));
      assertTrue(remotingService.getAcceptors().contains(acceptor3));
      assertFalse(remotingService.isStarted());
   }
   
   public void testMultipleAcceptorsStartedAndStoppedAddedAnotherAcceptorThenStarted() throws Exception
   {
      final Acceptor acceptor = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor2 = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor3 = EasyMock.createStrictMock(Acceptor.class);
      final Acceptor acceptor4 = EasyMock.createStrictMock(Acceptor.class);
      ConfigurationImpl config = new ConfigurationImpl();
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor;
         }
      });
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor2;
         }
      });
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor3;
         }
      });
      acceptor.start();
      acceptor2.start();
      acceptor3.start();
      acceptor.stop();
      acceptor2.stop();
      acceptor3.stop();
      EasyMock.replay(acceptor, acceptor2, acceptor3);
      remotingService.start();
      remotingService.stop();
      
      EasyMock.verify(acceptor, acceptor2, acceptor3);
      assertEquals(3, remotingService.getAcceptors().size());   
      assertTrue(remotingService.getAcceptors().contains(acceptor));
      assertTrue(remotingService.getAcceptors().contains(acceptor2));
      assertTrue(remotingService.getAcceptors().contains(acceptor3));
      assertFalse(remotingService.isStarted());
      
      EasyMock.reset(acceptor, acceptor2, acceptor3);
      remotingService.registerAcceptorFactory(new AcceptorFactory()
      {
         public Acceptor createAcceptor(Configuration configuration,
               RemotingHandler handler,                         
               ConnectionLifeCycleListener listener)
         {
            return acceptor4;
         }
      });
      acceptor.start();
      acceptor2.start();
      acceptor3.start();
      acceptor4.start();
      acceptor.stop();
      acceptor2.stop();
      acceptor3.stop();
      acceptor4.stop();
      EasyMock.replay(acceptor, acceptor2, acceptor3, acceptor4);
      remotingService.start();
      remotingService.stop();

      EasyMock.verify(acceptor, acceptor2, acceptor3, acceptor4);
      assertEquals(4, remotingService.getAcceptors().size());   
      assertTrue(remotingService.getAcceptors().contains(acceptor));
      assertTrue(remotingService.getAcceptors().contains(acceptor2));
      assertTrue(remotingService.getAcceptors().contains(acceptor3));
      assertTrue(remotingService.getAcceptors().contains(acceptor4));
      assertFalse(remotingService.isStarted());
   }
   
   public void testDispatcherNotNull() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());
      remotingService = new RemotingServiceImpl(config);
      assertNotNull(remotingService.getDispatcher());
   }

   public void testInterceptorsAddedToDispatcher() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();

      config.getInterceptorClassNames().add("org.jboss.messaging.tests.unit.core.remoting.impl.Interceptor1");
      config.getInterceptorClassNames().add("org.jboss.messaging.tests.unit.core.remoting.impl.Interceptor2");
      config.getInterceptorClassNames().add("org.jboss.messaging.tests.unit.core.remoting.impl.Interceptor3");
      
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);     
      assertNotNull(remotingService.getDispatcher());

      List<Interceptor> interceptors = remotingService.getDispatcher().getInterceptors();
      assertEquals(3, interceptors.size());
      assertTrue(interceptors.get(0) instanceof Interceptor1);
      assertTrue(interceptors.get(1) instanceof Interceptor2);
      assertTrue(interceptors.get(2) instanceof Interceptor3);
   }
   
   public void testAcceptorFactoriesAdded() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();

      config.getAcceptorFactoryClassNames().add("org.jboss.messaging.tests.unit.core.remoting.impl.AcceptorFactory1");
      config.getAcceptorFactoryClassNames().add("org.jboss.messaging.tests.unit.core.remoting.impl.AcceptorFactory2");
      config.getAcceptorFactoryClassNames().add("org.jboss.messaging.tests.unit.core.remoting.impl.AcceptorFactory3");
      
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.start();
      
      assertEquals(3, remotingService.getAcceptors().size());
      boolean got1 = false;
      boolean got2 = false;
      boolean got3 = false;
      for (Acceptor acceptor: remotingService.getAcceptors())
      {
         if (acceptor instanceof AcceptorFactory1.Acceptor1)
         {
            got1 = true;
         }
         if (acceptor instanceof AcceptorFactory2.Acceptor2)
         {
            got2 = true;
         }
         if (acceptor instanceof AcceptorFactory3.Acceptor3)
         {
            got3 = true;
         }
      }
      assertTrue(got1 && got2 && got3);
   }
   
   public void testCreateGetDestroyConnection() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.start();
      
      assertEquals(0, remotingService.getConnections().size());
      Connection connection1 = EasyMock.createStrictMock(Connection.class);
      EasyMock.expect(connection1.getID()).andStubReturn(1);
      
      Connection connection2 = EasyMock.createStrictMock(Connection.class);
      EasyMock.expect(connection2.getID()).andStubReturn(2);

      Connection connection3 = EasyMock.createStrictMock(Connection.class);
      EasyMock.expect(connection3.getID()).andStubReturn(3);

      EasyMock.replay(connection1, connection2, connection3);
      
      remotingService.connectionCreated(connection1);
      remotingService.connectionCreated(connection2);
      remotingService.connectionCreated(connection3);
      
      EasyMock.verify(connection1, connection2, connection3);
      
      assertEquals(3, remotingService.getConnections().size());
      
      RemotingConnection rc1 = remotingService.getConnection(1);
      assertNotNull(rc1);
      RemotingConnection rc2 = remotingService.getConnection(2);
      assertNotNull(rc2);
      RemotingConnection rc3 = remotingService.getConnection(3);
      assertNotNull(rc3);
      
      assertEquals(1, rc1.getID());
      assertEquals(2, rc2.getID());
      assertEquals(3, rc3.getID());
   
      EasyMock.reset(connection1, connection2, connection3);
      
      EasyMock.replay(connection1, connection2, connection3);
      
      remotingService.connectionDestroyed(1);
      remotingService.connectionDestroyed(2);
      remotingService.connectionDestroyed(3);
      
      EasyMock.verify(connection1, connection2, connection3);
      
      assertEquals(0, remotingService.getConnections().size());
      
      rc1 = remotingService.getConnection(1);
      assertNull(rc1);
      rc2 = remotingService.getConnection(2);
      assertNull(rc2);
      rc3 = remotingService.getConnection(3);
      assertNull(rc3);
   }
   
   public void testConnectionException() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      RemotingServiceImpl remotingService = new RemotingServiceImpl(config);
      remotingService.start();
      
      final long id = 1212;
      
      assertEquals(0, remotingService.getConnections().size());
      Connection connection1 = EasyMock.createStrictMock(Connection.class);
      EasyMock.expect(connection1.getID()).andStubReturn(id);      
      connection1.close();
      
      EasyMock.replay(connection1);
      
      remotingService.connectionCreated(connection1);
            
      RemotingConnection rc1 = remotingService.getConnection(id);
      assertNotNull(rc1);
      
      class Listener implements FailureListener
      {
         volatile MessagingException me;
         public void connectionFailed(MessagingException me)
         {
            this.me = me;
         }
      }
      
      Listener listener = new Listener();
      rc1.addFailureListener(listener);
      
      MessagingException me2 = new MessagingException(1212, "askjaksj");
      
      remotingService.connectionException(id, me2);
      
      assertNotNull(listener.me);
      assertTrue(listener.me == me2);
      
      EasyMock.verify(connection1);       
   }
   
   


}
