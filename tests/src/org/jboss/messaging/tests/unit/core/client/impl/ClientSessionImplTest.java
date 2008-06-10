/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientConnectionInternal;
import org.jboss.messaging.core.client.impl.ClientConsumerInternal;
import org.jboss.messaging.core.client.impl.ClientConsumerPacketHandler;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ClientSessionImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientSessionImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientSessionImplTest.class);

   // Private -----------------------------------------------------------------------------------------------------------

   public void testConstructor() throws Exception
   {            
      testConstructor(132, true, 10, true, true, true, true);
      testConstructor(132, false, 10, false, false, false, false);
   }

   public void testConstructorInvalidArgs() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      try
      {
         new ClientSessionImpl(conn, 1, false, -2, false, false, false, false);
         fail("Should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //Ok
      }

      try
      {
         new ClientSessionImpl(conn, 1, false, -10, false, false, false, false);
         fail("Should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //Ok
      }

      try
      {
         new ClientSessionImpl(conn, 1, false, 0, false, false, false, false);
         fail("Should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //Ok
      }
   }

   public void testCreateQueue() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      SessionCreateQueueMessage request = new SessionCreateQueueMessage(new SimpleString("blah"), new SimpleString("hagshg"),
            new SimpleString("jhjhs"), false, false);
      
      final int targetID = 121;
      
      EasyMock.expect(rc.sendBlocking(targetID, targetID, request)).andReturn(null);
      
      EasyMock.replay(conn);
      EasyMock.replay(rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      session.createQueue(request.getAddress(), request.getQueueName(), request.getFilterString(), request.isDurable(), request.isTemporary());
      
      EasyMock.verify(conn);
      EasyMock.verify(rc);      
   }
   
   public void testDeleteQueue() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      SessionDeleteQueueMessage request = new SessionDeleteQueueMessage(new SimpleString("blah"));
      
      final int targetID = 121;
      
      EasyMock.expect(rc.sendBlocking(targetID, targetID, request)).andReturn(null);
      
      EasyMock.replay(conn);
      EasyMock.replay(rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      session.deleteQueue(request.getQueueName());
      
      EasyMock.verify(conn);
      EasyMock.verify(rc);      
   }
   
   public void testQueueQuery() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      SessionQueueQueryMessage request = new SessionQueueQueryMessage(new SimpleString("blah"));
      
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage();
      
      final int targetID = 121;
      
      EasyMock.expect(rc.sendBlocking(targetID, targetID, request)).andReturn(resp);
      
      EasyMock.replay(conn);
      EasyMock.replay(rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      SessionQueueQueryResponseMessage resp2 = session.queueQuery(request.getQueueName());
      
      EasyMock.verify(conn);
      EasyMock.verify(rc);  
      
      assertTrue(resp == resp2);
   }
   
   public void testBindingQuery() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      SessionBindingQueryMessage request = new SessionBindingQueryMessage(new SimpleString("blah"));
      
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage();
      
      final int targetID = 121;
      
      EasyMock.expect(rc.sendBlocking(targetID, targetID, request)).andReturn(resp);
      
      EasyMock.replay(conn);
      EasyMock.replay(rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      SessionBindingQueryResponseMessage resp2 = session.bindingQuery(request.getAddress());
      
      EasyMock.verify(conn);
      EasyMock.verify(rc);  
      
      assertTrue(resp == resp2);
   }
   
   public void testAddDestination() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      SessionAddDestinationMessage request = new SessionAddDestinationMessage(new SimpleString("blah"), true);
      
      final int targetID = 121;
      
      EasyMock.expect(rc.sendBlocking(targetID, targetID, request)).andReturn(null);
      
      EasyMock.replay(conn);
      EasyMock.replay(rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      session.addDestination(request.getAddress(), request.isTemporary());
      
      EasyMock.verify(conn);
      EasyMock.verify(rc);  
   }
   
   public void testRemoveDestination() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      SessionRemoveDestinationMessage request = new SessionRemoveDestinationMessage(new SimpleString("blah"), true);
      
      final int targetID = 121;
      
      EasyMock.expect(rc.sendBlocking(targetID, targetID, request)).andReturn(null);
      
      EasyMock.replay(conn);
      EasyMock.replay(rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      session.removeDestination(request.getAddress(), true);
      
      EasyMock.verify(conn);
      EasyMock.verify(rc);  
   }
   
   public void testCreateConsumer() throws Exception
   {
      //First test with the wide createConsumer method
      
      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), new SimpleString("ygyggyg"),
            false, false, false, 121455, 76556, 121455);
      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), null,
            false, false, false, 121455, 76556, 121455);
      
      //test where server window size overrides client window size
      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), null,
            false, false, false, 121455, 76556, 675675765);
      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), null,
            false, false, false, 121455, 76556, 1);
      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), null,
            false, false, false, 121455, 76556, -1);
      
      //And with the method that takes defaults from the cf
      
      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), new SimpleString("ygyggyg"),
            false, false, false, 121455, 76556, 121455);
      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), null,
            false, false, false, 121455, 76556, 121455);
      
      //test where server window size overrides client window size
      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), null,
            false, false, false, 121455, 76556, 675675765);
      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), null,
            false, false, false, 121455, 76556, 1);
      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), null,
            false, false, false, 121455, 76556, -1);
      
      // And with the basic createConsumer method:
      
      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, 121455);
      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, 121455);
      
      //test where server window size overrides client window size
      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, 675675765);
      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, 1);
      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, -1);
   }
   
   // Private -------------------------------------------------------------------------------------------

   private void testCreateConsumerDefaultsMethod(final SimpleString queueName, final SimpleString filterString, final boolean noLocal,
         final boolean autoDeleteQueue, final boolean direct,
         final int windowSize, final int maxRate, final int serverWindowSize) throws Exception
   {
      ClientConnectionFactory cf = EasyMock.createStrictMock(ClientConnectionFactory.class);
      
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
         
      EasyMock.expect(cf.getDefaultConsumerWindowSize()).andReturn(windowSize);
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.getDefaultConsumerMaxRate()).andReturn(maxRate);      
                  
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      final long clientTargetID = 87126716;
      
      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
      
      final long sessionTargetID = 9121892;
      
      SessionCreateConsumerMessage request =
         new SessionCreateConsumerMessage(clientTargetID, queueName, filterString, noLocal, autoDeleteQueue,
                                          windowSize, maxRate);             
      
      SessionCreateConsumerResponseMessage resp = 
         new SessionCreateConsumerResponseMessage(656652, serverWindowSize);
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);
      
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      pd.register(new ClientConsumerPacketHandler(null, clientTargetID));
      
      rc.sendOneWay(resp.getConsumerTargetID(), sessionTargetID,
                    new ConsumerFlowCreditMessage(resp.getWindowSize()));
      
      EasyMock.replay(cf);
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);
      
      ClientSession session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      ClientConsumerInternal consumer = (ClientConsumerInternal)session.createConsumer(queueName, filterString, noLocal, autoDeleteQueue,
                                                                   direct);              
      EasyMock.verify(cf);
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd); 
      
      assertEquals(clientTargetID, consumer.getClientTargetID());
      
      if (serverWindowSize == -1)
      {
         assertEquals(0, consumer.getClientWindowSize());
      }
      else if (serverWindowSize == 1)
      {
         assertEquals(1, consumer.getClientWindowSize());
      }
      else if (serverWindowSize > 1)
      {
         assertEquals(serverWindowSize >> 1, consumer.getClientWindowSize());
      }
   }
   
   private void testCreateConsumerWideMethod(final SimpleString queueName, final SimpleString filterString, final boolean noLocal,
         final boolean autoDeleteQueue, final boolean direct,
         final int windowSize, final int maxRate, final int serverWindowSize) throws Exception
   {
      ClientConnectionFactory cf = EasyMock.createStrictMock(ClientConnectionFactory.class);
      
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
       
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      final long clientTargetID = 87126716;
      
      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
      
      final long sessionTargetID = 9121892;
      
      SessionCreateConsumerMessage request =
         new SessionCreateConsumerMessage(clientTargetID, queueName, filterString, noLocal, autoDeleteQueue,
                                          windowSize, maxRate);             
      
      SessionCreateConsumerResponseMessage resp = 
         new SessionCreateConsumerResponseMessage(656652, serverWindowSize);
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);
      
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      pd.register(new ClientConsumerPacketHandler(null, clientTargetID));
      
      rc.sendOneWay(resp.getConsumerTargetID(), sessionTargetID,
                    new ConsumerFlowCreditMessage(resp.getWindowSize()));
      
      EasyMock.replay(cf);
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);
      
      ClientSession session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      ClientConsumerInternal consumer = (ClientConsumerInternal)session.createConsumer(queueName, filterString, noLocal, autoDeleteQueue,
                                                                   direct, windowSize, maxRate);    
      EasyMock.verify(cf);
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd); 
      
      assertEquals(clientTargetID, consumer.getClientTargetID());
      
      if (serverWindowSize == -1)
      {
         assertEquals(0, consumer.getClientWindowSize());
      }
      else if (serverWindowSize == 1)
      {
         assertEquals(1, consumer.getClientWindowSize());
      }
      else if (serverWindowSize > 1)
      {
         assertEquals(serverWindowSize >> 1, consumer.getClientWindowSize());
      }
   }
   
   private void testCreateConsumerBasicMethod(final SimpleString queueName, final int windowSize,
         final int maxRate, final int serverWindowSize) throws Exception
   {
      ClientConnectionFactory cf = EasyMock.createStrictMock(ClientConnectionFactory.class);
      
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.getDefaultConsumerWindowSize()).andReturn(windowSize);
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.getDefaultConsumerMaxRate()).andReturn(maxRate);   
       
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      final long clientTargetID = 87126716;
      
      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
      
      final long sessionTargetID = 9121892;
      
      SessionCreateConsumerMessage request =
         new SessionCreateConsumerMessage(clientTargetID, queueName, null, false, false,
                                          windowSize, maxRate);             
      
      SessionCreateConsumerResponseMessage resp = 
         new SessionCreateConsumerResponseMessage(656652, serverWindowSize);
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);
      
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      pd.register(new ClientConsumerPacketHandler(null, clientTargetID));
      
      rc.sendOneWay(resp.getConsumerTargetID(), sessionTargetID,
                    new ConsumerFlowCreditMessage(resp.getWindowSize()));
      
      EasyMock.replay(cf);
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);
      
      ClientSession session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      ClientConsumerInternal consumer = (ClientConsumerInternal)session.createConsumer(queueName);    
      EasyMock.verify(cf);
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd); 
      
      assertEquals(clientTargetID, consumer.getClientTargetID());
      
      if (serverWindowSize == -1)
      {
         assertEquals(0, consumer.getClientWindowSize());
      }
      else if (serverWindowSize == 1)
      {
         assertEquals(1, consumer.getClientWindowSize());
      }
      else if (serverWindowSize > 1)
      {
         assertEquals(serverWindowSize >> 1, consumer.getClientWindowSize());
      }
   }
   
   private void testConstructor(final long serverTargetID,
         final boolean xa,
         final int lazyAckBatchSize, final boolean cacheProducers,                            
         final boolean autoCommitSends, final boolean autoCommitAcks,
         final boolean blockOnAcknowledge) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);

      EasyMock.replay(conn);
      EasyMock.replay(rc);

      ClientSession session = new ClientSessionImpl(conn, serverTargetID, xa,
            lazyAckBatchSize, cacheProducers, autoCommitSends, autoCommitAcks, blockOnAcknowledge);

      EasyMock.verify(conn);
      EasyMock.verify(rc);      

      assertTrue(conn == session.getConnection());
      assertEquals(xa, session.isXA());
      assertEquals(lazyAckBatchSize, session.getLazyAckBatchSize());
      assertEquals(cacheProducers, session.isCacheProducers());
      assertEquals(autoCommitSends, session.isAutoCommitSends());
      assertEquals(autoCommitAcks, session.isAutoCommitAcks());
      assertEquals(blockOnAcknowledge, session.isBlockOnAcknowledge());
   }
}

