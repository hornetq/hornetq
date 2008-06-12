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

import static org.jboss.messaging.tests.util.RandomUtil.randomXid;

import java.util.Arrays;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ClientConnectionInternal;
import org.jboss.messaging.core.client.impl.ClientConsumerInternal;
import org.jboss.messaging.core.client.impl.ClientConsumerPacketHandler;
import org.jboss.messaging.core.client.impl.ClientProducerInternal;
import org.jboss.messaging.core.client.impl.ClientProducerPacketHandler;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
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

   // Public -----------------------------------------------------------------------------------------------------------

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
      
      EasyMock.replay(conn, rc);

      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      session.createQueue(request.getAddress(), request.getQueueName(), request.getFilterString(), request.isDurable(), request.isTemporary());
      
      EasyMock.verify(conn, rc);     
   }
   
   public void testDeleteQueue() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      SessionDeleteQueueMessage request = new SessionDeleteQueueMessage(new SimpleString("blah"));
      
      final int targetID = 121;
      
      EasyMock.expect(rc.sendBlocking(targetID, targetID, request)).andReturn(null);
      
      EasyMock.replay(conn, rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      session.deleteQueue(request.getQueueName());
      
      EasyMock.verify(conn, rc);     
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
      
      EasyMock.replay(conn, rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      SessionQueueQueryResponseMessage resp2 = session.queueQuery(request.getQueueName());
      
      EasyMock.verify(conn, rc);
      
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
      
      EasyMock.replay(conn, rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      SessionBindingQueryResponseMessage resp2 = session.bindingQuery(request.getAddress());
      
      EasyMock.verify(conn, rc); 
      
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
      
      EasyMock.replay(conn, rc);
      
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      session.addDestination(request.getAddress(), request.isTemporary());
      
      EasyMock.verify(conn, rc); 
   }
   
   public void testRemoveDestination() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      SessionRemoveDestinationMessage request = new SessionRemoveDestinationMessage(new SimpleString("blah"), true);
      
      final int targetID = 121;
      
      EasyMock.expect(rc.sendBlocking(targetID, targetID, request)).andReturn(null);
      
      EasyMock.replay(conn, rc);
                  
      ClientSession session = new ClientSessionImpl(conn, targetID, false, -1, false, false, false, false);
                  
      session.removeDestination(request.getAddress(), true);
      
      EasyMock.verify(conn, rc); 
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
   
   public void testCreateProducer() throws Exception
   {
      //test with the wide method
      
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, false, false);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, false, true);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, true, false);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, true, true);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, false, false);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, false, true);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, true, false);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, true, true);
      
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, false, false);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, false, true);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, true, false);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, true, true);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, false, false);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, false, true);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, true, false);
      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, true, true);
      
      //Test with the basic method
      
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, false, false);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, false, true);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, true, false);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, true, true);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, false, false);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, false, true);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, true, false);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, true, true);
      
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, false, false);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, false, true);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, true, false);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, true, true);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, false, false);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, false, true);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, true, false);
      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, true, true);

      //Test with the rate limited method
      
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, false, false, false);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, false, false, true);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, false, true, false);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, false, true, true);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, true, false, false);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, true, false, true);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, true, true, false);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, true, true, true);
      
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, false, false);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, false, true);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, true, false);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, true, true);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, false, false);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, false, true);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, true, false);
      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, true, true);
      
      //Test with the create producer with window size method
      
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, false, false, false);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, false, false, true);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, false, true, false);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, false, true, true);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, true, false, false);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, true, false, true);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, true, true, false);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, true, true, true);
      
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, false, false);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, false, true);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, true, false);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, true, true);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, false, false);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, false, true);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, true, false);
      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, true, true);      
   }
   
   public void testProducerCaching() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      final SimpleString address1 = new SimpleString("gyugg");
      final SimpleString address2 = new SimpleString("g237429834");
      final int windowSize = 72887827;
      final int maxRate = -1;
      
      //In create producer method
            
      {
         EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
         
         final long clientTargetID = 7676876;
         
         EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
                  
         SessionCreateProducerMessage request =
            new SessionCreateProducerMessage(clientTargetID, address1, windowSize, maxRate);             
         
         SessionCreateProducerResponseMessage resp = 
            new SessionCreateProducerResponseMessage(67765765, windowSize, maxRate);
         
         EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);
         
         EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
         
         EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
                  
         pd.register(new ClientProducerPacketHandler(null, clientTargetID));
      }
      
      {
         EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
         
         final long clientTargetID = 54654654;
         
         EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
         
         SessionCreateProducerMessage request =
            new SessionCreateProducerMessage(clientTargetID, address2, windowSize, maxRate);             
         
         SessionCreateProducerResponseMessage resp = 
            new SessionCreateProducerResponseMessage(7676876, windowSize, maxRate);
         
         EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);
         
         EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
         
         EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
                  
         pd.register(new ClientProducerPacketHandler(null, clientTargetID));
      }
      
      EasyMock.replay(conn, rc, pd);
      
      //Create three with address1 - only one should be actually created
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, true, false, false, false);
      
      assertEquals(0, session.getProducerCache().size());
      
      ClientProducerInternal producer1 = (ClientProducerInternal)session.createProducer(address1, windowSize, maxRate,
                                                                                        false, false);
      
      assertEquals(0, session.getProducerCache().size());
      
      session.removeProducer(producer1);  
      
      assertEquals(1, session.getProducerCache().size());
      
      ClientProducerInternal producer2 = (ClientProducerInternal)session.createProducer(address1, windowSize, maxRate,
                                                                                       false, false);      
      
      assertEquals(0, session.getProducerCache().size());
      
      session.removeProducer(producer2);
      
      assertEquals(1, session.getProducerCache().size());
      
      ClientProducerInternal producer3 = (ClientProducerInternal)session.createProducer(address1, windowSize, maxRate,
                                                                                        false, false);
      
      assertEquals(0, session.getProducerCache().size());
      
      session.removeProducer(producer3);
      
      assertEquals(1, session.getProducerCache().size());
      
      //Create another with a different address
      
      ClientProducerInternal producer4 = (ClientProducerInternal)session.createProducer(address2, windowSize, maxRate,
                                                                                        false, false);
      
      assertEquals(1, session.getProducerCache().size());
      
      session.removeProducer(producer4); 
      
      assertEquals(2, session.getProducerCache().size());
            
      EasyMock.verify(conn, rc, pd);     
      
      assertTrue(producer1 == producer2);
      assertTrue(producer2 == producer3);
      assertFalse(producer1 == producer4);
      assertFalse(producer2 == producer4);
      assertFalse(producer3 == producer4);      
   }
   
   public void testProducerNoCaching() throws Exception
   { 
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
          
      final long sessionTargetID = 7617622;      
      final SimpleString address = new SimpleString("gyugg");
      final int windowSize = 72887827;
      final int maxRate = -1;

      for (int i = 0; i < 3; i++)
      {
         //In create producer method
         
         EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
         
         final long clientTargetID = i + 65655;
         
         EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
         
         
         SessionCreateProducerMessage request =
            new SessionCreateProducerMessage(clientTargetID, address, windowSize, maxRate);             
         
         SessionCreateProducerResponseMessage resp = 
            new SessionCreateProducerResponseMessage(i + 273263, windowSize, maxRate);
         
         EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);
         
         EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
         
         EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
                  
         pd.register(new ClientProducerPacketHandler(null, clientTargetID));      
      }

      EasyMock.replay(conn, rc, pd);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);

      ClientProducerInternal producer1 = (ClientProducerInternal)session.createProducer(address, windowSize, maxRate,
                                                                                        false, false);
      session.removeProducer(producer1);  
      
      ClientProducerInternal producer2 = (ClientProducerInternal)session.createProducer(address, windowSize, maxRate,
                                                                                       false, false);      
      session.removeProducer(producer2);
      
      ClientProducerInternal producer3 = (ClientProducerInternal)session.createProducer(address, windowSize, maxRate,
                                                                                        false, false);
      session.removeProducer(producer3);
      
      EasyMock.verify(conn, rc, pd);
      
      assertFalse(producer1 == producer2);
      assertFalse(producer2 == producer3);
      assertFalse(producer1 == producer3);
   }
   
   public void testCreateBrowser() throws Exception
   {
      testCreateBrowser(true);
      testCreateBrowser(false);
   }
   
   
   
   public void testGetXAResource() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      ClientSession session = new ClientSessionImpl(conn, 5465, false, -1, false, false, false, false);
      
      XAResource res = session.getXAResource();
      
      assertTrue(res == session);
   }
   
   public void testTransactedSessionAcknowledgeNotBroken() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      final int numMessages = 100;
      
      final int sessionTargetID = 71267162;
            
      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(numMessages - 1, true);
            
      rc.sendOneWay(sessionTargetID, sessionTargetID, message);

      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.SESS_COMMIT))).andReturn(null);
      
      //Create some consumers
      
      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      
      cons1.recover(numMessages);
      cons2.recover(numMessages);
            
      SessionAcknowledgeMessage message2 = new SessionAcknowledgeMessage(numMessages * 2 - 1, true);
      
      rc.sendOneWay(sessionTargetID, sessionTargetID, message2);

      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.SESS_ROLLBACK))).andReturn(null);
                  
      EasyMock.replay(conn, rc, pd, cons1, cons2);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      session.addConsumer(cons1);
      session.addConsumer(cons2);
      
      //Simulate some messages being delivered in a non broken sequence (i.e. what would happen with a single consumer
      //on the session)
            
      for (int i = 0; i < numMessages; i++)
      {
         session.delivered(i, false);
         
         session.acknowledge();
      }
      
      //Then commit
      session.commit();
      
      for (int i = numMessages; i < numMessages * 2; i++)
      {
         session.delivered(i, false);
         
         session.acknowledge();
      }
      
      session.rollback();
      
      EasyMock.verify(conn, rc, pd, cons1, cons2);
   }
   
   public void testAutoCommitSessionAcknowledge() throws Exception
   {
      testAutoCommitSessionAcknowledge(true);
      testAutoCommitSessionAcknowledge(false);
   }
            
   public void testTransactedSessionAcknowledgeBroken() throws Exception
   {
      testTransactedSessionAcknowledgeBroken(true);
      testTransactedSessionAcknowledgeBroken(false);
   }
         
   public void testTransactedSessionAcknowledgeNotBrokenExpired() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      final int[] messages = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      final int sessionTargetID = 71267162;
         
      for (int i = 0; i < messages.length; i++)
      {
         SessionCancelMessage message = new SessionCancelMessage(messages[i], true);
         
         rc.sendOneWay(sessionTargetID, sessionTargetID, message);
      }
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.SESS_COMMIT))).andReturn(null);
                  
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      //Simulate some messages being delivered in a non broken sequence (i.e. what would happen with a single consumer
      //on the session)
            
      for (int i = 0; i < messages.length; i++)
      {
         session.delivered(messages[i], true);
         
         session.acknowledge();
      }
      
      //Then commit
      session.commit();
      
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd); 
   }
   
   public void testTransactedSessionAcknowledgeBrokenExpired() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      final int[] messages = new int[] { 1, 3, 5, 7, 9, 2, 4, 10, 20, 21, 22, 23, 19, 18, 15, 30, 31, 32, 40, 35 };
      
      final int sessionTargetID = 71267162;
         
      for (int i = 0; i < messages.length; i++)
      {
         SessionCancelMessage message = new SessionCancelMessage(messages[i], true);
         
         rc.sendOneWay(sessionTargetID, sessionTargetID, message);
      }
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.SESS_COMMIT))).andReturn(null);
                  
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      //Simulate some messages being delivered in a broken sequence (i.e. what would happen with a single consumer
      //on the session)
            
      for (int i = 0; i < messages.length; i++)
      {
         session.delivered(messages[i], true);
         
         session.acknowledge();
      }
      
      //Then commit
      session.commit();
      
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd); 
   }
   
   public void testClose() throws Exception   
   {
      testClose(true);
      testClose(false);
   }
   
   
    
   public void testAddRemoveConsumer() throws Exception
   {
      testAddRemoveConsumer(true);
      testAddRemoveConsumer(false);
   }
   
   public void testAddRemoveProducer() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
                  
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      EasyMock.verify(conn, rc);
      
      EasyMock.reset(conn, rc);
                                      
      ClientProducerInternal prod1 = EasyMock.createStrictMock(ClientProducerInternal.class);
      ClientProducerInternal prod2 = EasyMock.createStrictMock(ClientProducerInternal.class);
      
      session.addProducer(prod1);
      session.addProducer(prod2);
      
      assertEquals(2, session.getProducers().size());
      assertTrue(session.getProducers().contains(prod1));
      assertTrue(session.getProducers().contains(prod2));
       
      session.removeProducer(prod1);
      
      assertEquals(1, session.getProducers().size());
      assertTrue(session.getProducers().contains(prod2));   
      
      session.removeProducer(prod2);
      
      assertEquals(0, session.getProducers().size()); 
   }
   
   public void testAddRemoveBrowser() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
                  
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      EasyMock.verify(conn, rc);
      
      EasyMock.reset(conn, rc);
                                      
      ClientBrowser browser1 = EasyMock.createStrictMock(ClientBrowser.class);
      ClientBrowser browser2 = EasyMock.createStrictMock(ClientBrowser.class);
      
      session.addBrowser(browser1);
      session.addBrowser(browser2);
      
      assertEquals(2, session.getBrowsers().size());
      assertTrue(session.getBrowsers().contains(browser1));
      assertTrue(session.getBrowsers().contains(browser2));
       
      session.removeBrowser(browser1);
      
      assertEquals(1, session.getBrowsers().size());
      assertTrue(session.getBrowsers().contains(browser2));   
      
      session.removeBrowser(browser2);
      
      assertEquals(0, session.getBrowsers().size()); 
   }
   
   public void testXACommit() throws Exception
   {
      testXACommit(false, false);
      testXACommit(false, true);
      testXACommit(true, false);
      testXACommit(true, true);
   }
   
   public void testXAEnd() throws Exception
   {
      testXAEnd(XAResource.TMSUSPEND, false);
      testXAEnd(XAResource.TMSUSPEND, true);
      testXAEnd(XAResource.TMSUCCESS, false);
      testXAEnd(XAResource.TMSUCCESS, true);
      testXAEnd(XAResource.TMFAIL, false);
      testXAEnd(XAResource.TMFAIL, true);
   }
   
   public void testXAForget() throws Exception
   {
      testXAForget(false);
      testXAForget(true);
   }
   
   public void testGetTransactionTimeout() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      Packet packet = new EmptyPacket(EmptyPacket.SESS_XA_GET_TIMEOUT);

      final int timeout = 1098289;
      
      SessionXAGetTimeoutResponseMessage resp = new SessionXAGetTimeoutResponseMessage(timeout);
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      int timeout2 = session.getTransactionTimeout();
            
      EasyMock.verify(conn, rc);  
      
      assertEquals(timeout, timeout2);
   }
   
   public void testIsSameRM() throws Exception
   {
      Location location1 = new LocationImpl(TransportType.TCP, "blah1");      
      Location location2 = new LocationImpl(TransportType.TCP, "blah2");
      
      ClientConnectionInternal conn1 = EasyMock.createStrictMock(ClientConnectionInternal.class);      
      RemotingConnection rc1 = EasyMock.createStrictMock(RemotingConnection.class);          
      EasyMock.expect(conn1.getRemotingConnection()).andReturn(rc1);
      
      ClientConnectionInternal conn2 = EasyMock.createStrictMock(ClientConnectionInternal.class);      
      RemotingConnection rc2 = EasyMock.createStrictMock(RemotingConnection.class);          
      EasyMock.expect(conn2.getRemotingConnection()).andReturn(rc2);
      
      ClientConnectionInternal conn3 = EasyMock.createStrictMock(ClientConnectionInternal.class);      
      RemotingConnection rc3 = EasyMock.createStrictMock(RemotingConnection.class);          
      EasyMock.expect(conn3.getRemotingConnection()).andReturn(rc3);
       
      EasyMock.expect(conn1.getRemotingConnection()).andReturn(rc1);      
      EasyMock.expect(rc1.getLocation()).andReturn(location1);      
      EasyMock.expect(conn2.getRemotingConnection()).andReturn(rc2);      
      EasyMock.expect(rc2.getLocation()).andReturn(location1);
      
      EasyMock.expect(conn2.getRemotingConnection()).andReturn(rc2);      
      EasyMock.expect(rc2.getLocation()).andReturn(location1);
      EasyMock.expect(conn1.getRemotingConnection()).andReturn(rc1);      
      EasyMock.expect(rc1.getLocation()).andReturn(location1);      
            
      EasyMock.expect(conn1.getRemotingConnection()).andReturn(rc1);      
      EasyMock.expect(rc1.getLocation()).andReturn(location1);      
      EasyMock.expect(conn3.getRemotingConnection()).andReturn(rc3);      
      EasyMock.expect(rc3.getLocation()).andReturn(location2);
      
      EasyMock.expect(conn3.getRemotingConnection()).andReturn(rc3);      
      EasyMock.expect(rc3.getLocation()).andReturn(location2);
      EasyMock.expect(conn1.getRemotingConnection()).andReturn(rc1);      
      EasyMock.expect(rc1.getLocation()).andReturn(location1);      
            
      EasyMock.expect(conn2.getRemotingConnection()).andReturn(rc2);      
      EasyMock.expect(rc2.getLocation()).andReturn(location1);      
      EasyMock.expect(conn3.getRemotingConnection()).andReturn(rc3);      
      EasyMock.expect(rc3.getLocation()).andReturn(location2);
      
      EasyMock.expect(conn3.getRemotingConnection()).andReturn(rc3);      
      EasyMock.expect(rc3.getLocation()).andReturn(location2);
      EasyMock.expect(conn2.getRemotingConnection()).andReturn(rc2);      
      EasyMock.expect(rc2.getLocation()).andReturn(location1);      
          
      EasyMock.replay(conn1, conn2, conn3, rc1, rc2, rc3);
      
      ClientSessionInternal session1 = new ClientSessionImpl(conn1, 4343, true, -1, false, false, false, false);
      
      ClientSessionInternal session2 = new ClientSessionImpl(conn2, 4343, true, -1, false, false, false, false);
      
      ClientSessionInternal session3 = new ClientSessionImpl(conn3, 4343, true, -1, false, false, false, false);
      
      assertTrue(session1.isSameRM(session2));
      assertTrue(session2.isSameRM(session1));
      
      assertFalse(session1.isSameRM(session3));
      assertFalse(session3.isSameRM(session1));
      
      assertFalse(session2.isSameRM(session3));
      assertFalse(session3.isSameRM(session2));
      
      EasyMock.verify(conn1, conn1, conn3, rc1, rc2, rc3);
   }
   
   public void testXAPrepare() throws Exception
   {
      testXAPrepare(false, false);
      testXAPrepare(false, true);
      testXAPrepare(true, false);
      testXAPrepare(true, true);
   }
   
   public void testXARecover() throws Exception
   {
      testXARecover(XAResource.TMNOFLAGS);
      testXARecover(XAResource.TMSTARTRSCAN);
      testXARecover(XAResource.TMENDRSCAN);
      testXARecover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
   }
   
   public void testXARollback() throws Exception
   {
      testXARollback(true);
      testXARollback(false);
   }
   
   public void testXASetTransactionTimeout() throws Exception
   {
      testXASetTransactionTimeout(false);
      testXASetTransactionTimeout(true);
   }
   
   public void testXAStart() throws Exception
   {
      testXAStart(XAResource.TMJOIN, false);
      testXAStart(XAResource.TMRESUME, false);
      testXAStart(XAResource.TMNOFLAGS, false);
      testXAStart(XAResource.TMJOIN, true);
      testXAStart(XAResource.TMRESUME, true);
      testXAStart(XAResource.TMNOFLAGS, true);
   }
   
   public void notXA() throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
              
      final long sessionTargetID = 9121892;
                  
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      EasyMock.verify(conn, rc);
      
      EasyMock.reset(conn, rc);      
      try
      {
         session.commit(randomXid(), false);
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.end(randomXid(), 8778);
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.forget(randomXid());
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.getTransactionTimeout();
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.isSameRM(session);
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.prepare(randomXid());
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.recover(89787);
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.rollback(randomXid());
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.setTransactionTimeout(767);
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
      
      try
      {
         session.start(randomXid(), 8768);
         fail("Should throw exception");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_RMERR, e.errorCode);
      }
   }
   
   // Private -------------------------------------------------------------------------------------------

   private void testClose(boolean delivered) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
          
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
                  
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      EasyMock.verify(conn, rc);
      
      EasyMock.reset(conn, rc);
      
      ClientProducerInternal prod1 = EasyMock.createStrictMock(ClientProducerInternal.class);
      ClientProducerInternal prod2 = EasyMock.createStrictMock(ClientProducerInternal.class);
      
      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      
      ClientBrowser browser1 = EasyMock.createStrictMock(ClientBrowser.class);
      ClientBrowser browser2 = EasyMock.createStrictMock(ClientBrowser.class);
                    
      prod1.close();
      prod2.close();
      cons1.close();
      cons2.close();
      browser1.close();
      browser2.close();
      
      final int numDeliveries = 10;
      
      if (delivered)
      {
         SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(numDeliveries - 1, true);
         
         rc.sendOneWay(sessionTargetID, sessionTargetID, message);
      }
            
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.CLOSE))).andReturn(null);
      
      conn.removeSession(session);      
            
      EasyMock.replay(conn, rc, prod1, prod2, cons1, cons2, browser1, browser2);
                 
      session.addProducer(prod1);
      session.addProducer(prod2);
      
      session.addConsumer(cons1);
      session.addConsumer(cons2);
      
      session.addBrowser(browser1);
      session.addBrowser(browser2);
      
      assertFalse(session.isClosed());
      
      if (delivered)
      {
         //Simulate there being some undelivered messages
         for (int i = 0; i < numDeliveries; i++)
         {
            session.delivered(i, false);
            session.acknowledge();
         }
      }
            
      session.close();
      
      EasyMock.verify(conn, rc, prod1, prod2, cons1, cons2, browser1, browser2);
      
      assertTrue(session.isClosed());  
      
      EasyMock.reset(conn, rc, prod1, prod2, cons1, cons2, browser1, browser2);
      
      EasyMock.replay(conn, rc, prod1, prod2, cons1, cons2, browser1, browser2);
      
      //Close again should do nothing
      
      session.close();
      
      EasyMock.verify(conn, rc, prod1, prod2, cons1, cons2, browser1, browser2);
      
      try
      {
         session.createQueue(new SimpleString("trtr"), new SimpleString("iuasij"), null, false, false);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.deleteQueue(new SimpleString("trtr"));
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.addDestination(new SimpleString("trtr"), false);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.removeDestination(new SimpleString("trtr"), false);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.queueQuery(new SimpleString("trtr"));
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.bindingQuery(new SimpleString("trtr"));
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createConsumer(new SimpleString("trtr"));
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createConsumer(new SimpleString("iasjq"), null, false, false, false);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createConsumer(new SimpleString("husuhsuh"), null, false, false, false, 8787, 7162761);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createBrowser(new SimpleString("husuhsuh"));
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createBrowser(new SimpleString("husuhsuh"), null);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createProducer(new SimpleString("husuhsuh"));
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createProducer(new SimpleString("iashi"), 878778, 8778, false, false);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createRateLimitedProducer(new SimpleString("uhsuhs"), 78676);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.createProducerWithWindowSize(new SimpleString("uhsuhs"), 78676);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.commit();
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.rollback();
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         session.acknowledge();
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
   }
   
   private void testXAStart(int flags, boolean error) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      Xid xid = randomXid();
      
      Packet packet = null;
      if (flags == XAResource.TMJOIN)
      {
         packet = new SessionXAJoinMessage(xid);           
      }
      else if (flags == XAResource.TMRESUME)
      {
         packet = new SessionXAResumeMessage(xid);
      }
      else if (flags == XAResource.TMNOFLAGS)
      {
         packet = new SessionXAStartMessage(xid);
      }

      final int numMessages = 10;
      
      if (flags != XAResource.TMNOFLAGS)
      {      
         SessionAcknowledgeMessage msg = new SessionAcknowledgeMessage(numMessages - 1, true);
         
         rc.sendOneWay(sessionTargetID, sessionTargetID, msg);
      }
      
      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      //Simulate some unflushed messages
      
      for (int i = 0; i < numMessages; i++)
      {
         session.delivered(i, false);
         session.acknowledge();
      }
      
      if (error)
      {
         try
         {
            session.start(xid, flags);
            fail("Should throw exception");
         }
         catch (XAException e)
         {
            assertEquals(XAException.XAER_RMERR, e.errorCode);
         }
      }
      else
      {
         session.start(xid, flags);
      }
      
      EasyMock.verify(conn, rc);          
   }
   
   private void testXASetTransactionTimeout(boolean error) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      final int timeout = 1897217;
      
      SessionXASetTimeoutMessage packet = new SessionXASetTimeoutMessage(timeout);
      
      SessionXASetTimeoutResponseMessage resp = new SessionXASetTimeoutResponseMessage(!error);
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      boolean ok = session.setTransactionTimeout(timeout);
      
      assertTrue(ok == !error);
      
      EasyMock.verify(conn, rc);  
   }
   
   private void testXARollback(boolean error) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      Xid xid = randomXid();
      
      SessionXARollbackMessage packet = new SessionXARollbackMessage(xid);
      
      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      if (error)
      {
         try
         {
            session.rollback(xid);
            fail("Should throw exception");
         }
         catch (XAException e)
         {
            assertEquals(XAException.XAER_RMERR, e.errorCode);
         }
      }
      else
      {
         session.rollback(xid);
      }
      
      EasyMock.verify(conn, rc);  
   }
   
   private void testXARecover(final int flags) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      final Xid[] xids = new Xid[] { randomXid(), randomXid(), randomXid() } ;
                  
      if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN)
      {
         EmptyPacket packet = new EmptyPacket(EmptyPacket.SESS_XA_INDOUBT_XIDS);
         
         SessionXAGetInDoubtXidsResponseMessage resp = new SessionXAGetInDoubtXidsResponseMessage(Arrays.asList(xids));
         
         EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
      }
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      Xid[] xids2 = session.recover(flags);
      
      if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN)
      {
         assertEquals(xids.length, xids2.length);
         
         for (int i = 0; i < xids.length; i++)
         {
            assertEquals(xids[i], xids2[i]);
         }
      }
      else
      {
         assertTrue(xids2.length == 0);
      }
      
      EasyMock.verify(conn, rc);  
   }
   
   private void testXAPrepare(boolean error, boolean readOnly) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      Xid xid = randomXid();
      
      SessionXAPrepareMessage packet = new SessionXAPrepareMessage(xid);
      
      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, error ? XAException.XAER_RMERR : readOnly ? XAResource.XA_RDONLY : XAResource.XA_OK, "blah");
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      if (error)
      {
         try
         {
            session.prepare(xid);
            fail("Should throw exception");
         }
         catch (XAException e)
         {
            assertEquals(XAException.XAER_RMERR, e.errorCode);
         }
      }
      else
      {
         int res = session.prepare(xid);
         
         if (readOnly)
         {
            assertEquals(XAResource.XA_RDONLY, res);
         }
         else
         {
            assertEquals(XAResource.XA_OK, res);
         }
      }
      
      EasyMock.verify(conn, rc);  
   }
   
   private void testXAForget(final boolean error) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      Xid xid = randomXid();
      
      Packet packet = new SessionXAForgetMessage(xid);

      final int numMessages = 10;
      
      SessionAcknowledgeMessage msg = new SessionAcknowledgeMessage(numMessages - 1, true);
      
      rc.sendOneWay(sessionTargetID, sessionTargetID, msg);
      
      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      //Simulate some unflushed messages
      
      for (int i = 0; i < numMessages; i++)
      {
         session.delivered(i, false);
         session.acknowledge();
      }
      
      if (error)
      {
         try
         {
            session.forget(xid);
            fail("Should throw exception");
         }
         catch (XAException e)
         {
            assertEquals(XAException.XAER_RMERR, e.errorCode);
         }
      }
      else
      {
         session.forget(xid);
      }
      
      EasyMock.verify(conn, rc);           
   }
   
   private void testXAEnd(int flags, boolean error) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      Xid xid = randomXid();
      
      Packet packet = null;
      if (flags == XAResource.TMSUSPEND)
      {
         packet = new EmptyPacket(EmptyPacket.SESS_XA_SUSPEND);                  
      }
      else if (flags == XAResource.TMSUCCESS)
      {
         packet = new SessionXAEndMessage(xid, false);
      }
      else if (flags == XAResource.TMFAIL)
      {
         packet = new SessionXAEndMessage(xid, true);
      }

      final int numMessages = 10;
      
      SessionAcknowledgeMessage msg = new SessionAcknowledgeMessage(numMessages - 1, true);
      
      rc.sendOneWay(sessionTargetID, sessionTargetID, msg);
      
      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      //Simulate some unflushed messages
      
      for (int i = 0; i < numMessages; i++)
      {
         session.delivered(i, false);
         session.acknowledge();
      }
      
      if (error)
      {
         try
         {
            session.end(xid, flags);
            fail("Should throw exception");
         }
         catch (XAException e)
         {
            assertEquals(XAException.XAER_RMERR, e.errorCode);
         }
      }
      else
      {
         session.end(xid, flags);
      }
      
      EasyMock.verify(conn, rc);          
   }
   
   private void testXACommit(boolean onePhase, boolean error) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
      
      Xid xid = randomXid();
      
      SessionXACommitMessage packet = new SessionXACommitMessage(xid, onePhase);
      
      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, packet)).andReturn(resp);
                       
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, true, -1, false, false, false, false);
      
      if (error)
      {
         try
         {
            session.commit(xid, onePhase);
            fail("Should throw exception");
         }
         catch (XAException e)
         {
            assertEquals(XAException.XAER_RMERR, e.errorCode);
         }
      }
      else
      {
         session.commit(xid, onePhase);
      }
      
      EasyMock.verify(conn, rc);  
   }
   
   private void testAddRemoveConsumer(boolean delivered) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
          
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
        
      final long sessionTargetID = 9121892;
                  
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      EasyMock.verify(conn, rc);
      
      EasyMock.reset(conn, rc);
         
      final int numDeliveries = 10;
      
      if (delivered)
      {
         SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(numDeliveries - 1, true);
         
         rc.sendOneWay(sessionTargetID, sessionTargetID, message);
      }
            
      rc.sendOneWay(sessionTargetID, sessionTargetID, new SessionCancelMessage(-1, false));
      
      rc.sendOneWay(sessionTargetID, sessionTargetID, new SessionCancelMessage(-1, false));
                       
      EasyMock.replay(conn, rc);
                       
      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      
      session.addConsumer(cons1);
      session.addConsumer(cons2);
      
      assertEquals(2, session.getConsumers().size());
      assertTrue(session.getConsumers().contains(cons1));
      assertTrue(session.getConsumers().contains(cons2));
      
      if (delivered)
      {
         //Simulate there being some undelivered messages
         for (int i = 0; i < numDeliveries; i++)
         {
            session.delivered(i, false);
            session.acknowledge();
         }
      }
            
      session.removeConsumer(cons1);
      
      assertEquals(1, session.getConsumers().size());
      assertTrue(session.getConsumers().contains(cons2));  
      
      session.removeConsumer(cons2);
      assertEquals(0, session.getConsumers().size());
      
      EasyMock.verify(conn, rc);          
   }
   
   private void testAutoCommitSessionAcknowledge(boolean blockOnAcknowledge) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      final int numMessages = 100;
      
      final int batchSize = 10;
            
      final int sessionTargetID = 71267162;
            
      for (int i = 0; i < numMessages / batchSize; i++)
      {
         SessionAcknowledgeMessage message = new SessionAcknowledgeMessage((i + 1) * batchSize - 1, true);
               
         if (blockOnAcknowledge)
         {
            EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, message)).andReturn(null);
         }
         else
         {
            rc.sendOneWay(sessionTargetID, sessionTargetID, message);
         }
      }

      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.SESS_COMMIT))).andReturn(null);
      
      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      
      for (int i = 0; i < numMessages / batchSize; i++)
      {
         SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(numMessages + (i + 1) * batchSize - 1, true);
               
         if (blockOnAcknowledge)
         {
            EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, message)).andReturn(null);
         }
         else
         {
            rc.sendOneWay(sessionTargetID, sessionTargetID, message);
         }
      }
      
      cons1.recover(numMessages * 2);
      cons2.recover(numMessages * 2);
            
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.SESS_ROLLBACK))).andReturn(null);
                                     
      EasyMock.replay(conn, rc, pd, cons1, cons2);
            
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, batchSize, false, false, true, blockOnAcknowledge);
      
      session.addConsumer(cons1);
      
      session.addConsumer(cons2);
      
      //Simulate some messages being delivered in a non broken sequence (i.e. what would happen with a single consumer
      //on the session)
            
      for (int i = 0; i < numMessages; i++)
      {
         session.delivered(i, false);
         
         session.acknowledge();
      }
      
      //Then commit
      session.commit();
      
      for (int i = numMessages; i < numMessages * 2; i++)
      {
         session.delivered(i, false);
         
         session.acknowledge();
      }
      
      session.rollback();
      
      EasyMock.verify(conn, rc, pd, cons1, cons2);
   }
   
   private void testTransactedSessionAcknowledgeBroken(boolean blockOnAcknowledge) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      final int[] messages = new int[] { 1, 3, 5, 7, 9, 2, 4, 10, 20, 21, 22, 23, 19, 18, 15, 30, 31, 32, 40, 35 };
      
      final int sessionTargetID = 71267162;
         
      for (int i = 0; i < messages.length; i++)
      {
         SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(messages[i], false);
         
         if (blockOnAcknowledge)
         {
            EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, message)).andReturn(null);
         }
         else
         {
            rc.sendOneWay(sessionTargetID, sessionTargetID, message);
         }
      }
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.SESS_COMMIT))).andReturn(null);
      
      final int[] messages2 = new int[] { 43, 44, 50, 47, 48, 60, 45, 61, 62, 64 };
      
      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      
      for (int i = 0; i < messages2.length; i++)
      {
         SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(messages2[i], false);
         
         if (blockOnAcknowledge)
         {
            EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, message)).andReturn(null);
         }
         else
         {
            rc.sendOneWay(sessionTargetID, sessionTargetID, message);
         }
      }
      
      //Recover back to the last committed
      cons1.recover(36);
      cons2.recover(36);
            
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, new EmptyPacket(EmptyPacket.SESS_ROLLBACK))).andReturn(null);
                        
      EasyMock.replay(conn, rc, pd, cons1, cons2);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, blockOnAcknowledge);
      
      session.addConsumer(cons1);
      session.addConsumer(cons2);
      
      //Simulate some messages being delivered in a broken sequence (i.e. what would happen with a single consumer
      //on the session)
            
      for (int i = 0; i < messages.length; i++)
      {
         session.delivered(messages[i], false);
         
         session.acknowledge();
      }
      
      //Then commit
      session.commit();
      
      for (int i = 0; i < messages2.length; i++)
      {
         session.delivered(messages2[i], false);
         
         session.acknowledge();
      }
      
      session.rollback();
      
      EasyMock.verify(conn, rc, pd, cons1, cons2); 
   }
   
   private void testCreateBrowser(boolean filter) throws Exception
   {
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
          
      final long sessionTargetID = 7617622;      
      final SimpleString queueName = new SimpleString("gyugg");
      final SimpleString sfilter = filter ? new SimpleString("ygyg") : null;
      
      SessionCreateBrowserMessage request =
         new SessionCreateBrowserMessage(queueName, sfilter);             
      
      SessionCreateBrowserResponseMessage resp = 
         new SessionCreateBrowserResponseMessage(76675765);
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);
      
      EasyMock.replay(conn, rc);
      
      ClientSessionInternal session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, false, false, false);
      
      if (filter)
      {
         ClientBrowser browser = session.createBrowser(queueName, sfilter);
      }
      else
      {
         ClientBrowser browser = session.createBrowser(queueName);
      }
      
      EasyMock.verify(conn, rc);
   }
   
   private void testCreateProducerWithWindowSizeMethod(final SimpleString address,
         final int windowSize, final int initialCredits,
         final int serverMaxRate,
         final boolean blockOnNPSend,
         final boolean blockOnPSend,
         final boolean autoCommitSends) throws Exception
   {
      ClientConnectionFactory cf = EasyMock.createStrictMock(ClientConnectionFactory.class);

      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);

      // In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);

      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);

      // Defaults from cf

      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);

      EasyMock.expect(cf.isDefaultBlockOnNonPersistentSend()).andReturn(blockOnNPSend);

      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);

      EasyMock.expect(cf.isDefaultBlockOnPersistentSend()).andReturn(blockOnPSend);   

      final long clientTargetID = 7676876;

      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);

      final long sessionTargetID = 9121892;

      SessionCreateProducerMessage request =
         new SessionCreateProducerMessage(clientTargetID, address, windowSize, -1);             

      SessionCreateProducerResponseMessage resp = 
         new SessionCreateProducerResponseMessage(67765765, initialCredits, serverMaxRate);

      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);

      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);      
      
      pd.register(new ClientProducerPacketHandler(null, clientTargetID));

      EasyMock.replay(cf);
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);

      ClientSession session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, autoCommitSends, false, false);

      ClientProducerInternal producer = (ClientProducerInternal)session.createProducerWithWindowSize(address, windowSize);

      EasyMock.verify(cf);
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd);

      assertEquals(address, producer.getAddress());
      assertEquals(autoCommitSends && blockOnNPSend, producer.isBlockOnNonPersistentSend());
      assertEquals(autoCommitSends && blockOnPSend, producer.isBlockOnPersistentSend());
      assertEquals(initialCredits, producer.getInitialWindowSize());
      assertEquals(serverMaxRate, producer.getMaxRate());
   }
   
   private void testCreateProducerRateLimitedMethod(final SimpleString address,
                                                    final int maxRate, final int initialCredits,
                                                    final int serverMaxRate,
                                                    final boolean blockOnNPSend,
                                                    final boolean blockOnPSend,
                                                    final boolean autoCommitSends) throws Exception
   {
      ClientConnectionFactory cf = EasyMock.createStrictMock(ClientConnectionFactory.class);

      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);

      // In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);

      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      // Defaults from cf
        
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.isDefaultBlockOnNonPersistentSend()).andReturn(blockOnNPSend);
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.isDefaultBlockOnPersistentSend()).andReturn(blockOnPSend);   

      final long clientTargetID = 7676876;

      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);

      final long sessionTargetID = 9121892;

      SessionCreateProducerMessage request =
         new SessionCreateProducerMessage(clientTargetID, address, -1, maxRate);             

      SessionCreateProducerResponseMessage resp = 
         new SessionCreateProducerResponseMessage(67765765, initialCredits, serverMaxRate);

      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);

      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);

      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
            
      pd.register(new ClientProducerPacketHandler(null, clientTargetID));

      EasyMock.replay(cf);
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);

      ClientSession session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, autoCommitSends, false, false);

      ClientProducerInternal producer = (ClientProducerInternal)session.createRateLimitedProducer(address, maxRate);

      EasyMock.verify(cf);
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd);

      assertEquals(address, producer.getAddress());
      assertEquals(autoCommitSends && blockOnNPSend, producer.isBlockOnNonPersistentSend());
      assertEquals(autoCommitSends && blockOnPSend, producer.isBlockOnPersistentSend());
      assertEquals(initialCredits, producer.getInitialWindowSize());
      assertEquals(serverMaxRate, producer.getMaxRate());
   }
   
   private void testCreateProducerBasicMethod(final SimpleString address, final int windowSize,
         final int maxRate, final int initialCredits,
         final int serverMaxRate,
         final boolean blockOnNPSend,
         final boolean blockOnPSend,
         final boolean autoCommitSends) throws Exception
   {
      ClientConnectionFactory cf = EasyMock.createStrictMock(ClientConnectionFactory.class);

      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);

      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);

      // In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);

      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      // Defaults from cf
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.getDefaultProducerWindowSize()).andReturn(windowSize);
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.getDefaultProducerMaxRate()).andReturn(maxRate);   
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.isDefaultBlockOnNonPersistentSend()).andReturn(blockOnNPSend);
      
      EasyMock.expect(conn.getConnectionFactory()).andReturn(cf);
      
      EasyMock.expect(cf.isDefaultBlockOnPersistentSend()).andReturn(blockOnPSend);   

      final long clientTargetID = 7676876;

      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);

      final long sessionTargetID = 9121892;

      SessionCreateProducerMessage request =
         new SessionCreateProducerMessage(clientTargetID, address, windowSize, maxRate);             

      SessionCreateProducerResponseMessage resp = 
         new SessionCreateProducerResponseMessage(67765765, initialCredits, serverMaxRate);

      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);

      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      pd.register(new ClientProducerPacketHandler(null, clientTargetID));

      EasyMock.replay(cf);
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);

      ClientSession session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, autoCommitSends, false, false);

      ClientProducerInternal producer = (ClientProducerInternal)session.createProducer(address);

      EasyMock.verify(cf);
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd);

      assertEquals(address, producer.getAddress());
      assertEquals(autoCommitSends && blockOnNPSend, producer.isBlockOnNonPersistentSend());
      assertEquals(autoCommitSends && blockOnPSend, producer.isBlockOnPersistentSend());
      assertEquals(initialCredits, producer.getInitialWindowSize());
      assertEquals(serverMaxRate, producer.getMaxRate());
   }
   
   private void testCreateProducerWideMethod(final SimpleString address, final int windowSize,
                                             final int maxRate, final int initialCredits,
                                             final int serverMaxRate,
                                             final boolean blockOnNPSend,
                                             final boolean blockOnPSend,
                                             final boolean autoCommitSends) throws Exception
   {
      ClientConnectionFactory cf = EasyMock.createStrictMock(ClientConnectionFactory.class);
      
      ClientConnectionInternal conn = EasyMock.createStrictMock(ClientConnectionInternal.class);
           
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
           
      //In ClientSessionImpl constructor
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
                        
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      final long clientTargetID = 7676876;
      
      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
      
      final long sessionTargetID = 9121892;
      
      SessionCreateProducerMessage request =
         new SessionCreateProducerMessage(clientTargetID, address, windowSize, maxRate);             
      
      SessionCreateProducerResponseMessage resp = 
         new SessionCreateProducerResponseMessage(67765765, initialCredits, serverMaxRate);
      
      EasyMock.expect(rc.sendBlocking(sessionTargetID, sessionTargetID, request)).andReturn(resp);
      
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(pd);
      
      EasyMock.expect(conn.getRemotingConnection()).andReturn(rc);
      
      pd.register(new ClientProducerPacketHandler(null, clientTargetID));
      
      EasyMock.replay(cf);
      EasyMock.replay(conn);
      EasyMock.replay(rc);
      EasyMock.replay(pd);
      
      ClientSession session = new ClientSessionImpl(conn, sessionTargetID, false, -1, false, autoCommitSends, false, false);

      ClientProducerInternal producer = (ClientProducerInternal)session.createProducer(address, windowSize, maxRate, blockOnNPSend, blockOnPSend);
      
      EasyMock.verify(cf);
      EasyMock.verify(conn);
      EasyMock.verify(rc);
      EasyMock.verify(pd);
      
      assertEquals(address, producer.getAddress());
      assertEquals(autoCommitSends && blockOnNPSend, producer.isBlockOnNonPersistentSend());
      assertEquals(autoCommitSends && blockOnPSend, producer.isBlockOnPersistentSend());
      assertEquals(initialCredits, producer.getInitialWindowSize());
      assertEquals(serverMaxRate, producer.getMaxRate());
   }
   
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

      ClientSessionInternal session = new ClientSessionImpl(conn, serverTargetID, xa,
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

