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

package org.jboss.messaging.tests.unit.core.client.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A ClientSessionFactoryImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientSessionFactoryImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientSessionFactoryImplTest.class);
   
   public void testDummy()
   {      
   }
   
//   public void testWideConstructor() throws Exception
//   {
//      final ConnectorFactory cf = EasyMock.createMock(ConnectorFactory.class);
//      Map<String, Object> params = new HashMap<String, Object>();
//      
//      testCreateClientSessionFactory(cf, params, 12321, 123123,
//            32342, 1254, 152454, 15454, false, false, false);
//      
//      testCreateClientSessionFactory(cf, params, 12542, 1625,
//            65465, 5454, 6544, 654654, true, true, true);      
//   }
//   
//   public void testLocationOnlyConstructor() throws Exception
//   {
//      final ConnectorFactory cf = EasyMock.createMock(ConnectorFactory.class);
//      
//      final ClientSessionFactory sf = new ClientSessionFactoryImpl(cf);
//      
//      assertTrue(cf == sf.getConnectorFactory());
//
//      checkDefaults(sf);
//   }
//    
//   public void testGetSetAttributes() throws Exception
//   {
//      final ConnectorFactory cf = EasyMock.createMock(ConnectorFactory.class);
//      
//      final ClientSessionFactory sf = new ClientSessionFactoryImpl(cf);
//      
//      Map<String, Object> params = new HashMap<String, Object>();
//      
//      checkGetSetAttributes(sf, cf, params, 123, 767, 12312, 1231, 23424, 123213, false, false, false);
//      checkGetSetAttributes(sf, cf, params, 1726, 134, 656, 3453, 4343, 6556, true, true, true);      
//   }   
//   
//   public void testCreateSession() throws Throwable
//   {
//      testCreateSessionWithUsernameAndPassword(null, null);
//   }
//   
//   public void testCreateSessionWithUsernameAndPassword() throws Throwable
//   {
//      testCreateSessionWithUsernameAndPassword("bob", "wibble");
//   }   
//   
//   // Private -----------------------------------------------------------------------------------------------------------
//      
//   private void testCreateSessionWithUsernameAndPassword(final String username, final String password) throws Throwable
//   {
//      ConnectionRegistry cr = EasyMock.createStrictMock(ConnectionRegistry.class);
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
//      
//      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
//      
//      ConnectorFactory cof = EasyMock.createStrictMock(ConnectorFactory.class);
//      
//      Map<String, Object> params = new HashMap<String, Object>();
//       
//      final long pingPeriod = 61123;
//      final long callTimeout = 2763;
//      
//      ClientSessionFactoryImpl cf =
//         new ClientSessionFactoryImpl(cof, params,
//               pingPeriod, callTimeout,
//               32432, 4323,
//               453453, 54543, false,
//               false, false);
//      
//      cf.setConnectionRegistry(cr);
//            
//      EasyMock.expect(cr.getConnection(cof, params, pingPeriod, callTimeout)).andReturn(rc);
//      
//      EasyMock.expect(rc.getPacketDispatcher()).andStubReturn(dispatcher);
//      long commandResponseTargetID = 1201922;
//      EasyMock.expect(dispatcher.generateID()).andReturn(commandResponseTargetID);
//      
//      boolean xa = RandomUtil.randomBoolean();
//      boolean autoCommitSends = RandomUtil.randomBoolean();
//      boolean autoCommitAcks = RandomUtil.randomBoolean();
//      int lazyAckBatchSize = 123;     
//      boolean cacheProducers = RandomUtil.randomBoolean();
//      int confirmationBatchSize = RandomUtil.randomInt();
//              
//      Version serverVersion = new VersionImpl("blah", 1, 1, 1, 12, "blah");
//      
//      CreateSessionResponseMessage response = 
//         new CreateSessionResponseMessage(124312, 16226, serverVersion.getIncrementingVersion(), confirmationBatchSize);
//      
//      EasyMock.expect(rc.sendBlocking(EasyMock.eq(PacketDispatcherImpl.RESERVED_HANDLER_ID),
//                                      EasyMock.eq(PacketDispatcherImpl.RESERVED_HANDLER_ID),
//                                      EasyMock.isA(CreateSessionMessage.class),
//                                      (CommandManager)EasyMock.isNull())).andReturn(response);
//      
//      dispatcher.register(EasyMock.isA(CommandManagerImpl.class));
//           
//      EasyMock.replay(cr, rc, dispatcher);
//      
//      ClientSession sess;
//      
//      if (username == null)         
//      {
//         sess = cf.createSession(xa, autoCommitSends, autoCommitAcks, lazyAckBatchSize, 
//                  cacheProducers);
//      }
//      else
//      {
//         sess = cf.createSession(username, password, xa, autoCommitSends, autoCommitAcks, lazyAckBatchSize,
//                                 cacheProducers);
//      }
//         
//      EasyMock.verify(cr, rc, dispatcher);
//
//      assertNotNull(sess.getName());
//   }
//   
//   
//   private void testCreateClientSessionFactory(final ConnectorFactory cf, final Map<String, Object> params,
//            final long pingPeriod, final long callTimeout,
//         final int defaultConsumerWindowSize, final int defaultConsumerMaxRate,
//         final int defaultProducerWindowSize, final int defaultProducerMaxRate,
//         final boolean defaultBlockOnAcknowledge,
//         final boolean defaultSendNonPersistentMessagesBlocking,
//         final boolean defaultSendPersistentMessagesBlocking) throws Exception
//   {
//      ClientSessionFactory sf =
//         new ClientSessionFactoryImpl(cf, params, pingPeriod, callTimeout, defaultConsumerWindowSize, defaultConsumerMaxRate,
//               defaultProducerWindowSize, defaultProducerMaxRate, defaultBlockOnAcknowledge,
//               defaultSendNonPersistentMessagesBlocking, defaultSendPersistentMessagesBlocking);
//      
//      assertTrue(cf == sf.getConnectorFactory());
//      assertTrue(params == sf.getTransportParams());
//      assertEquals(pingPeriod, sf.getPingPeriod());
//      assertEquals(callTimeout, sf.getCallTimeout());
//      assertEquals(defaultConsumerWindowSize, sf.getConsumerWindowSize());
//      assertEquals(defaultConsumerMaxRate, sf.getConsumerMaxRate());
//      assertEquals(defaultProducerWindowSize, sf.getProducerWindowSize());
//      assertEquals(defaultProducerMaxRate, sf.getProducerMaxRate());
//      assertEquals(defaultBlockOnAcknowledge, sf.isBlockOnAcknowledge());
//      assertEquals(defaultSendNonPersistentMessagesBlocking, sf.isBlockOnNonPersistentSend());
//      assertEquals(defaultSendPersistentMessagesBlocking, sf.isBlockOnPersistentSend());
//   }
//   
//   private void checkDefaults(final ClientSessionFactory cf) throws Exception
//   {
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_PING_PERIOD, cf.getPingPeriod());
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT, cf.getCallTimeout());
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE, cf.getConsumerWindowSize());
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE, cf.getConsumerMaxRate());
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE, cf.getProducerWindowSize());
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE, cf.getProducerMaxRate());
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE, cf.isBlockOnAcknowledge());
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND, cf.isBlockOnNonPersistentSend());
//      assertEquals(ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND, cf.isBlockOnPersistentSend());      
//   }
//   
//   private void checkGetSetAttributes(ClientSessionFactory sf,
//         final ConnectorFactory cf, final Map<String, Object> params,
//         final long pingPeriod, final long callTimeout,
//         final int defaultConsumerWindowSize, final int defaultConsumerMaxRate,
//         final int defaultProducerWindowSize, final int defaultProducerMaxRate,
//         final boolean defaultBlockOnAcknowledge,
//         final boolean defaultBlockOnPersistentSend,
//         final boolean defaultBlockOnNonPersistentSend)
//   {
//      sf.setConnectorFactory(cf);
//      assertTrue(cf == sf.getConnectorFactory());
//      sf.setTransportParams(params);
//      assertTrue(params == sf.getTransportParams());
//      sf.setPingPeriod(pingPeriod);
//      assertEquals(pingPeriod, sf.getPingPeriod());
//      sf.setCallTimeout(callTimeout);
//      assertEquals(callTimeout, sf.getCallTimeout());
//      sf.setConsumerWindowSize(defaultConsumerWindowSize);
//      assertEquals(defaultConsumerWindowSize, sf.getConsumerWindowSize());
//      sf.setConsumerMaxRate(defaultConsumerMaxRate);
//      assertEquals(defaultConsumerMaxRate, sf.getConsumerMaxRate());
//      sf.setProducerWindowSize(defaultProducerWindowSize);
//      assertEquals(defaultProducerWindowSize, sf.getProducerWindowSize());
//      sf.setProducerMaxRate(defaultProducerMaxRate);
//      assertEquals(defaultProducerMaxRate, sf.getProducerMaxRate());
//      sf.setBlockOnAcknowledge(defaultBlockOnAcknowledge);
//      assertEquals(defaultBlockOnAcknowledge, sf.isBlockOnAcknowledge());
//      sf.setBlockOnPersistentSend(defaultBlockOnPersistentSend);
//      assertEquals(defaultBlockOnPersistentSend, sf.isBlockOnPersistentSend());
//      sf.setBlockOnNonPersistentSend(defaultBlockOnNonPersistentSend);
//      assertEquals(defaultBlockOnNonPersistentSend, sf.isBlockOnNonPersistentSend());
//   }
   
}
