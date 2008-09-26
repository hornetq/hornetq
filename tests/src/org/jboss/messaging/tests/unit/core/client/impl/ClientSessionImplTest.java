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
 * A ClientSessionImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientSessionImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientSessionImplTest.class);

   // Public -----------------------------------------------------------------------------------------------------------

   public void testDummy()
   {      
   }
   
//   public void testConstructor() throws Exception
//   {            
//      testConstructor(132, true, 10, true, true, true, true, 100);
//      testConstructor(132, false, 10, false, false, false, false, 12);
//   }
//
//   public void testConstructorInvalidArgs() throws Exception
//   {  
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      Executor executor = EasyMock.createStrictMock(Executor.class);
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class); 
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      try
//      {
//         new ClientSessionImpl("blah", 1, false, -2, false, false, false, false, rc, cf, pd, 100, cm);
//         fail("Should throw exception");
//      }
//      catch (IllegalArgumentException e)
//      {
//         //Ok
//      }
//
//      try
//      {
//         new ClientSessionImpl("blah", 1, false, -10, false, false, false, false, rc, cf, pd, 100, cm);
//         fail("Should throw exception");
//      }
//      catch (IllegalArgumentException e)
//      {
//         //Ok
//      }
//
//      try
//      {
//         new ClientSessionImpl("blah", 1, false, 0, false, false, false, false, rc, cf, pd, 100, cm);
//         fail("Should throw exception");
//      }
//      catch (IllegalArgumentException e)
//      {
//         //Ok
//      }
//   }
//
//   public void testCreateQueue() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//  
//      SessionCreateQueueMessage request = new SessionCreateQueueMessage(new SimpleString("blah"), new SimpleString("hagshg"),
//            new SimpleString("jhjhs"), false, false);
//      
//      final int targetID = 121;
//      
//      EasyMock.expect(cm.sendCommandBlocking(targetID, request)).andReturn(null);
//      
//      EasyMock.replay(rc, cf, pd);
//
//      ClientSession session =
//         new ClientSessionImpl("blah", targetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//                  
//      session.createQueue(request.getAddress(), request.getQueueName(), request.getFilterString(), request.isDurable(), request.isDurable());
//      
//      EasyMock.verify(rc, cf, pd);     
//   }
//   
//   public void testDeleteQueue() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      SessionDeleteQueueMessage request = new SessionDeleteQueueMessage(new SimpleString("blah"));
//      
//      final int targetID = 121;
//      
//      EasyMock.expect(cm.sendCommandBlocking(targetID, request)).andReturn(null);
//      
//      EasyMock.replay(rc, cf, pd, cm);
//                  
//      ClientSession session = new ClientSessionImpl("blah", targetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//                  
//      session.deleteQueue(request.getQueueName());
//      
//      EasyMock.verify(rc, cf, pd, cm);     
//   }
//   
//   public void testQueueQuery() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);    
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      SessionQueueQueryMessage request = new SessionQueueQueryMessage(new SimpleString("blah"));
//      
//      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage();
//      
//      final int targetID = 121;
//      
//      EasyMock.expect(cm.sendCommandBlocking(targetID, request)).andReturn(resp);
//      
//      EasyMock.replay(rc, cf, pd, cm);
//                  
//      ClientSession session = new ClientSessionImpl("blah", targetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//                  
//      SessionQueueQueryResponseMessage resp2 = session.queueQuery(request.getQueueName());
//      
//      EasyMock.verify(rc, rc, cf, pd, cm);
//      
//      assertTrue(resp == resp2);
//   }
//   
//   public void testBindingQuery() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//
//      SessionBindingQueryMessage request = new SessionBindingQueryMessage(new SimpleString("blah"));
//      
//      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage();
//      
//      final int targetID = 121;
//      
//      EasyMock.expect(cm.sendCommandBlocking(targetID, request)).andReturn(resp);
//      
//      EasyMock.replay(rc, cf, pd, cm);
//                  
//      ClientSession session = new ClientSessionImpl("blah", targetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//                  
//      SessionBindingQueryResponseMessage resp2 = session.bindingQuery(request.getAddress());
//      
//      EasyMock.verify(rc, cf, pd, cm); 
//      
//      assertTrue(resp == resp2);
//   }
//   
//   public void testAddDestination() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      SessionAddDestinationMessage request = new SessionAddDestinationMessage(new SimpleString("blah"), true, true);
//      
//      final int targetID = 121;
//      
//      EasyMock.expect(cm.sendCommandBlocking(targetID,  request)).andReturn(null);
//      
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSession session = new ClientSessionImpl("blah", targetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//                  
//      session.addDestination(request.getAddress(), request.isDurable(), request.isTemporary());
//      
//      EasyMock.verify(rc, cf, pd, cm); 
//   }
//   
//   public void testRemoveDestination() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      SessionRemoveDestinationMessage request = new SessionRemoveDestinationMessage(new SimpleString("blah"), true);
//      
//      final int targetID = 121;
//      
//      EasyMock.expect(cm.sendCommandBlocking(targetID, request)).andReturn(null);
//      
//      EasyMock.replay(rc, cf, pd, cm);
//                  
//      ClientSession session = new ClientSessionImpl("blah", targetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//                  
//      session.removeDestination(request.getAddress(), true);
//      
//      EasyMock.verify(rc, cf, pd, cm); 
//   }
//   
//   public void testCreateConsumer() throws Exception
//   {
//      //First test with the wide createConsumer method
//      
//      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), new SimpleString("ygyggyg"),
//            false, false, false, 121455, 76556, 121455);
//      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), null,
//            false, false, false, 121455, 76556, 121455);
//      
//      //test where server window size overrides client window size
//      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), null,
//            false, false, false, 121455, 76556, 675675765);
//      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), null,
//            false, false, false, 121455, 76556, 1);
//      testCreateConsumerWideMethod(new SimpleString("usahduiahs"), null,
//            false, false, false, 121455, 76556, -1);
//      
//      //And with the method that takes defaults from the cf
//      
//      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), new SimpleString("ygyggyg"),
//            false, false, false, 121455, 76556, 121455);
//      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), null,
//            false, false, false, 121455, 76556, 121455);
//      
//      //test where server window size overrides client window size
//      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), null,
//            false, false, false, 121455, 76556, 675675765);
//      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), null,
//            false, false, false, 121455, 76556, 1);
//      testCreateConsumerDefaultsMethod(new SimpleString("usahduiahs"), null,
//            false, false, false, 121455, 76556, -1);
//      
//      // And with the basic createConsumer method:
//      
//      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, 121455);
//      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, 121455);
//      
//      //test where server window size overrides client window size
//      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, 675675765);
//      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, 1);
//      testCreateConsumerBasicMethod(new SimpleString("usahduiahs"), 121455, 76556, -1);
//   }
//   
//   public void testCreateProducer() throws Exception
//   {
//      //test with the wide method
//      
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, false, false);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, false, true);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, true, false);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, true, true);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, false, false);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, false, true);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, true, false);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, true, true);
//      
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, false, false);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, false, true);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, true, false);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, true, true);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, false, false);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, false, true);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, true, false);
//      testCreateProducerWideMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, true, true);
//      
//      //Test with the basic method
//      
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, false, false);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, false, true);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, true, false);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, false, true, true);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, false, false);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, false, true);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, true, false);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 545454, 5454, true, true, true);
//      
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, false, false);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, false, true);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, true, false);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, false, true, true);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, false, false);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, false, true);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, true, false);
//      testCreateProducerBasicMethod(new SimpleString("yugygugy"), 545454, 5454, 675765, 3232, true, true, true);
//
//      //Test with the rate limited method
//      
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, false, false, false);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, false, false, true);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, false, true, false);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, false, true, true);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, true, false, false);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, true, false, true);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, true, true, false);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, -1, 5454, true, true, true);
//      
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, false, false);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, false, true);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, true, false);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, true, true);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, false, false);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, false, true);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, true, false);
//      testCreateProducerRateLimitedMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, true, true);
//      
//      //Test with the create producer with window size method
//      
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, false, false, false);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, false, false, true);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, false, true, false);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, false, true, true);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, true, false, false);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, true, false, true);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, true, true, false);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 545454, -1, true, true, true);
//      
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, false, false);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, false, true);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, true, false);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, false, true, true);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, false, false);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, false, true);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, true, false);
//      testCreateProducerWithWindowSizeMethod(new SimpleString("yugygugy"), 5454, 675765, 3232, true, true, true);      
//   }
//   
//   public void testProducerCaching() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//      
//      final SimpleString address1 = new SimpleString("gyugg");
//      final SimpleString address2 = new SimpleString("g237429834");
//      final int windowSize = 72887827;
//      final int maxRate = -1;
//      
//      //In create producer method
//            
//      {
//         final long clientTargetID = 7676876;
//         
//         EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//                  
//         SessionCreateProducerMessage request =
//            new SessionCreateProducerMessage(clientTargetID, address1, windowSize, maxRate);             
//         
//         SessionCreateProducerResponseMessage resp = 
//            new SessionCreateProducerResponseMessage(67765765, windowSize, maxRate);
//         
//         EasyMock.expect(cm.sendCommandBlocking(sessionTargetID,  request)).andReturn(resp);
//               
//         pd.register(new ClientSessionPacketHandler(null, clientTargetID, null));
//      }
//      
//      {  
//         final long clientTargetID = 54654654;
//         
//         EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//         
//         SessionCreateProducerMessage request =
//            new SessionCreateProducerMessage(clientTargetID, address2, windowSize, maxRate);             
//         
//         SessionCreateProducerResponseMessage resp = 
//            new SessionCreateProducerResponseMessage(7676876, windowSize, maxRate);
//         
//         EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//         
//         pd.register(new ClientSessionPacketHandler(null, clientTargetID, null));
//      }
//      
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      //Create three with address1 - only one should be actually created
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, true, false, false, false, rc, cf, pd, 100, cm);
//      
//      assertEquals(0, session.getProducerCache().size());
//      
//      ClientProducerInternal producer1 = (ClientProducerInternal)session.createProducer(address1, windowSize, maxRate,
//                                                                                        false, false);
//      
//      assertEquals(0, session.getProducerCache().size());
//      
//      session.removeProducer(producer1);  
//      
//      assertEquals(1, session.getProducerCache().size());
//      
//      ClientProducerInternal producer2 = (ClientProducerInternal)session.createProducer(address1, windowSize, maxRate,
//                                                                                       false, false);      
//      
//      assertEquals(0, session.getProducerCache().size());
//      
//      session.removeProducer(producer2);
//      
//      assertEquals(1, session.getProducerCache().size());
//      
//      ClientProducerInternal producer3 = (ClientProducerInternal)session.createProducer(address1, windowSize, maxRate,
//                                                                                        false, false);
//      
//      assertEquals(0, session.getProducerCache().size());
//      
//      session.removeProducer(producer3);
//      
//      assertEquals(1, session.getProducerCache().size());
//      
//      //Create another with a different address
//      
//      ClientProducerInternal producer4 = (ClientProducerInternal)session.createProducer(address2, windowSize, maxRate,
//                                                                                        false, false);
//      
//      assertEquals(1, session.getProducerCache().size());
//      
//      session.removeProducer(producer4); 
//      
//      assertEquals(2, session.getProducerCache().size());
//            
//      EasyMock.verify(rc, cf, pd, cm);
//      
//      assertTrue(producer1 == producer2);
//      assertTrue(producer2 == producer3);
//      assertFalse(producer1 == producer4);
//      assertFalse(producer2 == producer4);
//      assertFalse(producer3 == producer4);      
//   }
//   
//   public void testProducerNoCaching() throws Exception
//   {       
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);     
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//          
//      final long sessionTargetID = 7617622;      
//      final SimpleString address = new SimpleString("gyugg");
//      final int windowSize = 72887827;
//      final int maxRate = -1;
//
//      for (int i = 0; i < 3; i++)
//      {
//         //In create producer method
//          
//         final long clientTargetID = i + 65655;
//         
//         EasyMock.expect(pd.generateID()).andReturn(clientTargetID);         
//         
//         SessionCreateProducerMessage request =
//            new SessionCreateProducerMessage(clientTargetID, address, windowSize, maxRate);             
//         
//         SessionCreateProducerResponseMessage resp = 
//            new SessionCreateProducerResponseMessage(i + 273263, windowSize, maxRate);
//         
//         EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//                    
//         pd.register(new ClientSessionPacketHandler(null, clientTargetID, null));      
//      }
//
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      ClientProducerInternal producer1 = (ClientProducerInternal)session.createProducer(address, windowSize, maxRate,
//                                                                                        false, false);
//      session.removeProducer(producer1);  
//      
//      ClientProducerInternal producer2 = (ClientProducerInternal)session.createProducer(address, windowSize, maxRate,
//                                                                                       false, false);      
//      session.removeProducer(producer2);
//      
//      ClientProducerInternal producer3 = (ClientProducerInternal)session.createProducer(address, windowSize, maxRate,
//                                                                                        false, false);
//      session.removeProducer(producer3);
//      
//      EasyMock.verify(rc, cf, pd, cm);
//      
//      assertFalse(producer1 == producer2);
//      assertFalse(producer2 == producer3);
//      assertFalse(producer1 == producer3);
//   }
//   
//   public void testCreateBrowser() throws Exception
//   {
//      testCreateBrowser(true);
//      testCreateBrowser(false);
//   }
//         
//   public void testGetXAResource() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      ClientSession session = new ClientSessionImpl("blah", 5465, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      XAResource res = session.getXAResource();
//      
//      assertTrue(res == session);
//   }
//   
//   public void testTransactedSessionAcknowledgeNotBroken() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//   
//      final int numMessages = 100;
//      
//      final int sessionTargetID = 71267162;
//            
//      SessionAcknowledgeMessageBlah message = new SessionAcknowledgeMessageBlah(numMessages - 1, true);
//            
//      cm.sendCommandOneway(sessionTargetID, message);
//
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.SESS_COMMIT))).andReturn(null);
//      
//      //Create some consumers
//      
//      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      
//      cons1.recover(numMessages);
//      cons2.recover(numMessages);
//            
//      SessionAcknowledgeMessageBlah message2 = new SessionAcknowledgeMessageBlah(numMessages * 2 - 1, true);
//      
//      cm.sendCommandOneway(sessionTargetID, message2);
//
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.SESS_ROLLBACK))).andReturn(null);
//                  
//      EasyMock.replay(rc, cf, pd, cons1, cons2, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      session.addConsumer(cons1);
//      session.addConsumer(cons2);
//      
//      //Simulate some messages being delivered in a non broken sequence (i.e. what would happen with a single consumer
//      //on the session)
//            
//      for (int i = 0; i < numMessages; i++)
//      {
//         session.delivered(i, false);
//         
//         session.acknowledge();
//      }
//      
//      //Then commit
//      session.commit();
//      
//      for (int i = numMessages; i < numMessages * 2; i++)
//      {
//         session.delivered(i, false);
//         
//         session.acknowledge();
//      }
//      
//      session.rollback();
//      
//      EasyMock.verify(rc, cf, pd, cons1, cons2, cm);
//   }
//   
//   public void testAutoCommitSessionAcknowledge() throws Exception
//   {
//      testAutoCommitSessionAcknowledge(true);
//      testAutoCommitSessionAcknowledge(false);
//   }
//            
//   public void testTransactedSessionAcknowledgeBroken() throws Exception
//   {
//      testTransactedSessionAcknowledgeBroken(true);
//      testTransactedSessionAcknowledgeBroken(false);
//   }
//         
//   public void testTransactedSessionAcknowledgeNotBrokenExpired() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      final int[] messages = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
//      
//      final int sessionTargetID = 71267162;
//         
//      for (int i = 0; i < messages.length; i++)
//      {
//         SessionCancelMessage message = new SessionCancelMessage(messages[i], true);
//         
//         cm.sendCommandOneway(sessionTargetID, message);
//      }
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.SESS_COMMIT))).andReturn(null);
//                  
//      EasyMock.replay(rc, cf, pd, cm);
//
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      //Simulate some messages being delivered in a non broken sequence (i.e. what would happen with a single consumer
//      //on the session)
//            
//      for (int i = 0; i < messages.length; i++)
//      {
//         session.delivered(messages[i], true);
//         
//         session.acknowledge();
//      }
//      
//      //Then commit
//      session.commit();
//      
//      EasyMock.verify(rc, cf, pd, cm);
//   }
//   
//   public void testTransactedSessionAcknowledgeBrokenExpired() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      final int[] messages = new int[] { 1, 3, 5, 7, 9, 2, 4, 10, 20, 21, 22, 23, 19, 18, 15, 30, 31, 32, 40, 35 };
//      
//      final int sessionTargetID = 71267162;
//         
//      for (int i = 0; i < messages.length; i++)
//      {
//         SessionCancelMessage message = new SessionCancelMessage(messages[i], true);
//         
//         cm.sendCommandOneway(sessionTargetID, message);
//      }
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.SESS_COMMIT))).andReturn(null);
//                  
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      //Simulate some messages being delivered in a broken sequence (i.e. what would happen with a single consumer
//      //on the session)
//            
//      for (int i = 0; i < messages.length; i++)
//      {
//         session.delivered(messages[i], true);
//         
//         session.acknowledge();
//      }
//      
//      //Then commit
//      session.commit();
//      
//      EasyMock.verify(rc, cf, pd, cm);; 
//   }
//
//   public void testCleanUp2() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      ConnectionRegistry reg = EasyMock.createStrictMock(ConnectionRegistry.class);
//        
//      final long sessionTargetID = 9121892;
//                  
//      EasyMock.replay(rc, cf, pd, cm, reg);
//      
//      ClientSessionImpl session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      session.setConnectionRegistry(reg);
//      
//      EasyMock.verify(rc, cf, pd, cm, reg);
//      
//      EasyMock.reset(rc, cf, pd, cm, reg);
//      
//      ClientProducerInternal prod1 = EasyMock.createStrictMock(ClientProducerInternal.class);
//      ClientProducerInternal prod2 = EasyMock.createStrictMock(ClientProducerInternal.class);
//      
//      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      
//      ClientBrowser browser1 = EasyMock.createStrictMock(ClientBrowser.class);
//      ClientBrowser browser2 = EasyMock.createStrictMock(ClientBrowser.class);
//                    
//      prod1.cleanUp();
//      prod2.cleanUp();
//      cons1.cleanUp();
//      cons2.cleanUp();
//      browser1.cleanUp();
//      browser2.cleanUp();
//      
//      cm.close();
//      final String connectionID = "uahsjash";
//      EasyMock.expect(rc.getID()).andStubReturn(connectionID);
//      reg.returnConnection(connectionID);
//        
//      EasyMock.replay(cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm, reg, rc);
//                 
//      session.addProducer(prod1);
//      session.addProducer(prod2);
//      
//      session.addConsumer(cons1);
//      session.addConsumer(cons2);
//      
//      session.addBrowser(browser1);
//      session.addBrowser(browser2);
//      
//      assertFalse(session.isClosed());
//      
//      session.cleanUp();
//
//      assertTrue(session.isClosed());
//      
//      EasyMock.verify(rc, cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm, reg, rc);      
//   }
//         
//   public void testClose() throws Exception   
//   {
//      testClose(true);
//      testClose(false);
//   }
//         
//   public void testAddRemoveConsumer() throws Exception
//   {
//      testAddRemoveConsumer(true);
//      testAddRemoveConsumer(false);
//   }
//   
//   public void testAddRemoveProducer() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//                  
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      EasyMock.verify(rc, cf, pd, cm);
//      
//      EasyMock.reset(rc, cf, pd, cm);
//                                      
//      ClientProducerInternal prod1 = EasyMock.createStrictMock(ClientProducerInternal.class);
//      ClientProducerInternal prod2 = EasyMock.createStrictMock(ClientProducerInternal.class);
//      
//      session.addProducer(prod1);
//      session.addProducer(prod2);
//      
//      assertEquals(2, session.getProducers().size());
//      assertTrue(session.getProducers().contains(prod1));
//      assertTrue(session.getProducers().contains(prod2));
//       
//      session.removeProducer(prod1);
//      
//      assertEquals(1, session.getProducers().size());
//      assertTrue(session.getProducers().contains(prod2));   
//      
//      session.removeProducer(prod2);
//      
//      assertEquals(0, session.getProducers().size()); 
//   }
//   
//   public void testAddRemoveBrowser() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//                  
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      EasyMock.verify(rc, cf, pd, cm);
//      
//      EasyMock.reset(rc, cf, pd, cm);
//                                      
//      ClientBrowser browser1 = EasyMock.createStrictMock(ClientBrowser.class);
//      ClientBrowser browser2 = EasyMock.createStrictMock(ClientBrowser.class);
//      
//      session.addBrowser(browser1);
//      session.addBrowser(browser2);
//      
//      assertEquals(2, session.getBrowsers().size());
//      assertTrue(session.getBrowsers().contains(browser1));
//      assertTrue(session.getBrowsers().contains(browser2));
//       
//      session.removeBrowser(browser1);
//      
//      assertEquals(1, session.getBrowsers().size());
//      assertTrue(session.getBrowsers().contains(browser2));   
//      
//      session.removeBrowser(browser2);
//      
//      assertEquals(0, session.getBrowsers().size()); 
//   }
//   
//   public void testXACommit() throws Exception
//   {
//      testXACommit(false, false);
//      testXACommit(false, true);
//      testXACommit(true, false);
//      testXACommit(true, true);
//   }
//   
//   public void testXAEnd() throws Exception
//   {
//      testXAEnd(XAResource.TMSUSPEND, false);
//      testXAEnd(XAResource.TMSUSPEND, true);
//      testXAEnd(XAResource.TMSUCCESS, false);
//      testXAEnd(XAResource.TMSUCCESS, true);
//      testXAEnd(XAResource.TMFAIL, false);
//      testXAEnd(XAResource.TMFAIL, true);
//   }
//   
//   public void testXAForget() throws Exception
//   {
//      testXAForget(false);
//      testXAForget(true);
//   }
//   
//   public void testGetTransactionTimeout() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//      
//      Packet packet = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
//
//      final int timeout = 1098289;
//      
//      SessionXAGetTimeoutResponseMessage resp = new SessionXAGetTimeoutResponseMessage(timeout);
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      int timeout2 = session.getTransactionTimeout();
//            
//      EasyMock.verify(rc, cf, pd, cm);  
//      
//      assertEquals(timeout, timeout2);
//   }
//   
//   public void testIsSameRM() throws Exception
//   {      
//      RemotingConnection rc1 = EasyMock.createStrictMock(RemotingConnection.class);
//      RemotingConnection rc2 = EasyMock.createStrictMock(RemotingConnection.class);
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//                               
//      EasyMock.replay(rc1, rc2, cf, pd, cm);
//      
//      ClientSessionInternal session1 =
//         new ClientSessionImpl("blah", 4343, true, -1, false, false, false, false, rc1, cf, pd, 100, cm);
//      
//      ClientSessionInternal session2 =
//         new ClientSessionImpl("blah2", 4343, true, -1, false, false, false, false, rc2, cf, pd, 100, cm);
//      
//      ClientSessionInternal session3 =
//         new ClientSessionImpl("blah3", 4343, true, -1, false, false, false, false, rc2, cf, pd, 100, cm);
//      
//      assertFalse(session1.isSameRM(session2));
//      assertFalse(session2.isSameRM(session1));
//      
//      assertTrue(session2.isSameRM(session3));
//      assertTrue(session3.isSameRM(session2));
//      
//      assertFalse(session1.isSameRM(session3));
//      assertFalse(session3.isSameRM(session1));
//      
//      assertTrue(session1.isSameRM(session1));
//      assertTrue(session2.isSameRM(session2));
//      assertTrue(session3.isSameRM(session3));
//      
//      EasyMock.verify(rc1, rc2, cf, pd, cm);
//   }
//   
//   public void testXAPrepare() throws Exception
//   {
//      testXAPrepare(false, false);
//      testXAPrepare(false, true);
//      testXAPrepare(true, false);
//      testXAPrepare(true, true);
//   }
//   
//   public void testXARecover() throws Exception
//   {
//      testXARecover(XAResource.TMNOFLAGS);
//      testXARecover(XAResource.TMSTARTRSCAN);
//      testXARecover(XAResource.TMENDRSCAN);
//      testXARecover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
//   }
//   
//   public void testXARollback() throws Exception
//   {
//      testXARollback(true);
//      testXARollback(false);
//   }
//   
//   public void testXASetTransactionTimeout() throws Exception
//   {
//      testXASetTransactionTimeout(false);
//      testXASetTransactionTimeout(true);
//   }
//   
//   public void testXAStart() throws Exception
//   {
//      testXAStart(XAResource.TMJOIN, false);
//      testXAStart(XAResource.TMRESUME, false);
//      testXAStart(XAResource.TMNOFLAGS, false);
//      testXAStart(XAResource.TMJOIN, true);
//      testXAStart(XAResource.TMRESUME, true);
//      testXAStart(XAResource.TMNOFLAGS, true);
//   }
//
//   public void testCleanUp1() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      ConnectionRegistry reg = EasyMock.createStrictMock(ConnectionRegistry.class);
//
//      SessionCreateQueueMessage request = new SessionCreateQueueMessage(new SimpleString("blah"), new SimpleString("hagshg"),
//            new SimpleString("jhjhs"), false, false);
//
//      final int targetID = 121;
//
//      EasyMock.expect(cm.sendCommandBlocking(targetID, request)).andReturn(null);
//
//      EasyMock.replay(rc, cf, pd, cm);
//
//      ClientSessionImpl session = new ClientSessionImpl("blah", targetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      session.setConnectionRegistry(reg);
//
//      session.createQueue(request.getAddress(), request.getQueueName(), request.getFilterString(), request.isDurable(), request.isDurable());
//
//      EasyMock.verify(rc, cf, pd, cm);
//
//      EasyMock.reset(rc, cf, pd, cm);   
//      cm.close();
//      final String connectionID = "uahsjash";
//      EasyMock.expect(rc.getID()).andStubReturn(connectionID);
//      reg.returnConnection(connectionID);
//      EasyMock.replay(rc, cf, pd, cm);
//      session.cleanUp();
//      EasyMock.verify(rc, cf, pd, cm);
//   }
//
//   public void notXA() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//              
//      final long sessionTargetID = 9121892;
//                  
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      EasyMock.verify(rc, cf, pd, cm);
//      
//      EasyMock.reset(rc, cf, pd, cm);      
//      try
//      {
//         session.commit(randomXid(), false);
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.end(randomXid(), 8778);
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.forget(randomXid());
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.getTransactionTimeout();
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.isSameRM(session);
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.prepare(randomXid());
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.recover(89787);
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.rollback(randomXid());
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.setTransactionTimeout(767);
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//      
//      try
//      {
//         session.start(randomXid(), 8768);
//         fail("Should throw exception");
//      }
//      catch (XAException e)
//      {
//         assertEquals(XAException.XAER_RMERR, e.errorCode);
//      }
//   }
//   
//   public void testCreateMessage() throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      MessagingBuffer buff = EasyMock.createMock(MessagingBuffer.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      EasyMock.expect(rc.createBuffer(ClientSessionImpl.INITIAL_MESSAGE_BODY_SIZE)).andStubReturn(buff);      
//      EasyMock.replay(rc);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", 453543, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      ClientMessage msg = session.createClientMessage(false);
//      assertFalse(msg.isDurable());
//      
//      msg = session.createClientMessage(true);
//      assertTrue(msg.isDurable());
//      
//      final byte type = 123;
//      
//      msg = session.createClientMessage(type, false);
//      assertFalse(msg.isDurable());
//      assertEquals(type, msg.getType());
//      
//      msg = session.createClientMessage(type, true);
//      assertTrue(msg.isDurable());
//      assertEquals(type, msg.getType());
//            
//      final long expiration = 120912902;
//      final long timestamp = 1029128;
//      final byte priority = 12;
//            
//      msg = session.createClientMessage(type, false, expiration, timestamp, priority);
//      assertFalse(msg.isDurable());
//      assertEquals(type, msg.getType());
//      assertEquals(expiration, msg.getExpiration());
//      assertEquals(timestamp, msg.getTimestamp());
//      assertEquals(priority, msg.getPriority());
//
//      msg = session.createClientMessage(type, true, expiration, timestamp, priority);
//      assertTrue(msg.isDurable());
//      assertEquals(type, msg.getType());
//      assertEquals(expiration, msg.getExpiration());
//      assertEquals(timestamp, msg.getTimestamp());
//      assertEquals(priority, msg.getPriority());
//   }
//   
//   // Private -------------------------------------------------------------------------------------------
//
//   private void testClose(boolean delivered) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      ConnectionRegistry reg = EasyMock.createStrictMock(ConnectionRegistry.class);
//        
//      final long sessionTargetID = 9121892;
//                  
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionImpl session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      session.setConnectionRegistry(reg);
//      
//      EasyMock.verify(rc, cf, pd, cm);
//      
//      EasyMock.reset(rc, cf, pd, cm);
//      
//      ClientProducerInternal prod1 = EasyMock.createStrictMock(ClientProducerInternal.class);
//      ClientProducerInternal prod2 = EasyMock.createStrictMock(ClientProducerInternal.class);
//      
//      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      
//      ClientBrowser browser1 = EasyMock.createStrictMock(ClientBrowser.class);
//      ClientBrowser browser2 = EasyMock.createStrictMock(ClientBrowser.class);
//                    
//      prod1.close();
//      prod2.close();
//      cons1.close();
//      cons2.close();
//      browser1.close();
//      browser2.close();
//      
//      final int numDeliveries = 10;
//      
//      if (delivered)
//      {
//         SessionAcknowledgeMessageBlah message = new SessionAcknowledgeMessageBlah(numDeliveries - 1, true);
//         
//         cm.sendCommandOneway(sessionTargetID, message);
//      }
//            
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
//      
//      cm.close();
//                 
//      final String connectionID = "uahsjash";
//      EasyMock.expect(rc.getID()).andStubReturn(connectionID);
//      reg.returnConnection(connectionID);
//             
//      EasyMock.replay(rc, cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm);
//                 
//      session.addProducer(prod1);
//      session.addProducer(prod2);
//      
//      session.addConsumer(cons1);
//      session.addConsumer(cons2);
//      
//      session.addBrowser(browser1);
//      session.addBrowser(browser2);
//      
//      assertFalse(session.isClosed());
//      
//      if (delivered)
//      {
//         //Simulate there being some undelivered messages
//         for (int i = 0; i < numDeliveries; i++)
//         {
//            session.delivered(i, false);
//            session.acknowledge();
//         }
//      }
//            
//      session.close();      
//      
//      EasyMock.verify(rc, cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm);
//      
//      assertTrue(session.isClosed());  
//      
//      EasyMock.reset(rc, cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm);
//      
//      EasyMock.replay(rc, cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm);
//      
//      //Close again should do nothing
//      
//      session.close();
//      
//      EasyMock.verify(rc, cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm);
//      
//      try
//      {
//         session.createQueue(new SimpleString("trtr"), new SimpleString("iuasij"), null, false, false);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.deleteQueue(new SimpleString("trtr"));
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.addDestination(new SimpleString("trtr"), false, false);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.removeDestination(new SimpleString("trtr"), false);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.queueQuery(new SimpleString("trtr"));
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.bindingQuery(new SimpleString("trtr"));
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createConsumer(new SimpleString("trtr"));
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createConsumer(new SimpleString("iasjq"), new SimpleString("iuahsdiaj"), false);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createConsumer(new SimpleString("iasjq"), new SimpleString("iuahsdiaj"), false, 123412, 7162761);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createBrowser(new SimpleString("husuhsuh"));
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createBrowser(new SimpleString("husuhsuh"), null);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createProducer(new SimpleString("husuhsuh"));
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createProducer(new SimpleString("iashi"), 878778, 8778, false, false);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createRateLimitedProducer(new SimpleString("uhsuhs"), 78676);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.createProducerWithWindowSize(new SimpleString("uhsuhs"), 78676);
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.commit();
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.rollback();
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//      try
//      {
//         session.acknowledge();
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
//      }
//      
//   }
//   
//   private void testXAStart(int flags, boolean error) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//      
//      Xid xid = randomXid();
//      
//      Packet packet = null;
//      if (flags == XAResource.TMJOIN)
//      {
//         packet = new SessionXAJoinMessage(xid);           
//      }
//      else if (flags == XAResource.TMRESUME)
//      {
//         packet = new SessionXAResumeMessage(xid);
//      }
//      else if (flags == XAResource.TMNOFLAGS)
//      {
//         packet = new SessionXAStartMessage(xid);
//      }
//
//      final int numMessages = 10;
//      
//      if (flags != XAResource.TMNOFLAGS)
//      {      
//         SessionAcknowledgeMessageBlah msg = new SessionAcknowledgeMessageBlah(numMessages - 1, true);
//         
//         cm.sendCommandOneway(sessionTargetID, msg);
//      }
//      
//      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      //Simulate some unflushed messages
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         session.delivered(i, false);
//         session.acknowledge();
//      }
//      
//      if (error)
//      {
//         try
//         {
//            session.start(xid, flags);
//            fail("Should throw exception");
//         }
//         catch (XAException e)
//         {
//            assertEquals(XAException.XAER_RMERR, e.errorCode);
//         }
//      }
//      else
//      {
//         session.start(xid, flags);
//      }
//      
//      EasyMock.verify(rc, cf, pd, cm);          
//   }
//   
//   private void testXASetTransactionTimeout(boolean error) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);      
//        
//      final long sessionTargetID = 9121892;
//      
//      final int timeout = 1897217;
//      
//      SessionXASetTimeoutMessage packet = new SessionXASetTimeoutMessage(timeout);
//      
//      SessionXASetTimeoutResponseMessage resp = new SessionXASetTimeoutResponseMessage(!error);
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      boolean ok = session.setTransactionTimeout(timeout);
//      
//      assertTrue(ok == !error);
//      
//      EasyMock.verify(rc, cf, pd, cm);  
//   }
//   
//   private void testXARollback(boolean error) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//           
//      final long sessionTargetID = 9121892;
//      
//      Xid xid = randomXid();
//      
//      SessionXARollbackMessage packet = new SessionXARollbackMessage(xid);
//      
//      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      if (error)
//      {
//         try
//         {
//            session.rollback(xid);
//            fail("Should throw exception");
//         }
//         catch (XAException e)
//         {
//            assertEquals(XAException.XAER_RMERR, e.errorCode);
//         }
//      }
//      else
//      {
//         session.rollback(xid);
//      }
//      
//      EasyMock.verify(rc, cf, pd, cm);  
//   }
//   
//   private void testXARecover(final int flags) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      final long sessionTargetID = 9121892;
//      
//      final Xid[] xids = new Xid[] { randomXid(), randomXid(), randomXid() } ;
//                  
//      if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN)
//      {
//         PacketImpl packet = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
//         
//         SessionXAGetInDoubtXidsResponseMessage resp = new SessionXAGetInDoubtXidsResponseMessage(Arrays.asList(xids));
//         
//         EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//      }
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      Xid[] xids2 = session.recover(flags);
//      
//      if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN)
//      {
//         assertEquals(xids.length, xids2.length);
//         
//         for (int i = 0; i < xids.length; i++)
//         {
//            assertEquals(xids[i], xids2[i]);
//         }
//      }
//      else
//      {
//         assertTrue(xids2.length == 0);
//      }
//      
//      EasyMock.verify(rc, cf, pd, cm);  
//   }
//   
//   private void testXAPrepare(boolean error, boolean readOnly) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//      
//      Xid xid = randomXid();
//      
//      SessionXAPrepareMessage packet = new SessionXAPrepareMessage(xid);
//      
//      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, error ? XAException.XAER_RMERR : readOnly ? XAResource.XA_RDONLY : XAResource.XA_OK, "blah");
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      if (error)
//      {
//         try
//         {
//            session.prepare(xid);
//            fail("Should throw exception");
//         }
//         catch (XAException e)
//         {
//            assertEquals(XAException.XAER_RMERR, e.errorCode);
//         }
//      }
//      else
//      {
//         int res = session.prepare(xid);
//         
//         if (readOnly)
//         {
//            assertEquals(XAResource.XA_RDONLY, res);
//         }
//         else
//         {
//            assertEquals(XAResource.XA_OK, res);
//         }
//      }
//      
//      EasyMock.verify(rc, cf, pd, cm);  
//   }
//   
//   private void testXAForget(final boolean error) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//      
//      Xid xid = randomXid();
//      
//      Packet packet = new SessionXAForgetMessage(xid);
//
//      final int numMessages = 10;
//      
//      SessionAcknowledgeMessageBlah msg = new SessionAcknowledgeMessageBlah(numMessages - 1, true);
//      
//      cm.sendCommandOneway(sessionTargetID, msg);
//      
//      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
//      
//      //Simulate some unflushed messages
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         session.delivered(i, false);
//         session.acknowledge();
//      }
//      
//      if (error)
//      {
//         try
//         {
//            session.forget(xid);
//            fail("Should throw exception");
//         }
//         catch (XAException e)
//         {
//            assertEquals(XAException.XAER_RMERR, e.errorCode);
//         }
//      }
//      else
//      {
//         session.forget(xid);
//      }
//      
//      EasyMock.verify(rc, cf, pd, cm);           
//   }
//   
//   private void testXAEnd(int flags, boolean error) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//      
//      Xid xid = randomXid();
//      
//      Packet packet = null;
//      if (flags == XAResource.TMSUSPEND)
//      {
//         packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);                  
//      }
//      else if (flags == XAResource.TMSUCCESS)
//      {
//         packet = new SessionXAEndMessage(xid, false);
//      }
//      else if (flags == XAResource.TMFAIL)
//      {
//         packet = new SessionXAEndMessage(xid, true);
//      }
//
//      final int numMessages = 10;
//      
//      SessionAcknowledgeMessageBlah msg = new SessionAcknowledgeMessageBlah(numMessages - 1, true);
//      
//      cm.sendCommandOneway(sessionTargetID, msg);
//      
//      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false,rc, cf, pd, 100, cm);
//      
//      //Simulate some unflushed messages
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         session.delivered(i, false);
//         session.acknowledge();
//      }
//      
//      if (error)
//      {
//         try
//         {
//            session.end(xid, flags);
//            fail("Should throw exception");
//         }
//         catch (XAException e)
//         {
//            assertEquals(XAException.XAER_RMERR, e.errorCode);
//         }
//      }
//      else
//      {
//         session.end(xid, flags);
//      }
//      
//      EasyMock.verify(rc, cf, pd, cm);          
//   }
//   
//   private void testXACommit(boolean onePhase, boolean error) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//      
//      Xid xid = randomXid();
//      
//      SessionXACommitMessage packet = new SessionXACommitMessage(xid, onePhase);
//      
//      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false,rc, cf, pd, 100, cm);
//      
//      if (error)
//      {
//         try
//         {
//            session.commit(xid, onePhase);
//            fail("Should throw exception");
//         }
//         catch (XAException e)
//         {
//            assertEquals(XAException.XAER_RMERR, e.errorCode);
//         }
//      }
//      else
//      {
//         session.commit(xid, onePhase);
//      }
//      
//      EasyMock.verify(rc, cf, pd, cm);  
//   }
//   
//   private void testAddRemoveConsumer(boolean delivered) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//        
//      final long sessionTargetID = 9121892;
//                  
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false,rc, cf, pd, 100, cm);
//      
//      EasyMock.verify(rc, cf, pd, cm);
//      
//      EasyMock.reset(rc, cf, pd, cm);
//         
//      final int numDeliveries = 10;
//      
//      if (delivered)
//      {
//         SessionAcknowledgeMessageBlah message = new SessionAcknowledgeMessageBlah(numDeliveries - 1, true);
//         
//         cm.sendCommandOneway(sessionTargetID, message);
//      }
//            
//      cm.sendCommandOneway(sessionTargetID, new SessionCancelMessage(-1, false));
//      
//      cm.sendCommandOneway(sessionTargetID, new SessionCancelMessage(-1, false));
//                       
//      EasyMock.replay(rc, cf, pd, cm);
//                       
//      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      
//      session.addConsumer(cons1);
//      session.addConsumer(cons2);
//      
//      assertEquals(2, session.getConsumers().size());
//      assertTrue(session.getConsumers().contains(cons1));
//      assertTrue(session.getConsumers().contains(cons2));
//      
//      if (delivered)
//      {
//         //Simulate there being some undelivered messages
//         for (int i = 0; i < numDeliveries; i++)
//         {
//            session.delivered(i, false);
//            session.acknowledge();
//         }
//      }
//            
//      session.removeConsumer(cons1);
//      
//      assertEquals(1, session.getConsumers().size());
//      assertTrue(session.getConsumers().contains(cons2));  
//      
//      session.removeConsumer(cons2);
//      assertEquals(0, session.getConsumers().size());
//      
//      EasyMock.verify(rc, cf, pd, cm);          
//   }
//   
//   private void testAutoCommitSessionAcknowledge(boolean blockOnAcknowledge) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      final int numMessages = 100;
//      
//      final int batchSize = 10;
//            
//      final int sessionTargetID = 71267162;
//            
//      for (int i = 0; i < numMessages / batchSize; i++)
//      {
//         SessionAcknowledgeMessageBlah message = new SessionAcknowledgeMessageBlah((i + 1) * batchSize - 1, true);
//               
//         if (blockOnAcknowledge)
//         {
//            EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, message)).andReturn(null);
//         }
//         else
//         {
//            cm.sendCommandOneway(sessionTargetID, message);
//         }
//      }
//
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.SESS_COMMIT))).andReturn(null);
//      
//      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      
//      for (int i = 0; i < numMessages / batchSize; i++)
//      {
//         SessionAcknowledgeMessageBlah message = new SessionAcknowledgeMessageBlah(numMessages + (i + 1) * batchSize - 1, true);
//                   
//         if (blockOnAcknowledge)
//         {
//            EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, message)).andReturn(null);
//         }
//         else
//         {
//            cm.sendCommandOneway(sessionTargetID, message);
//         }
//      }
//      
//      cons1.recover(numMessages * 2);
//      cons2.recover(numMessages * 2);
//            
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.SESS_ROLLBACK))).andReturn(null);
//                                     
//      EasyMock.replay(rc, cf, pd, cons1, cons2, cm);
//            
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, batchSize, false, false, true, blockOnAcknowledge,rc, cf, pd, 100, cm);
//      
//      session.addConsumer(cons1);
//      
//      session.addConsumer(cons2);
//      
//      //Simulate some messages being delivered in a non broken sequence (i.e. what would happen with a single consumer
//      //on the session)
//            
//      for (int i = 0; i < numMessages; i++)
//      {
//         session.delivered(i, false);
//         
//         session.acknowledge();
//      }
//      
//      //Then commit
//      session.commit();
//      
//      for (int i = numMessages; i < numMessages * 2; i++)
//      {
//         session.delivered(i, false);
//         
//         session.acknowledge();
//      }
//      
//      session.rollback();
//      
//      EasyMock.verify(rc, cf, pd, cons1, cons2, cm);
//   }
//   
//   private void testTransactedSessionAcknowledgeBroken(boolean blockOnAcknowledge) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      final int[] messages = new int[] { 1, 3, 5, 7, 9, 2, 4, 10, 20, 21, 22, 23, 19, 18, 15, 30, 31, 32, 40, 35 };
//      
//      final int sessionTargetID = 71267162;
//         
//      for (int i = 0; i < messages.length; i++)
//      {
//         SessionAcknowledgeMessageBlah message = new SessionAcknowledgeMessageBlah(messages[i], false);
//         
//         if (blockOnAcknowledge)
//         {
//            EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, message)).andReturn(null);
//         }
//         else
//         {
//            cm.sendCommandOneway(sessionTargetID, message);
//         }
//      }
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.SESS_COMMIT))).andReturn(null);
//      
//      final int[] messages2 = new int[] { 43, 44, 50, 47, 48, 60, 45, 61, 62, 64 };
//      
//      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
//      
//      for (int i = 0; i < messages2.length; i++)
//      {
//         SessionAcknowledgeMessageBlah message = new SessionAcknowledgeMessageBlah(messages2[i], false);
//         
//         if (blockOnAcknowledge)
//         {
//            EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, message)).andReturn(null);
//         }
//         else
//         {
//            cm.sendCommandOneway(sessionTargetID, message);
//         }
//      }
//      
//      //Recover back to the last committed
//      cons1.recover(36);
//      cons2.recover(36);
//            
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, new PacketImpl(PacketImpl.SESS_ROLLBACK))).andReturn(null);
//                        
//      EasyMock.replay(rc, cf, pd, cons1, cons2, cm);
//      
//      ClientSessionInternal session =
//         new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, blockOnAcknowledge,rc, cf, pd, 100, cm);
//      
//      session.addConsumer(cons1);
//      session.addConsumer(cons2);
//      
//      //Simulate some messages being delivered in a broken sequence (i.e. what would happen with a single consumer
//      //on the session)
//            
//      for (int i = 0; i < messages.length; i++)
//      {
//         session.delivered(messages[i], false);
//         
//         session.acknowledge();
//      }
//      
//      //Then commit
//      session.commit();
//      
//      for (int i = 0; i < messages2.length; i++)
//      {
//         session.delivered(messages2[i], false);
//         
//         session.acknowledge();
//      }
//      
//      session.rollback();
//      
//      EasyMock.verify(rc, cf, pd, cons1, cons2, cm); 
//   }
//   
//   private void testCreateBrowser(boolean filter) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//          
//      final long sessionTargetID = 7617622;      
//      final SimpleString queueName = new SimpleString("gyugg");
//      final SimpleString sfilter = filter ? new SimpleString("ygyg") : null;
//      
//      SessionCreateBrowserMessage request =
//         new SessionCreateBrowserMessage(queueName, sfilter);             
//      
//      SessionCreateBrowserResponseMessage resp = 
//         new SessionCreateBrowserResponseMessage(76675765);
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//      
//      EasyMock.replay(rc, cf, pd, cm);
//      
//      ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false,rc, cf, pd, 100, cm);
//      
//      if (filter)
//      {
//         ClientBrowser browser = session.createBrowser(queueName, sfilter);
//      }
//      else
//      {
//         ClientBrowser browser = session.createBrowser(queueName);
//      }
//      
//      EasyMock.verify(rc, cf, pd, cm);
//   }
//   
//   private void testCreateProducerWithWindowSizeMethod(final SimpleString address,
//         final int windowSize, final int initialCredits,
//         final int serverMaxRate,
//         final boolean blockOnNPSend,
//         final boolean blockOnPSend,
//         final boolean autoCommitSends) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//
//      // Defaults from cf
//
//      EasyMock.expect(cf.isBlockOnNonPersistentSend()).andReturn(blockOnNPSend);
//
//      EasyMock.expect(cf.isBlockOnPersistentSend()).andReturn(blockOnPSend);   
//
//      final long clientTargetID = 7676876;
//
//      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//
//      final long sessionTargetID = 9121892;
//
//      SessionCreateProducerMessage request =
//         new SessionCreateProducerMessage(clientTargetID, address, windowSize, -1);             
//
//      SessionCreateProducerResponseMessage resp = 
//         new SessionCreateProducerResponseMessage(67765765, initialCredits, serverMaxRate);
//
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//
//
//      pd.register(new ClientSessionPacketHandler(null, clientTargetID, null));
//
//      EasyMock.replay(cf, rc, pd, cm);
//
//      ClientSession session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, autoCommitSends, false, false,rc, cf, pd, 100, cm);
//
//      ClientProducerInternal producer = (ClientProducerInternal)session.createProducerWithWindowSize(address, windowSize);
//
//      EasyMock.verify(cf, rc, pd, cm);   
//
//      assertEquals(address, producer.getAddress());
//      assertEquals(autoCommitSends && blockOnNPSend, producer.isBlockOnNonPersistentSend());
//      assertEquals(autoCommitSends && blockOnPSend, producer.isBlockOnPersistentSend());
//      assertEquals(initialCredits, producer.getInitialWindowSize());
//      assertEquals(serverMaxRate, producer.getMaxRate());
//   }
//   
//   private void testCreateProducerRateLimitedMethod(final SimpleString address,
//                                                    final int maxRate, final int initialCredits,
//                                                    final int serverMaxRate,
//                                                    final boolean blockOnNPSend,
//                                                    final boolean blockOnPSend,
//                                                    final boolean autoCommitSends) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      // Defaults from cf
//        
//      EasyMock.expect(cf.isBlockOnNonPersistentSend()).andReturn(blockOnNPSend);
//      
//      EasyMock.expect(cf.isBlockOnPersistentSend()).andReturn(blockOnPSend);   
//
//      final long clientTargetID = 7676876;
//
//      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//
//      final long sessionTargetID = 9121892;
//
//      SessionCreateProducerMessage request =
//         new SessionCreateProducerMessage(clientTargetID, address, -1, maxRate);             
//
//      SessionCreateProducerResponseMessage resp = 
//         new SessionCreateProducerResponseMessage(67765765, initialCredits, serverMaxRate);
//
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//        
//      pd.register(new ClientSessionPacketHandler(null, clientTargetID, null));
//
//      EasyMock.replay(cf, rc, pd, cm);
//
//      ClientSession session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, autoCommitSends, false, false,rc, cf, pd, 100, cm);
//
//      ClientProducerInternal producer = (ClientProducerInternal)session.createRateLimitedProducer(address, maxRate);
//
//      EasyMock.verify(cf, rc, pd, cm);
//
//      assertEquals(address, producer.getAddress());
//      assertEquals(autoCommitSends && blockOnNPSend, producer.isBlockOnNonPersistentSend());
//      assertEquals(autoCommitSends && blockOnPSend, producer.isBlockOnPersistentSend());
//      assertEquals(initialCredits, producer.getInitialWindowSize());
//      assertEquals(serverMaxRate, producer.getMaxRate());
//   }
//   
//   private void testCreateProducerBasicMethod(final SimpleString address, final int windowSize,
//         final int maxRate, final int initialCredits,
//         final int serverMaxRate,
//         final boolean blockOnNPSend,
//         final boolean blockOnPSend,
//         final boolean autoCommitSends) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      // Defaults from cf
//      
//      EasyMock.expect(cf.getProducerWindowSize()).andReturn(windowSize);
//      
//      EasyMock.expect(cf.getProducerMaxRate()).andReturn(maxRate);   
//      
//      EasyMock.expect(cf.isBlockOnNonPersistentSend()).andReturn(blockOnNPSend);
//        
//      EasyMock.expect(cf.isBlockOnPersistentSend()).andReturn(blockOnPSend);   
//
//      final long clientTargetID = 7676876;
//
//      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//
//      final long sessionTargetID = 9121892;
//
//      SessionCreateProducerMessage request =
//         new SessionCreateProducerMessage(clientTargetID, address, windowSize, maxRate);             
//
//      SessionCreateProducerResponseMessage resp = 
//         new SessionCreateProducerResponseMessage(67765765, initialCredits, serverMaxRate);
//
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//  
//      pd.register(new ClientSessionPacketHandler(null, clientTargetID, null));
//
//      EasyMock.replay(cf, rc, pd, cm);
//
//      ClientSession session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, autoCommitSends, false, false,rc, cf, pd, 100, cm);
//
//      ClientProducerInternal producer = (ClientProducerInternal)session.createProducer(address);
//
//      EasyMock.verify(cf, rc, pd, cm);
//
//      assertEquals(address, producer.getAddress());
//      assertEquals(autoCommitSends && blockOnNPSend, producer.isBlockOnNonPersistentSend());
//      assertEquals(autoCommitSends && blockOnPSend, producer.isBlockOnPersistentSend());
//      assertEquals(initialCredits, producer.getInitialWindowSize());
//      assertEquals(serverMaxRate, producer.getMaxRate());
//   }
//   
//   private void testCreateProducerWideMethod(final SimpleString address, final int windowSize,
//                                             final int maxRate, final int initialCredits,
//                                             final int serverMaxRate,
//                                             final boolean blockOnNPSend,
//                                             final boolean blockOnPSend,
//                                             final boolean autoCommitSends) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//                        
//      final long clientTargetID = 7676876;
//      
//      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//      
//      final long sessionTargetID = 9121892;
//      
//      SessionCreateProducerMessage request =
//         new SessionCreateProducerMessage(clientTargetID, address, windowSize, maxRate);             
//      
//      SessionCreateProducerResponseMessage resp = 
//         new SessionCreateProducerResponseMessage(67765765, initialCredits, serverMaxRate);
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//      
//      pd.register(new ClientSessionPacketHandler(null, clientTargetID, null));
//      
//      EasyMock.replay(cf, rc, pd, cm);
//      
//      ClientSession session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, autoCommitSends, false, false,rc, cf, pd, 100, cm);
//
//      ClientProducerInternal producer = (ClientProducerInternal)session.createProducer(address, windowSize, maxRate, blockOnNPSend, blockOnPSend);
//      
//      EasyMock.verify(cf, rc, pd, cm);
//      
//      assertEquals(address, producer.getAddress());
//      assertEquals(autoCommitSends && blockOnNPSend, producer.isBlockOnNonPersistentSend());
//      assertEquals(autoCommitSends && blockOnPSend, producer.isBlockOnPersistentSend());
//      assertEquals(initialCredits, producer.getInitialWindowSize());
//      assertEquals(serverMaxRate, producer.getMaxRate());
//   }
//   
//   private void testCreateConsumerDefaultsMethod(final SimpleString queueName, final SimpleString filterString, final boolean noLocal,
//         final boolean autoDeleteQueue, final boolean direct,
//         final int windowSize, final int maxRate, final int serverWindowSize) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//         
//      EasyMock.expect(cf.getConsumerWindowSize()).andReturn(windowSize);
//      
//      EasyMock.expect(cf.getConsumerMaxRate()).andReturn(maxRate);      
//                  
//      final long clientTargetID = 87126716;
//      
//      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//      
//      final long sessionTargetID = 9121892;
//      
//      SessionCreateConsumerMessage request =
//         new SessionCreateConsumerMessage(clientTargetID, queueName, filterString, 
//                                          windowSize, maxRate);             
//      
//      SessionCreateConsumerResponseMessage resp = 
//         new SessionCreateConsumerResponseMessage(656652, serverWindowSize);
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//      
//      pd.register(new ClientConsumerPacketHandler(null, clientTargetID, null));
//      
//      cm.sendCommandOneway(resp.getConsumerTargetID(), 
//                    new SessionConsumerFlowCreditMessage(resp.getWindowSize()));
//      
//      EasyMock.replay(cf, rc, pd, cm);
//      ClientSession session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false,rc, cf, pd, 100, cm);
//      
//      ClientConsumerInternal consumer =
//         (ClientConsumerInternal)session.createConsumer(queueName, filterString,
//                                                                   direct);              
//      EasyMock.verify(cf, rc, pd, cm);
//      
//      assertEquals(clientTargetID, consumer.getClientTargetID());
//      
//      if (serverWindowSize == -1)
//      {
//         assertEquals(0, consumer.getClientWindowSize());
//      }
//      else if (serverWindowSize == 1)
//      {
//         assertEquals(1, consumer.getClientWindowSize());
//      }
//      else if (serverWindowSize > 1)
//      {
//         assertEquals(serverWindowSize >> 1, consumer.getClientWindowSize());
//      }
//   }
//   
//   private void testCreateConsumerWideMethod(final SimpleString queueName, final SimpleString filterString, final boolean noLocal,
//         final boolean autoDeleteQueue, final boolean direct,
//         final int windowSize, final int maxRate, final int serverWindowSize) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      final long clientTargetID = 87126716;
//      
//      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//      
//      final long sessionTargetID = 9121892;
//      
//      SessionCreateConsumerMessage request =
//         new SessionCreateConsumerMessage(clientTargetID, queueName, filterString,
//                                          windowSize, maxRate);             
//      
//      SessionCreateConsumerResponseMessage resp = 
//         new SessionCreateConsumerResponseMessage(656652, serverWindowSize);
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//          
//      pd.register(new ClientConsumerPacketHandler(null, clientTargetID, null));
//      
//      cm.sendCommandOneway(resp.getConsumerTargetID(), 
//                    new SessionConsumerFlowCreditMessage(resp.getWindowSize()));
//      
//      EasyMock.replay(cf, rc, pd, cm);
//      
//      ClientSession session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false,rc, cf, pd, 100, cm);
//      
//      ClientConsumerInternal consumer = (ClientConsumerInternal)session.createConsumer(queueName, filterString,
//                                                                   direct, windowSize, maxRate);    
//      EasyMock.verify(cf, rc, pd, cm); 
//      
//      assertEquals(clientTargetID, consumer.getClientTargetID());
//      
//      if (serverWindowSize == -1)
//      {
//         assertEquals(0, consumer.getClientWindowSize());
//      }
//      else if (serverWindowSize == 1)
//      {
//         assertEquals(1, consumer.getClientWindowSize());
//      }
//      else if (serverWindowSize > 1)
//      {
//         assertEquals(serverWindowSize >> 1, consumer.getClientWindowSize());
//      }
//   }
//   
//   private void testCreateConsumerBasicMethod(final SimpleString queueName, final int windowSize,
//         final int maxRate, final int serverWindowSize) throws Exception
//   {
//      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      EasyMock.expect(cf.getConsumerWindowSize()).andReturn(windowSize);
//      
//      EasyMock.expect(cf.getConsumerMaxRate()).andReturn(maxRate);   
//       
//      final long clientTargetID = 87126716;
//      
//      EasyMock.expect(pd.generateID()).andReturn(clientTargetID);
//      
//      final long sessionTargetID = 9121892;
//      
//      SessionCreateConsumerMessage request =
//         new SessionCreateConsumerMessage(clientTargetID, queueName, null,
//                                          windowSize, maxRate);             
//      
//      SessionCreateConsumerResponseMessage resp = 
//         new SessionCreateConsumerResponseMessage(656652, serverWindowSize);
//      
//      EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, request)).andReturn(resp);
//      
//      pd.register(new ClientConsumerPacketHandler(null, clientTargetID, null));
//      
//      cm.sendCommandOneway(resp.getConsumerTargetID(), 
//                    new SessionConsumerFlowCreditMessage(resp.getWindowSize()));
//      
//      EasyMock.replay(cf, rc, pd, cm);
//      
//      ClientSession session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false,rc, cf, pd, 100, cm);
//      
//      ClientConsumerInternal consumer = (ClientConsumerInternal)session.createConsumer(queueName);    
//      EasyMock.verify(cf, rc, pd, cm);
//      
//      assertEquals(clientTargetID, consumer.getClientTargetID());
//      
//      if (serverWindowSize == -1)
//      {
//         assertEquals(0, consumer.getClientWindowSize());
//      }
//      else if (serverWindowSize == 1)
//      {
//         assertEquals(1, consumer.getClientWindowSize());
//      }
//      else if (serverWindowSize > 1)
//      {
//         assertEquals(serverWindowSize >> 1, consumer.getClientWindowSize());
//      }
//   }
//   
//   private void testConstructor(final long serverTargetID,
//         final boolean xa,
//         final int lazyAckBatchSize, final boolean cacheProducers,                            
//         final boolean autoCommitSends, final boolean autoCommitAcks,
//         final boolean blockOnAcknowledge,
//         final int version) throws Exception
//   {      
//      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);      
//      ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      
//      EasyMock.replay(rc, cf, pd, cm);
//
//      ClientSessionInternal session = new ClientSessionImpl("blah", serverTargetID, xa,
//            lazyAckBatchSize, cacheProducers, autoCommitSends, autoCommitAcks, blockOnAcknowledge,
//            rc, cf, pd, version, cm);
//
//      EasyMock.verify(rc, cf, pd, cm);
//     
//      assertEquals(xa, session.isXA());
//      assertEquals(lazyAckBatchSize, session.getLazyAckBatchSize());
//      assertEquals(cacheProducers, session.isCacheProducers());
//      assertEquals(autoCommitSends, session.isAutoCommitSends());
//      assertEquals(autoCommitAcks, session.isAutoCommitAcks());
//      assertEquals(blockOnAcknowledge, session.isBlockOnAcknowledge());
//      assertEquals(version, session.getVersion());
//   }
}

