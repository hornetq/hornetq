/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.timing.core.client.impl;

import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A ClientConsumerImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientConsumerImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientConsumerImplTest.class);

   public void testDummy()
   {      
   }
   
//   public void testSetHandlerWhileReceiving() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
//      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
//      Channel channel = EasyMock.createStrictMock(Channel.class);
//
//      final ClientConsumerInternal consumer =
//         new ClientConsumerImpl(session, 123, 67576, false, executor, channel);
//      
//      MessageHandler handler = new MessageHandler()
//      {
//         public void onMessage(ClientMessage msg)
//         {            
//         }
//      };
//      
//      Thread t = new Thread()
//      {
//         public void run()
//         {
//            try
//            {
//               consumer.receive(2000);
//            }
//            catch (Exception e)
//            {
//            }
//         }
//      };
//      
//      t.start();
//      
//      Thread.sleep(1000);
//      
//      try
//      {
//         consumer.setMessageHandler(handler);
//         
//         fail("Should throw exception");
//      }
//      catch (MessagingException e)
//      {
//         assertEquals(MessagingException.ILLEGAL_STATE, e.getCode());
//      } 
//      finally
//      {
//         t.interrupt();
//      }
//   }
//   
//   public void testCloseWhileReceiving() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
//      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
//      Channel channel = EasyMock.createStrictMock(Channel.class);
//       
//      final long clientTargetID = 283748;
//      final long targetID = 12934;
//      final long sessionTargetID = 23847327;
//      
//      final ClientConsumerInternal consumer =
//         new ClientConsumerImpl(session, targetID, clientTargetID, 787, false, pd, executor, cm);
//                   
//      pd.unregister(clientTargetID);
//      session.removeConsumer(consumer);
//      EasyMock.expect(cm.sendCommandBlocking(targetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
//
//      EasyMock.replay(session,cm, executor, pd);
//      
//      class ReceiverThread extends Thread
//      {
//         volatile boolean returned;
//         volatile ClientMessage msg;
//         volatile boolean failed;
//         public void run()
//         {
//            try
//            {
//               msg = consumer.receive();
//               returned = true;
//            }
//            catch (Exception e)
//            {
//               failed = true;
//            }
//         }
//      };
//      
//      ReceiverThread t = new ReceiverThread();
//      
//      t.start();
//      
//      Thread.sleep(2000);
//                  
//      consumer.close();
//      
//      Thread.sleep(2000);
//      
//      assertTrue(t.returned);
//      assertNull(t.msg);
//      assertFalse(t.failed);
//      
//      t.join();
//      
//      EasyMock.verify(session, cm, executor, pd);
//   }
//   
//   public void testReceiveHandleMessagesAfterReceiveNoTimeout() throws Exception
//   {
//      testReceiveHandleMessagesAfterReceive(4000);
//   }
//   
//   public void testReceiveHandleMessagesAfterReceiveTimeout() throws Exception
//   {
//      testReceiveHandleMessagesAfterReceive(0);
//   }
//   
//   private void testReceiveHandleMessagesAfterReceive(final int timeout) throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);      
//      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//       
//      final int numMessages = 10;
//      
//      final List<ClientMessage> msgs = new ArrayList<ClientMessage>();
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
//         
//         msgs.add(msg);
//         
//         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
//         
//         EasyMock.expect(msg.isExpired()).andStubReturn(false);
//         
//         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
//         
//         session.delivered((long)i, false);
//         
//         EasyMock.expect(msg.getEncodeSize()).andReturn(1);
//      }
//      
//      EasyMock.replay(session, cm, executor, pd);
//      EasyMock.replay(msgs.toArray());
//            
//      final ClientConsumerInternal consumer =
//         new ClientConsumerImpl(session, 675765, 67565, 787, false, pd, executor, cm);
//            
//      final long pause = 2000;
//      
//      class AdderThread extends Thread      
//      {
//         volatile boolean failed;
//         
//         public void run()
//         {
//            try
//            {
//               Thread.sleep(pause);
//               
//               for (ClientMessage msg: msgs)
//               {
//                  consumer.handleMessage(msg);
//               }
//            }
//            catch (Exception e)
//            {
//               log.error("Failed to add messages", e);
//               failed = true;
//            }
//         }
//      };
//      
//      AdderThread t = new AdderThread();
//      
//      t.start();
//            
//      for (int i = 0; i < numMessages; i++)
//      {      
//         ClientMessage msg;
//         
//         if (timeout == 0)
//         {
//            msg = consumer.receive();
//         }
//         else
//         {
//            msg = consumer.receive(timeout);
//         }
//   
//         assertTrue(msg == msgs.get(i));
//      }
//
//      assertNull(consumer.receiveImmediate());  
//      
//      t.join();
//      
//      assertFalse(t.failed);
//      
//      EasyMock.verify(session, cm, executor, pd);
//      EasyMock.verify(msgs.toArray());
//      
//      assertEquals(0, consumer.getBufferSize());   
//   }
//   
//   public void testReceiveHandleMessagesAfterReceiveWithTimeout() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
//      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class); 
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//       
//      final int numMessages = 10;
//      
//      final List<ClientMessage> msgs = new ArrayList<ClientMessage>();
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
//         
//         msgs.add(msg);
//         
//         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
//      }
//      
//      EasyMock.replay(session, cm, executor, pd);
//      EasyMock.replay(msgs.toArray());
//            
//      final ClientConsumerInternal consumer =
//         new ClientConsumerImpl(session, 675765, 67565, 787, false, pd, executor, cm);
//            
//      final long pause = 2000;
//      
//      final long timeout = 1000;
//      
//      class AdderThread extends Thread      
//      {
//         volatile boolean failed;
//         
//         public void run()
//         {
//            try
//            {
//               Thread.sleep(pause);
//               
//               for (ClientMessage msg: msgs)
//               {
//                  consumer.handleMessage(msg);
//               }
//            }
//            catch (Exception e)
//            {
//               log.error("Failed to add messages", e);
//               failed = true;
//            }
//         }
//      };
//      
//      AdderThread t = new AdderThread();
//      
//      t.start();
//            
//      ClientMessage msg = consumer.receive(timeout);         
//        
//      assertNull(msg);  
//      
//      t.join();
//      
//      assertFalse(t.failed);
//      
//      EasyMock.verify(session, cm, executor, pd);
//      EasyMock.verify(msgs.toArray());
//      
//      assertEquals(numMessages, consumer.getBufferSize());   
//   }
//   
//   public void testReceiveExpiredWithTimeout() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
//            
//      final int numMessages = 10;
//      
//      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
//         
//         msgs.add(msg);
//         
//         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
//         
//         EasyMock.expect(msg.isExpired()).andStubReturn(true);
//         
//         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
//         
//         session.delivered((long)i, true);
//         
//         EasyMock.expect(msg.getEncodeSize()).andReturn(1);
//      }
//      
//      EasyMock.replay(session, cm, executor, pd);
//      EasyMock.replay(msgs.toArray());
//            
//      ClientConsumerInternal consumer =
//         new ClientConsumerImpl(session, 675765, 67565, 787, false, pd, executor, cm);
//      
//      for (ClientMessage msg: msgs)
//      {
//         consumer.handleMessage(msg);
//      }
//
//      assertEquals(numMessages, consumer.getBufferSize());         
//
//      for (ClientMessage msg: msgs)
//      {
//         assertNull(consumer.receive(100));
//      }
//      
//      EasyMock.verify(session, cm, executor, pd);
//      EasyMock.verify(msgs.toArray());
//   }
//   
//   public void testWaitForOnMessageToCompleteOnClose() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);      
//      ExecutorService executor = Executors.newSingleThreadExecutor();
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//             
//      final long clientTargetID = 283748;
//      final long targetID = 12934;
//
//      final ClientConsumerInternal consumer =
//         new ClientConsumerImpl(session, targetID, clientTargetID, 787, false, pd, executor, cm);
//      
//      class MyHandler implements MessageHandler
//      {
//         volatile boolean failed;
//         volatile boolean complete;
//         public void onMessage(ClientMessage msg)
//         {            
//            try
//            {
//               Thread.sleep(1000);
//               complete = true;
//            }
//            catch (Exception e)
//            {         
//               failed = true;
//            }            
//         }         
//      };
//      
//      MyHandler handler = new MyHandler();
//      
//      consumer.setMessageHandler(handler);
//     
//      ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
//        
//      EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
//      
//      EasyMock.expect(msg.isExpired()).andStubReturn(false);
//      
//      EasyMock.expect(msg.getDeliveryID()).andStubReturn(0L);
//      
//      session.delivered(0L, false);
//      
//      EasyMock.expect(msg.getEncodeSize()).andReturn(1);
//      
//      pd.unregister(clientTargetID);
//      session.removeConsumer(consumer);
//      EasyMock.expect(cm.sendCommandBlocking(targetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
//      EasyMock.replay(session, cm, pd, msg);
//             
//      consumer.handleMessage(msg);
//      
//      consumer.close();
//      
//      assertTrue(handler.complete);
//      
//      assertFalse(handler.failed);
//      
//      EasyMock.verify(session, cm, pd, msg);           
//   }
//
//   public void testWaitForOnMessageToCompleteOnCloseTimeout() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//      ExecutorService executor = Executors.newSingleThreadExecutor();
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
//             
//      final long clientTargetID = 283748;
//      final long targetID = 12934;
// 
//      final ClientConsumerInternal consumer =
//         new ClientConsumerImpl(session, targetID, clientTargetID, 787, false, pd, executor, cm);
//      
//      class MyHandler implements MessageHandler
//      {
//         volatile boolean failed;
//         volatile boolean complete;
//         public void onMessage(ClientMessage msg)
//         {            
//            try
//            {
//               Thread.sleep(ClientConsumerImpl.CLOSE_TIMEOUT_MILLISECONDS + 2000);
//               complete = true;
//            }
//            catch (Exception e)
//            {         
//               failed = true;
//            }            
//         }         
//      };
//      
//      MyHandler handler = new MyHandler();
//      
//      consumer.setMessageHandler(handler);
//     
//      ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
//        
//      EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
//      
//      EasyMock.expect(msg.isExpired()).andStubReturn(false);
//      
//      EasyMock.expect(msg.getDeliveryID()).andStubReturn(0L);
//      
//      session.delivered(0L, false);
//      
//      EasyMock.expect(msg.getEncodeSize()).andReturn(1);
//      
//      pd.unregister(clientTargetID);
//      session.removeConsumer(consumer);
//      EasyMock.expect(cm.sendCommandBlocking(targetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
//      EasyMock.replay(session, cm, pd, msg);
//             
//      consumer.handleMessage(msg);
//      
//      long start = System.currentTimeMillis();
//      consumer.close();
//      long end = System.currentTimeMillis();
//      assertTrue((end - start) >= ClientConsumerImpl.CLOSE_TIMEOUT_MILLISECONDS);
//      
//      assertFalse(handler.complete);
//      
//      assertFalse(handler.failed);
//      
//      EasyMock.verify(session, cm, pd, msg);           
//   }
//   
//   public void testWaitForOnMessageToCompleteOnCloseSameThread() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);      
//      ExecutorService executor = new DirectExecutorService();
//      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//             
//      final long clientTargetID = 283748;
//      final long targetID = 12934;
//      
//      final ClientConsumerInternal consumer =
//         new ClientConsumerImpl(session, targetID, clientTargetID, 787, false, pd, executor, cm);
//      
//      class MyHandler implements MessageHandler
//      {
//         volatile boolean failed;
//         volatile boolean complete;
//         public void onMessage(ClientMessage msg)
//         {            
//            try
//            {
//               Thread.sleep(1000);
//               complete = true;
//            }
//            catch (Exception e)
//            {         
//               failed = true;
//            }            
//         }         
//      };
//      
//      MyHandler handler = new MyHandler();
//      
//      consumer.setMessageHandler(handler);
//     
//      ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
//        
//      EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
//      
//      EasyMock.expect(msg.isExpired()).andStubReturn(false);
//      
//      EasyMock.expect(msg.getDeliveryID()).andStubReturn(0L);
//      
//      session.delivered(0L, false);
//      
//      EasyMock.expect(msg.getEncodeSize()).andReturn(1);
//      
//      pd.unregister(clientTargetID);
//      session.removeConsumer(consumer);
//      EasyMock.expect(cm.sendCommandBlocking(targetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
//      EasyMock.replay(session, cm, pd, msg);
//             
//      consumer.handleMessage(msg);
//      
//      consumer.close();
//      
//      assertTrue(handler.complete);
//      
//      assertFalse(handler.failed);
//      
//      EasyMock.verify(session, cm, pd, msg);           
//   }
   
   // Private -----------------------------------------------------------------------------------------------------------

}
