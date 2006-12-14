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
package org.jboss.test.messaging.jms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.aop.Dispatcher;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.remoting.HandleMessageResponse;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.server.endpoint.Ack;
import org.jboss.jms.server.endpoint.Cancel;
import org.jboss.jms.server.endpoint.ClientDelivery;
import org.jboss.jms.server.endpoint.DefaultAck;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.tx.ClientTransaction.SessionTxState;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.IdBlock;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvocationResponse;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.invocation.InternalInvocation;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.jms.message.MessageTest;
import org.jboss.util.id.GUID;

/**
 * 
 * A WireFormatTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * WireFormatTest.java,v 1.1 2006/02/02 17:35:29 timfox Exp
 */
public class WireFormatTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
      
   private static final Logger log = Logger.getLogger(WireFormatTest.class);

   // TODO - replace with a dynamic value
   private static final byte CURRENT_VERSION = 8;

   // Static --------------------------------------------------------
      
   // Attributes ----------------------------------------------------
   
   protected TestWireFormat wf;
   
   //Session
   
   protected Method sendMethod;
   
   protected Method acknowledgeMethod;
   
   protected Method acknowledgeBatchMethod;
   
   protected Method cancelDeliveriesMethod;
   
   //Consumer
        
   protected Method moreMethod;
   
 
   //connection
   
   protected Method sendTransactionMethod;
   
   //callback
   
   // Constructors --------------------------------------------------

   public WireFormatTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      
      wf = new TestWireFormat();
      
      Class sessionDelegate = SessionDelegate.class;
      
      Class consumerDelegate = ConsumerDelegate.class;
      
      Class connectionDelegate = ConnectionDelegate.class;
      
      //Session
      
      sendMethod = sessionDelegate.getMethod("send", new Class[] { JBossMessage.class });
       
      acknowledgeMethod = sessionDelegate.getMethod("acknowledge", new Class[] { Ack.class });
      
      acknowledgeBatchMethod = sessionDelegate.getMethod("acknowledgeBatch", new Class[] { java.util.List.class });
      
      cancelDeliveriesMethod = sessionDelegate.getMethod("cancelDeliveries", new Class[] { java.util.List.class });
      
      //TODO - this isn't complete - there are other methods to test
            
      //Consumer
            
      moreMethod = consumerDelegate.getMethod("more", null);

      //Connection
      
      sendTransactionMethod = connectionDelegate.getMethod("sendTransaction", new Class[] { TransactionRequest.class });
      
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {      
      super.tearDown();
   }
   
   //Session
   
   public void testAcknowledge() throws Exception
   {
      wf.testAcknowledge();
   }
   
   public void testAcknowledgeBatch() throws Exception
   {
      wf.testAcknowledgeBatch();
   }
   
   public void testSend() throws Exception
   {
      wf.testSend();
   }
   
   //Consumer
   
   public void testMore() throws Exception
   {
      wf.testMore();
   }
   
   public void testCancelDeliveries() throws Exception
   {
      wf.testCancelDeliveries();
   }
   
   //Connection
   
   public void testSendTransaction() throws Exception
   {
      wf.testSendTransaction();
   }
   
   //Others
   
      
   public void testExceptionResponse() throws Exception
   {
      wf.testExceptionResponse();
   }
    
   public void testNullResponse() throws Exception
   {
      wf.testNullResponse();
   }
   
   public void testSerializableRequest() throws Exception
   {
      wf.testSerializableRequest();
   }
   
   public void testSerializableResponse() throws Exception
   {
      wf.testSerializableResponse();
   }
   
   public void testCallBack() throws Exception
   {
      wf.testCallback();
   }
   
   public void testIDBlockResponse() throws Exception
   {
      wf.testGetIdBlockResponse();
   }
   
   public void testHandleMessageResponse() throws Exception
   {
      wf.testHandleMessageResponse();
   }
            
   // Public --------------------------------------------------------
   
   public static class SerializableObject implements Serializable
   {      
      /** The serialVersionUID */
      private static final long serialVersionUID = 1L;

      public SerializableObject()
      {         
      }

      SerializableObject(String s, long l)
      {
         this.s = s;
         this.l = l;
      }
      
      public String s;
      
      public long l;      
   }
         
   /**
    * We extend the class so we have access to protected fields
    */
   class TestWireFormat extends JMSWireFormat
   {      
      public void testAcknowledge() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, acknowledgeMethod, acknowledgeMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         int deliveryID = 765;
         Ack ack = new DefaultAck(deliveryID);
         
         Object[] args = new Object[] { ack };
         
         mi.setArguments(args);
         
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, mi);
         
         InvocationRequest ir = new InvocationRequest(null, null, mm, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
                  
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         DataInputStream dis = new DataInputStream(bis); 
                 
         //Check the bytes
         
         //First byte should be version
         assertEquals(77, dis.readByte());
         
         //First byte should be ACKNOWLEDGE
         assertEquals(JMSWireFormat.ACKNOWLEDGE, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
         
         //Next should be the deliveryid
         long l = dis.readLong();
         
         assertEquals(deliveryID, l);

         //Now eos
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getParameter();
         
         assertEquals(77, mm.getVersion());
         
         MethodInvocation mi2 = (MethodInvocation)mm.getLoad();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());
         
         Ack l2 = (Ack)mi2.getArguments()[0];
         
         assertEquals(deliveryID, l2.getDeliveryId());
         
      }
      
      public void testAcknowledgeBatch() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, acknowledgeBatchMethod, acknowledgeBatchMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         long ackA = 1343;
         
         long ackB = 176276;
         
         long ackC = 17261726;
                  
         List acks = new ArrayList();
         acks.add(new DefaultAck(ackA));
         acks.add(new DefaultAck(ackB));
         acks.add(new DefaultAck(ackC));
         
         Object[] args = new Object[] { acks };
         
         mi.setArguments(args);
                  
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, mi);
         
         InvocationRequest ir = new InvocationRequest(null, null, mm, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
                  
         OutputStream oos = new DataOutputStream(bos);
      
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         DataInputStream dis = new DataInputStream(bis); 
                 
         //Check the bytes
         
         //First byte should be version
         assertEquals(77, dis.readByte());
         
         //First byte should be ACKNOWLEDGE
         assertEquals(JMSWireFormat.ACKNOWLEDGE_BATCH, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
         
         //Next should be number of acks
         assertEquals(3, dis.readInt());
         
         //Now the acks
         long l1 = dis.readLong();
         
         long l2 = dis.readLong();
         
         long l3 = dis.readLong();
         
         assertEquals(ackA, l1);
         assertEquals(ackB, l2);
         assertEquals(ackC, l3);
         
         
         //Now eos
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getParameter();
         
         assertEquals(77, mm.getVersion());
         
         MethodInvocation mi2 = (MethodInvocation)mm.getLoad();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());
         
         List acks2 = (List)mi2.getArguments()[0];
         
         assertEquals(3, acks.size());
         
         assertEquals(ackA, ((DefaultAck)acks2.get(0)).getDeliveryId());
         assertEquals(ackB, ((DefaultAck)acks2.get(1)).getDeliveryId());
         assertEquals(ackC, ((DefaultAck)acks2.get(2)).getDeliveryId());
         
      }
      
      
      /*
       * Test that general serializable invocation requests are marshalled correctky
       */
      public void testSerializableRequest() throws Exception
      {
         String s = new GUID().toString();
         
         long l = 123456789;
         
         SerializableObject so = new SerializableObject(s, l);  
         
         String sessionID = "sessionid123";
         
         String subsystem = "Testing123";
         
         Map requestPayload = new HashMap();
         
         requestPayload.put("testingkey1", "wibble1");
         
         Map returnPayload = new HashMap();
         
         returnPayload.put("testingkey2", "wibble2");
         
         InvokerLocator locator = new InvokerLocator("socket://0.0.0.0:0");
                
         InvocationRequest ir = new InvocationRequest(sessionID, subsystem, so, requestPayload, returnPayload, locator);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         // Check the bytes
                  
         DataInputStream dis = new DataInputStream(bis);
         
         // First byte should be version
         byte version = dis.readByte();
         
         assertEquals(CURRENT_VERSION, version);
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
                                 
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         assertNotNull(ir2);
         
         assertEquals("sessionid123", ir2.getSessionId());
         
         assertEquals("Testing123", ir2.getSubsystem());
         
         assertEquals("wibble1", ir2.getRequestPayload().get("testingkey1"));
         
         assertEquals("wibble2", ir2.getReturnPayload().get("testingkey2"));
         
         assertEquals("socket", ir2.getLocator().getProtocol());
         
         assertEquals(s, ((SerializableObject)ir2.getParameter()).s);
         
         assertEquals(l, ((SerializableObject)ir2.getParameter()).l);                          
      }
      
      /*
       * Test that general serializable invocation responses are marshalled correctky
       */
      public void testSerializableResponse() throws Exception
      {
         String s = new GUID().toString();
         
         long l = 987654321;
         
         SerializableObject so = new SerializableObject(s, l);  
         
         String sessionID = "sessionid456";
         
         Map payload = new HashMap();
         
         payload.put("testingkey3", "wibble3");
                        
         InvocationResponse ir = new InvocationResponse(sessionID, so, false, payload);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         DataInputStream dis = new DataInputStream(bis);
         
         // First byte should be version
         byte version = dis.readByte();
         
         assertEquals(CURRENT_VERSION, version);
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
            
         InvocationResponse ir2 = (InvocationResponse)wf.read(ois, null);
         
         assertNotNull(ir2);
         
         assertEquals("sessionid456", ir2.getSessionId());
         
         assertEquals("wibble3", ir2.getPayload().get("testingkey3"));
         
         assertEquals(s, ((SerializableObject)ir2.getResult()).s);
         
         assertEquals(l, ((SerializableObject)ir2.getResult()).l);         
      }
      
      public void testExceptionResponse() throws Exception
      {
         String sessionID = "sessionid456";
         
         Map payload = new HashMap();
         
         payload.put("testingkey3", "wibble3");
                        
         InvocationResponse ir = new InvocationResponse(sessionID, new Exception("Flamingo"), true, payload);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         DataInputStream dis = new DataInputStream(bis);
         
         // First byte should be version
         byte version = dis.readByte();
         
         assertEquals(CURRENT_VERSION, version);
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
                  
         InvocationResponse ir2 = (InvocationResponse)wf.read(ois, null);
         
         assertNotNull(ir2);
         
         assertEquals("sessionid456", ir2.getSessionId());
         
         assertEquals("wibble3", ir2.getPayload().get("testingkey3"));
         
         assertEquals("Flamingo", ((Exception)ir2.getResult()).getMessage());
         
         assertTrue(ir2.isException());
                 
      }
                  
      public void testSend() throws Exception
      {       
         JBossMessage m = new JBossMessage(123);
         
         MessageTest.configureMessage(m);
         
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, sendMethod, sendMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         mi.setArguments(new Object[] {m});
         
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, mi);
         
         InvocationRequest ir = new InvocationRequest(null, null, mm, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
                  
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         DataInputStream dis = new DataInputStream(bis); 
               
         //Check the bytes
         
         //First byte should be version
         assertEquals(77, dis.readByte());
         
         //First byte should be SEND
         assertEquals(JMSWireFormat.SEND, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
         
         //Next should be be type         
         byte type = dis.readByte();
         
         assertEquals(JBossMessage.TYPE, type);
         
         //Next should come the message
         JBossMessage m2 = new JBossMessage();
         
         m2.read(dis);
         
         //should be eos
         
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }

         MessageTest.ensureEquivalent(m, m2);
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getParameter();
         
         assertEquals(77, mm.getVersion());
         
         MethodInvocation mi2 = (MethodInvocation)mm.getLoad();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());
         
         JBossMessage m3 = (JBossMessage)mi2.getArguments()[0];
         
         MessageTest.ensureEquivalent(m, m3);
                  
      }  
      
      public void testSendTransaction() throws Exception
      {       
         JBossMessage m = new JBossMessage(123);
         MessageTest.configureMessage(m);
         
         long deliveryId = 89281389;
         
         int deliveryCount = 12;
         
         MessageProxy proxy = JBossMessage.createThinDelegate(deliveryId, m, deliveryCount);
                     
         DeliveryInfo info = new DeliveryInfo(proxy, 76762, 98982);
         
         int sessionId = 8787;
         
         ClientTransaction state = new ClientTransaction();
         state.addMessage(sessionId, m);
         state.addAck(sessionId, info);
          
         TransactionRequest request = new TransactionRequest(TransactionRequest.ONE_PHASE_COMMIT_REQUEST, null, state);
                           
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, sendTransactionMethod, sendTransactionMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         mi.setArguments(new Object[] {request});
         
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, mi);
         
         InvocationRequest ir = new InvocationRequest(null, null, mm, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
                  
         wf.write(ir, oos);
        
         oos.flush();
               
         byte[] bytes = bos.toByteArray();
              
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         DataInputStream dis = new DataInputStream(bis); 
               
         //Check the bytes
             
         //First byte should be version
         assertEquals(77, dis.readByte());
         
         //First byte should be SEND_TRANSACTION
         assertEquals(JMSWireFormat.SEND_TRANSACTION, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
                  
         //Next should come the transaction request
         
         TransactionRequest req = new TransactionRequest();
                         
         req.read(dis);
         
         //should be eos
                
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         ClientTransaction state2 = req.getState();
         
         Collection sessionStates = state2.getSessionStates();
         
         assertEquals(1, sessionStates.size());
         
         SessionTxState sess = (SessionTxState)sessionStates.iterator().next();

         JBossMessage m2 = (JBossMessage)sess.getMsgs().get(0);
         
         MessageTest.ensureEquivalent(m, m2);
         
         assertEquals(TransactionRequest.ONE_PHASE_COMMIT_REQUEST, req.getRequestType());
         
         Ack ack = (Ack)sess.getAcks().get(0);
         
         assertEquals(deliveryId, ack.getDeliveryId());
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getParameter();
         
         assertEquals(77, mm.getVersion());
         
         MethodInvocation mi2 = (MethodInvocation)mm.getLoad();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());
         
         TransactionRequest req2 = (TransactionRequest)mi2.getArguments()[0];
         
         ClientTransaction state3 = req2.getState();
         
         Collection sessionStates2 = state3.getSessionStates();
         
         SessionTxState sess2 = (SessionTxState)sessionStates2.iterator().next();
         
         JBossMessage m3 = (JBossMessage)sess2.getMsgs().get(0);
         
         MessageTest.ensureEquivalent(m, m3);
         
         assertEquals(TransactionRequest.ONE_PHASE_COMMIT_REQUEST, req2.getRequestType());
         
         Ack ack2 = (Ack)sess2.getAcks().get(0);
         
         assertEquals(deliveryId, ack2.getDeliveryId());
         
      }  
            
      
      public void testCancelDeliveries() throws Exception
      {                            
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         List cancels = new ArrayList();
         
         Cancel cancel1 = new Cancel(65654, 43);
         Cancel cancel2 = new Cancel(65765, 2);
         cancels.add(cancel1);
         cancels.add(cancel2);
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, cancelDeliveriesMethod, cancelDeliveriesMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         mi.setArguments(new Object[] {cancels});
         
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, mi);
         
         InvocationRequest ir = new InvocationRequest(null, null, mm, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
                  
         wf.write(ir, oos);
        
         oos.flush();
               
         byte[] bytes = bos.toByteArray();
              
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         DataInputStream dis = new DataInputStream(bis); 
               
         //Check the bytes
             
         //First byte should be version
         assertEquals(77, dis.readByte());
         
         //Next byte should be CANCEL_MESSAGES
         assertEquals(JMSWireFormat.CANCEL_DELIVERIES, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
                  
         //Next should the size of the list
         
         int size = dis.readInt();
         
         assertEquals(2, size);
         
         //then the AckInfos
         Cancel rcancel1 = new Cancel();
         
         Cancel rcancel2 = new Cancel();
         
         rcancel1.read(dis);
         
         rcancel2.read(dis);
         
         assertEquals(cancel1.getDeliveryCount(), rcancel1.getDeliveryCount());
         
         assertEquals(cancel1.getDeliveryId(), cancel1.getDeliveryId());
         
         assertEquals(cancel2.getDeliveryCount(), rcancel2.getDeliveryCount());
         
         assertEquals(cancel2.getDeliveryId(), cancel2.getDeliveryId());
                   
         //should be eos
                
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getParameter();
         
         assertEquals(77, mm.getVersion());
         
         MethodInvocation mi2 = (MethodInvocation)mm.getLoad();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());
         
         List list = (List)mi2.getArguments()[0];
        
         assertEquals(2, list.size());
         
         Cancel xack1 = (Cancel)list.get(0);
         Cancel xack2 = (Cancel)list.get(1);
         
         assertEquals(cancel1.getDeliveryId(), xack1.getDeliveryId());
         
         assertEquals(cancel1.getDeliveryCount(), xack1.getDeliveryCount());
         
         assertEquals(cancel2.getDeliveryId(), xack2.getDeliveryId());
         
         assertEquals(cancel2.getDeliveryCount(), xack2.getDeliveryCount());
         
      }  
      
      public void testNullResponse() throws Exception
      {
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, null);
         InvocationResponse resp = new InvocationResponse(null, mm, false, null);

         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         OutputStream oos = new DataOutputStream(bos);
         
         wf.write(resp, oos);
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
       
         DataInputStream dis = new DataInputStream(bis);

         // First byte should be version
         assertEquals(77, dis.readByte());

         // Should be 1 byte
         byte b = dis.readByte();

         assertEquals(JMSWireFormat.NULL_RESPONSE, b);

         // Should be eos
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();

         InputStream ois = new DataInputStream(bis);

         InvocationResponse ir2 = (InvocationResponse)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getResult();
         
         assertEquals(77, mm.getVersion());
         
         assertNull(mm.getLoad());
            
      }
      
      
      
      public void testMore() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, moreMethod, moreMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, mi);
         
         InvocationRequest ir = new InvocationRequest(null, null, mm, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
                  
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         DataInputStream dis = new DataInputStream(bis); 
         
         //Check the bytes
         
         //First byte should be version
         assertEquals(77, dis.readByte());         
         
         //Second byte should be MORE
         assertEquals(JMSWireFormat.MORE, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
         
         //Now eos
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         InputStream ois = new DataInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getParameter();
         
         assertEquals(77, mm.getVersion());
         
         MethodInvocation mi2 = (MethodInvocation)mm.getLoad();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());         
      }
      
      
      
      public void testCallback() throws Exception
      {
         int consumerID = 12345678;
         
         JBossMessage m1 = new JBossMessage(123);
         JBossMessage m2 = new JBossMessage(456);
         JBossMessage m3 = new JBossMessage(789);
         
         List msgs = new ArrayList();
         
         MessageProxy del1 = JBossMessage.createThinDelegate(1, m1, 7);
         MessageProxy del2 = JBossMessage.createThinDelegate(2, m2, 8);
         MessageProxy del3 = JBossMessage.createThinDelegate(3, m3, 9);
         
         MessageTest.configureMessage(m1);
         MessageTest.configureMessage(m2);
         MessageTest.configureMessage(m3);
         
         msgs.add(del1);
         msgs.add(del2);
         msgs.add(del3);         
         
         ClientDelivery dr = new ClientDelivery(msgs, consumerID);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
         
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, dr);
         
         InvocationRequest ir = new InvocationRequest("dummySessionId", null, mm, null, null, null);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                  
         DataInputStream dis = new DataInputStream(bis);
         
         //Check the bytes
         
         //First byte should be version
         assertEquals(77, dis.readByte());         
         
         //Second byte should be CALLBACK
         assertEquals(JMSWireFormat.CALLBACK, dis.readByte());
         
         //Next should be sessionID
         assertEquals("dummySessionId", dis.readUTF());
         
         //Next int should be consumer id
         assertEquals(12345678, dis.readInt());
         
         //Next int should be number of messages
         assertEquals(3, dis.readInt());
                           
         //Next byte should be type
         assertEquals(JBossMessage.TYPE, dis.readByte());
         
         //Next int should be delivery count
         assertEquals(7, dis.readInt());
         
         //Delivery id
         assertEquals(1, dis.readLong());
         
         //And now the message itself
         JBossMessage r1 = new JBossMessage();
         
         r1.read(dis);
         
         
         //Next byte should be type
         assertEquals(JBossMessage.TYPE, dis.readByte());
         
         //Next int should be delivery count
         assertEquals(8, dis.readInt());
         
         // Delivery id
         assertEquals(2, dis.readLong());
         
         //And now the message itself
         JBossMessage r2 = new JBossMessage();
         
         r2.read(dis);
         
         
         //Next byte should be type
         assertEquals(JBossMessage.TYPE, dis.readByte());
         
         //Next int should be delivery count
         assertEquals(9, dis.readInt());
         
         // Delivery id
         assertEquals(3, dis.readLong());
         
         //And now the message itself
         JBossMessage r3 = new JBossMessage();
         
         r3.read(dis);
         
         MessageTest.ensureEquivalent(m1, r1);
         MessageTest.ensureEquivalent(m2, r2);
         MessageTest.ensureEquivalent(m3, r3);
         
         //eos
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         InternalInvocation ii = (InternalInvocation) ir2.getParameter();
         
         Object[] parameters = ii.getParameters();
         
         assertNotNull(parameters);
         
         Callback callback = (Callback) parameters[0];
         
         mm = (MessagingMarshallable)callback.getParameter();
         
         assertEquals(77, mm.getVersion());
                  
         ClientDelivery dr2 = (ClientDelivery)mm.getLoad();
         
         List msgs2 = dr2.getMessages();
         
         assertEquals(consumerID, dr2.getConsumerId());
         
         MessageProxy p1 = (MessageProxy)msgs2.get(0);
         MessageProxy p2 = (MessageProxy)msgs2.get(1);
         MessageProxy p3 = (MessageProxy)msgs2.get(2);
         
         assertEquals(del1.getDeliveryCount(), p1.getDeliveryCount());
         assertEquals(del2.getDeliveryCount(), p2.getDeliveryCount());
         assertEquals(del3.getDeliveryCount(), p3.getDeliveryCount());
         
         JBossMessage q1 = p1.getMessage();
         JBossMessage q2 = p1.getMessage();
         JBossMessage q3 = p1.getMessage();
         
         MessageTest.ensureEquivalent(m1, q1);
         MessageTest.ensureEquivalent(m2, q2);
         MessageTest.ensureEquivalent(m3, q3);         
      }
      
                  
      public void testGetIdBlockResponse() throws Exception
      {
         IdBlock block = new IdBlock(132, 465);
         
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, block);
                  
         InvocationResponse ir = new InvocationResponse(null, mm, false, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         DataInputStream dis = new DataInputStream(bis);
                   
         // First byte should be version
         assertEquals(77, dis.readByte());
         
         int b = dis.readByte();
         
         assertEquals(JMSWireFormat.ID_BLOCK_RESPONSE, b);
         
         IdBlock block2 = new IdBlock();
         
         block2.read(dis);
         
         assertEquals(block.getLow(), block2.getLow());
         assertEquals(block.getHigh(), block2.getHigh());
         
         //eos
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
         
         InvocationResponse ir2 = (InvocationResponse)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getResult();
         
         assertEquals(77, mm.getVersion());
         
         IdBlock block3 = (IdBlock)mm.getLoad();
         
         assertEquals(block.getLow(), block3.getLow());
         assertEquals(block.getHigh(), block3.getHigh());                  
      }    
      
      public void testHandleMessageResponse() throws Exception
      {
         HandleMessageResponse h = new HandleMessageResponse(true, 76876);
         
         MessagingMarshallable mm = new MessagingMarshallable((byte)77, h);
                  
         InvocationResponse ir = new InvocationResponse(null, mm, false, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         DataInputStream dis = new DataInputStream(bis);
                   
         // First byte should be version
         assertEquals(77, dis.readByte());
         
         int b = dis.readByte();
         
         assertEquals(JMSWireFormat.HANDLE_MESSAGE_RESPONSE, b);
         
         HandleMessageResponse h2 = new HandleMessageResponse();
         
         h2.read(dis);
         
         assertEquals(h.clientIsFull(), h2.clientIsFull());
         assertEquals(h.getNumberAccepted(), h2.getNumberAccepted());
         
         //eos
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         InputStream ois = new DataInputStream(bis);
         
         InvocationResponse ir2 = (InvocationResponse)wf.read(ois, null);
         
         mm = (MessagingMarshallable)ir2.getResult();
         
         assertEquals(77, mm.getVersion());
         
         HandleMessageResponse h3 = (HandleMessageResponse)mm.getLoad();
         
         assertEquals(h.clientIsFull(), h3.clientIsFull());
         assertEquals(h.getNumberAccepted(), h3.getNumberAccepted());                 
      }    
   }
}