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
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.server.endpoint.Ack;
import org.jboss.jms.server.endpoint.Cancel;
import org.jboss.jms.server.endpoint.ClientDelivery;
import org.jboss.jms.server.endpoint.DefaultAck;
import org.jboss.jms.server.endpoint.DefaultCancel;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.tx.ClientTransaction.SessionTxState;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.IDBlock;
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
   
   protected Method acknowledgeDeliveryMethod;
   
   protected Method acknowledgeDeliveriesMethod;
   
   protected Method cancelDeliveryMethod;
   
   protected Method cancelDeliveriesMethod;      
   
   //Consumer
        
   protected Method changeRateMethod;
   
   protected Method cancelInflightMessagesMethod;
   
 
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
       
      acknowledgeDeliveryMethod = sessionDelegate.getMethod("acknowledgeDelivery", new Class[] { Ack.class });
      
      acknowledgeDeliveriesMethod = sessionDelegate.getMethod("acknowledgeDeliveries", new Class[] { java.util.List.class });
      
      cancelDeliveryMethod = sessionDelegate.getMethod("cancelDelivery", new Class[] { Cancel.class });
            
      cancelDeliveriesMethod = sessionDelegate.getMethod("cancelDeliveries", new Class[] { java.util.List.class });
      
      //TODO - this isn't complete - there are other methods to test
            
      //Consumer
            
      changeRateMethod = consumerDelegate.getMethod("changeRate", new Class[] { Float.TYPE });
      
      cancelInflightMessagesMethod = consumerDelegate.getMethod("cancelInflightMessages", new Class[] { Long.TYPE });

      //Connection
      
      sendTransactionMethod = connectionDelegate.getMethod("sendTransaction", new Class[] { TransactionRequest.class });
      
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {      
      super.tearDown();
   }
   
   //Session
   
   public void testAcknowledgeDelivery() throws Exception
   {
      wf.testAcknowledgeDelivery();
   }
   
   public void testAcknowledgeDeliveries() throws Exception
   {
      wf.testAcknowledgeDeliveries();
   }
   
   public void testCancelDelivery() throws Exception
   {
      wf.testCancelDelivery();
   }
   
   public void testCancelDeliveries() throws Exception
   {
      wf.testCancelDeliveries();
   }
   
   public void testSend() throws Exception
   {
      wf.testSend();
   }
   
   //Consumer
   
   public void testChangeRate() throws Exception
   {
      wf.testChangeRate();
   }
   
   public void testCancelInflightMessages() throws Exception
   {
      wf.testCancelInflightMessages();
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
   
   public void testMessageDelivery() throws Exception
   {
      wf.testMessageDelivery();
   }
   
   public void testIDBlockResponse() throws Exception
   {
      wf.testGetIdBlockResponse();
   }
   
   //TODO need a test for the polled callbacks
          
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
   private class TestWireFormat extends JMSWireFormat
   {      
      public void testAcknowledgeDelivery() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, acknowledgeDeliveryMethod, acknowledgeDeliveryMethod, null);
         
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
         
         assertEquals(deliveryID, l2.getDeliveryID());
         
      }
      
      public void testAcknowledgeDeliveries() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, acknowledgeDeliveriesMethod, acknowledgeDeliveriesMethod, null);
         
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
         assertEquals(JMSWireFormat.ACKNOWLEDGE_LIST, dis.readByte());
         
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
         
         assertEquals(ackA, ((DefaultAck)acks2.get(0)).getDeliveryID());
         assertEquals(ackB, ((DefaultAck)acks2.get(1)).getDeliveryID());
         assertEquals(ackC, ((DefaultAck)acks2.get(2)).getDeliveryID());
         
      }
      
      public void testCancelDelivery() throws Exception
      {
         long methodHash = 6236354;
         
         int objectId = 543271;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, cancelDeliveryMethod, cancelDeliveryMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         long deliveryID = 765;
         
         int deliveryCount = 12;
         
         Cancel cancel = new DefaultCancel(deliveryID, deliveryCount, true, true);
         
         Object[] args = new Object[] { cancel };
         
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
         
         //First byte should be CANCEL
         assertEquals(JMSWireFormat.CANCEL, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
         
         //Next should be the deliveryid
         long l = dis.readLong();
         
         //Then delivery count
         int count = dis.readInt();
         
         boolean expired = dis.readBoolean();
         
         boolean reachedMaxDeliveries = dis.readBoolean();
         
         assertEquals(deliveryID, l);
         
         assertEquals(deliveryCount, count);
         
         assertEquals(expired, true);
         
         assertEquals(reachedMaxDeliveries, true);

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
         
         Cancel l2 = (Cancel)mi2.getArguments()[0];
         
         assertEquals(deliveryID, l2.getDeliveryId());
         
         assertEquals(deliveryCount, l2.getDeliveryCount());
         
         assertEquals(expired, l2.isExpired());
         
         assertEquals(reachedMaxDeliveries, l2.isReachedMaxDeliveryAttempts());
         
      }
      
      public void testCancelDeliveries() throws Exception
      {                            
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         List cancels = new ArrayList();
         
         DefaultCancel cancel1 = new DefaultCancel(65654, 43, true, false);
         DefaultCancel cancel2 = new DefaultCancel(65765, 2, false, true);
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
         assertEquals(JMSWireFormat.CANCEL_LIST, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
                  
         //Next should the size of the list
         
         int size = dis.readInt();
         
         assertEquals(2, size);
         
         //then the AckInfos
         long deliveryId = dis.readLong();
         int deliveryCount = dis.readInt();
         boolean expired = dis.readBoolean();
         boolean reachedMaxDeliveries = dis.readBoolean();
         DefaultCancel rcancel1 = new DefaultCancel(deliveryId, deliveryCount, expired, reachedMaxDeliveries);
         
         deliveryId = dis.readLong();
         deliveryCount = dis.readInt();
         expired = dis.readBoolean();
         reachedMaxDeliveries = dis.readBoolean();
         DefaultCancel rcancel2 = new DefaultCancel(deliveryId, deliveryCount, expired, reachedMaxDeliveries);

         assertEquals(cancel1.getDeliveryCount(), rcancel1.getDeliveryCount());
         
         assertEquals(cancel1.getDeliveryId(), cancel1.getDeliveryId());
         
         assertEquals(cancel1.isExpired(), cancel1.isExpired());
         
         assertEquals(cancel1.isReachedMaxDeliveryAttempts(), cancel1.isReachedMaxDeliveryAttempts());
         
         assertEquals(cancel2.getDeliveryCount(), rcancel2.getDeliveryCount());
         
         assertEquals(cancel2.getDeliveryId(), cancel2.getDeliveryId());
         
         assertEquals(cancel2.isExpired(), cancel2.isExpired());
         
         assertEquals(cancel2.isReachedMaxDeliveryAttempts(), cancel2.isReachedMaxDeliveryAttempts());
                   
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
         
         DefaultCancel xack1 = (DefaultCancel)list.get(0);
         DefaultCancel xack2 = (DefaultCancel)list.get(1);
         
         assertEquals(cancel1.getDeliveryId(), xack1.getDeliveryId());
         
         assertEquals(cancel1.getDeliveryCount(), xack1.getDeliveryCount());
         
         assertEquals(cancel2.getDeliveryId(), xack2.getDeliveryId());
         
         assertEquals(cancel2.getDeliveryCount(), xack2.getDeliveryCount());
         
      }  
      
      public void testCancelInflightMessages() throws Exception
      {
         long methodHash = 6236354;
         
         int objectId = 543271;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, cancelInflightMessagesMethod, cancelInflightMessagesMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         long lastDeliveryId = 765;
         
         Object[] args = new Object[] { new Long(lastDeliveryId) };
         
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
         
         //First byte should be CANCEL
         assertEquals(JMSWireFormat.CANCEL_INFLIGHT_MESSAGES, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
         
         //Next should be the lastdeliveryid
         long l = dis.readLong();
         
         assertEquals(lastDeliveryId, l);
         
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
         
         Long l2 = (Long)mi2.getArguments()[0];
         
         assertEquals(lastDeliveryId, l2.longValue());
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
                     
         DeliveryInfo info = new DeliveryInfo(proxy, 76762, 98982, null);
         
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
         
         assertEquals(deliveryId, ack.getDeliveryID());
         
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
         
         assertEquals(deliveryId, ack2.getDeliveryID());
         
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
                  
      public void testChangeRate() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         float rate = 123.45f;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, changeRateMethod, changeRateMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId)); 
         
         Object[] args = new Object[] { new Float(rate) };
         
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
         
         //Second byte should be CHANGE_RATE
         assertEquals(JMSWireFormat.CHANGE_RATE, dis.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, dis.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, dis.readLong());
         
         //Next should be the float
         float f2 = dis.readFloat();
         
         assertTrue(rate == f2);
         
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
         
         Float f3 = (Float)mi2.getArguments()[0];
         
         assertTrue(rate == f3.floatValue());
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());         
      }
      
            
      public void testMessageDelivery() throws Exception
      {
         int consumerID = 12345678;
         
         JBossMessage m1 = new JBossMessage(123);

         MessageProxy del1 = JBossMessage.createThinDelegate(1, m1, 7);
         
         MessageTest.configureMessage(m1);

         ClientDelivery dr = new ClientDelivery(del1, consumerID);
         
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
         
         //Second byte should be MESSAGE_DELIVERY
         assertEquals(JMSWireFormat.MESSAGE_DELIVERY, dis.readByte());
         
         //Next should be sessionID
         assertEquals("dummySessionId", dis.readUTF());
         
         //Next int should be consumer id
         assertEquals(12345678, dis.readInt());
                     
         //Next byte should be type
         assertEquals(JBossMessage.TYPE, dis.readByte());
         
         //Next int should be delivery count
         assertEquals(7, dis.readInt());
         
         //Delivery id
         assertEquals(1, dis.readLong());
         
         //And now the message itself
         JBossMessage r1 = new JBossMessage();
         
         r1.read(dis);         
         
         MessageTest.ensureEquivalent(m1, r1);
         
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
         
         MessageProxy p1 = dr2.getMessage();
         
         assertEquals(consumerID, dr2.getConsumerId());
         

         assertEquals(del1.getDeliveryCount(), p1.getDeliveryCount());

         JBossMessage q1 = p1.getMessage();
  
         MessageTest.ensureEquivalent(m1, q1);      
      }
      
                  
      public void testGetIdBlockResponse() throws Exception
      {
         IDBlock block = new IDBlock(132, 465);
         
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
         
         IDBlock block2 = new IDBlock();
         
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
         
         IDBlock block3 = (IDBlock)mm.getLoad();
         
         assertEquals(block.getLow(), block3.getLow());
         assertEquals(block.getHigh(), block3.getHigh());                  
      }    
            
   }
}