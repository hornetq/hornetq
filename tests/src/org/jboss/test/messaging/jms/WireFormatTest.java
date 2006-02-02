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
import java.io.EOFException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;

import junit.framework.TestCase;

import org.jboss.aop.Dispatcher;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageDelegate;
import org.jboss.jms.server.endpoint.DeliveryRunnable;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvocationResponse;
import org.jboss.remoting.InvokerLocator;
import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;
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
public class WireFormatTest extends TestCase
{
   // Constants -----------------------------------------------------
      
   private static final Logger log = Logger.getLogger(WireFormatTest.class);

   // Static --------------------------------------------------------
      
   // Attributes ----------------------------------------------------
   
   protected TestWireFormat wf;
   
   protected Method sendMethod;
   
   protected Method acknowledgeMethod;
   
   protected Method activateMethod;
   
   protected Method deactivateMethod;
   
   protected Method getMessageNowMethod;

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
      
      Class producerDelegate = ProducerDelegate.class;
      
      Class sessionDelegate = SessionDelegate.class;
      
      Class consumerDelegate = ConsumerDelegate.class;
      
      sendMethod = producerDelegate.getMethod("send", new Class[] { Message.class });
      
      acknowledgeMethod = sessionDelegate.getMethod("acknowledge", null);
      
      activateMethod = consumerDelegate.getMethod("activate", null);
      
      deactivateMethod = consumerDelegate.getMethod("deactivate", null);
      
      getMessageNowMethod = consumerDelegate.getMethod("getMessageNow", new Class[] { Boolean.TYPE });
      
   }

   public void tearDown() throws Exception
   {      
      super.tearDown();
   }
   
   
   public void testAcknowledge() throws Exception
   {
      wf.testAcknowledge();
   }
   
   public void testActivate() throws Exception
   {
      wf.testActivate();
   }
   
   public void testCallback() throws Exception
   {
      wf.testCallback();
   }
   
   public void testDeactivate() throws Exception
   {
      wf.testDeactivate();
   }
      
   public void testExceptionResponse() throws Exception
   {
      wf.testExceptionResponse();
   }
   
   public void testGetMessageNow() throws Exception
   {
      wf.testGetMessageNow();
   }
   
   public void testGetMessageNowResponse() throws Exception
   {
      wf.testGetMessageNowResponse();
   }
   
   public void testNullResponse() throws Exception
   {
      wf.testNullResponse();
   }
   
   public void testSend() throws Exception
   {
      wf.testSend();
   }
   
   public void testSerializableRequest() throws Exception
   {
      wf.testSerializableRequest();
   }
   
   public void testSerializableResponse() throws Exception
   {
      wf.testSerializableResponse();
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
         
   /** We extend the class so we have access to protected fields */
   class TestWireFormat extends JMSWireFormat
   {      
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
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         JBossObjectInputStream ois = new JBossObjectInputStream(bis);
         
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
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         JBossObjectInputStream ois = new JBossObjectInputStream(bis);
         
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
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         JBossObjectInputStream ois = new JBossObjectInputStream(bis);
         
         InvocationResponse ir2 = (InvocationResponse)wf.read(ois, null);
         
         assertNotNull(ir2);
         
         assertEquals("sessionid456", ir2.getSessionId());
         
         assertEquals("wibble3", ir2.getPayload().get("testingkey3"));
         
         assertEquals("Flamingo", ((Exception)ir2.getResult()).getMessage());
         
         assertTrue(ir2.isException());
                 
      }
                  
      public void testSend() throws Exception
      {       
         JBossMessage m = new JBossMessage(new GUID().toString());
         
         MessageTest.configureMessage(m);
         
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, sendMethod, sendMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         mi.setArguments(new Object[] {m});
         
         InvocationRequest ir = new InvocationRequest(null, null, mi, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
                  
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         JBossObjectInputStream ois = new JBossObjectInputStream(bis); 
               
         //Check the bytes
         
         //First byte should be SEND
         assertEquals(JMSWireFormat.SEND, ois.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, ois.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, ois.readLong());
         
         //Next should be be type         
         byte type = ois.readByte();
         
         assertEquals(JBossMessage.TYPE, type);
         
         //Next should come the message
         JBossMessage m2 = new JBossMessage();
         
         m2.readExternal(ois);
         
         //should be eos
         
         try
         {
            ois.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }

         MessageTest.ensureEquivalent(m, m2);
         
         bis.reset();
         ois = new JBossObjectInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         MethodInvocation mi2 = (MethodInvocation)ir2.getParameter();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());
         
         JBossMessage m3 = (JBossMessage)mi2.getArguments()[0];
         
         MessageTest.ensureEquivalent(m, m3);
                  
      }  
      
      public void testNullResponse() throws Exception
      {
         InvocationResponse resp = new InvocationResponse(null, null, false, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
                  
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
         
         wf.write(resp, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         DataInputStream dis = new DataInputStream(bis);
         
         //Should be 1 byte
         byte b = dis.readByte();
         
         assertEquals(JMSWireFormat.NULL_RESPONSE, b);
         
         //Should be eos
         try
         {
            dis.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         dis.reset();
         
         JBossObjectInputStream ois = new JBossObjectInputStream(bis);
         
         InvocationResponse ir2 = (InvocationResponse)wf.read(ois, null);
         
         assertNull(ir2.getResult());
            
      }
         
      public void testGetMessageNow() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, getMessageNowMethod, getMessageNowMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         mi.setArguments(new Object[] {Boolean.valueOf(true)});
         
         InvocationRequest ir = new InvocationRequest(null, null, mi, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
                  
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         JBossObjectInputStream ois = new JBossObjectInputStream(bis); 
         
         //Check the bytes
         
         //First byte should be GETMESSAGENOW
         assertEquals(JMSWireFormat.GETMESSAGENOW, ois.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, ois.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, ois.readLong());
         
         //Next boolean should be wait
         assertEquals(true, ois.readBoolean());
         
         //Now eos
         try
         {
            ois.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         ois = new JBossObjectInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         MethodInvocation mi2 = (MethodInvocation)ir2.getParameter();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());
         
         boolean wait = ((Boolean)mi2.getArguments()[0]).booleanValue();
         
         assertEquals(true, wait);
                          
      }
      
      public void testActivate() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, activateMethod, activateMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         InvocationRequest ir = new InvocationRequest(null, null, mi, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
                  
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         JBossObjectInputStream ois = new JBossObjectInputStream(bis); 
         
         //Check the bytes
         
         //First byte should be ACTIVATE
         assertEquals(JMSWireFormat.ACTIVATE, ois.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, ois.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, ois.readLong());
         
         //Now eos
         try
         {
            ois.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         ois = new JBossObjectInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         MethodInvocation mi2 = (MethodInvocation)ir2.getParameter();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());         
      }
      
      public void testDeactivate() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, deactivateMethod, deactivateMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         InvocationRequest ir = new InvocationRequest(null, null, mi, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
                  
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         JBossObjectInputStream ois = new JBossObjectInputStream(bis); 
                  
         //Check the bytes
         
         //First byte should be ACTIVATE
         assertEquals(JMSWireFormat.DEACTIVATE, ois.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, ois.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, ois.readLong());
         
         //Now eos
         try
         {
            ois.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         ois = new JBossObjectInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         MethodInvocation mi2 = (MethodInvocation)ir2.getParameter();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());         
      }
      
      public void testAcknowledge() throws Exception
      {
         long methodHash = 62365354;
         
         int objectId = 54321;
         
         MethodInvocation mi = new MethodInvocation(null, methodHash, acknowledgeMethod, acknowledgeMethod, null);
         
         mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));   
         
         InvocationRequest ir = new InvocationRequest(null, null, mi, null, null, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
                  
         wf.write(ir, oos);
         
         oos.flush();
         
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         JBossObjectInputStream ois = new JBossObjectInputStream(bis); 
                 
         //Check the bytes
         
         //First byte should be ACKNOWLEDGE
         assertEquals(JMSWireFormat.ACKNOWLEDGE, ois.readByte());
         
         //Next int should be objectId
         assertEquals(objectId, ois.readInt());
         
         //Next long should be methodHash
         assertEquals(methodHash, ois.readLong());
         
         //Now eos
         try
         {
            ois.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         ois = new JBossObjectInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         MethodInvocation mi2 = (MethodInvocation)ir2.getParameter();
         
         assertEquals(methodHash, mi2.getMethodHash());
         
         assertEquals(objectId, ((Integer)mi2.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue());         
      }
      
      public void testCallback() throws Exception
      {
         int consumerID = 12345678;
         
         JBossMessage m = new JBossMessage(new GUID().toString());
         
         MessageDelegate del = JBossMessage.createThinDelegate(m, 7);
         
         MessageTest.configureMessage(m);
         
         DeliveryRunnable dr = new DeliveryRunnable(del, null, consumerID, false);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
         
         InvocationRequest ir = new InvocationRequest(null, null, dr, null, null, null);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                  
         JBossObjectInputStream ois = new JBossObjectInputStream(bis);
         
         //Check the bytes
         
         //First byte should be CALLBACK
         assertEquals(JMSWireFormat.CALLBACK, ois.readByte());
         
         //Next int should be consumer id
         assertEquals(12345678, ois.readInt());
         
         //Next byte should be type
         assertEquals(JBossMessage.TYPE, ois.readByte());
         
         //Next int should be delivery count
         assertEquals(7, ois.readInt());
         
         //And now the message itself
         JBossMessage m2 = new JBossMessage();
         
         m2.readExternal(ois);
         
         MessageTest.ensureEquivalent(m, m2);
         
         //eos
         try
         {
            ois.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         ois = new JBossObjectInputStream(bis);
         
         InvocationRequest ir2 = (InvocationRequest)wf.read(ois, null);
         
         DeliveryRunnable dr2 = (DeliveryRunnable)ir2.getParameter();
         
         MessageDelegate del2 = dr2.getMessageDelegate();
         
         JBossMessage m3 = del2.getMessage();
         
         assertEquals(consumerID, dr2.getConsumerID());
         
         assertEquals(7, del2.getDeliveryCount());
         
         MessageTest.ensureEquivalent(m, m3);
          
      }
      
      public void testGetMessageNowResponse() throws Exception
      {
         JBossMessage m = new JBossMessage(new GUID().toString());
         
         MessageTest.configureMessage(m);
         
         MessageDelegate del = JBossMessage.createThinDelegate(m, 4);
         
         InvocationResponse ir = new InvocationResponse(null, del, false, null);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         JBossObjectOutputStream oos = new JBossObjectOutputStream(bos);
         
         wf.write(ir, oos);
         
         oos.flush();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
         
         JBossObjectInputStream ois = new JBossObjectInputStream(bis);
         
         //First byte should be MESSAGE_RESPONSE
         
         int b = ois.readByte();
         
         assertEquals(JMSWireFormat.MESSAGE_RESPONSE, b);
         
         // Next byte is type
         byte type = ois.readByte();
         
         assertEquals(JBossMessage.TYPE, type);
         
         //Next is delivery count
         int deliveryCount = ois.readInt();
         
         assertEquals(4, deliveryCount);
         
         //And now the message itself
         JBossMessage m2 = new JBossMessage();
         
         m2.readExternal(ois);
         
         MessageTest.ensureEquivalent(m, m2);
         
         //eos
         try
         {
            ois.readByte();
            fail("End of stream expected");
         }
         catch (EOFException e)
         {
            //Ok
         }
         
         bis.reset();
         
         ois = new JBossObjectInputStream(bis);
         
         InvocationResponse ir2 = (InvocationResponse)wf.read(ois, null);
         
         MessageDelegate del2 = (MessageDelegate)ir2.getResult();
         
         JBossMessage m3 = del2.getMessage();
         
         MessageTest.ensureEquivalent(m, m3);                 
         
         assertEquals(4, del2.getDeliveryCount());
      }            
   }
}