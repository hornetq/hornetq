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
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jboss.jms.client.delegate.ClientBrowserDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTemporaryQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.endpoint.Ack;
import org.jboss.jms.server.endpoint.Cancel;
import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.jms.server.endpoint.DefaultAck;
import org.jboss.jms.server.endpoint.DefaultCancel;
import org.jboss.jms.server.endpoint.DeliveryRecovery;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.wireformat.BrowserHasNextMessageRequest;
import org.jboss.jms.wireformat.BrowserHasNextMessageResponse;
import org.jboss.jms.wireformat.BrowserNextMessageBlockRequest;
import org.jboss.jms.wireformat.BrowserNextMessageBlockResponse;
import org.jboss.jms.wireformat.BrowserNextMessageRequest;
import org.jboss.jms.wireformat.BrowserNextMessageResponse;
import org.jboss.jms.wireformat.CloseRequest;
import org.jboss.jms.wireformat.ClosingRequest;
import org.jboss.jms.wireformat.ConnectionCreateSessionDelegateRequest;
import org.jboss.jms.wireformat.ConnectionCreateSessionDelegateResponse;
import org.jboss.jms.wireformat.ConnectionFactoryCreateConnectionDelegateRequest;
import org.jboss.jms.wireformat.ConnectionFactoryCreateConnectionDelegateResponse;
import org.jboss.jms.wireformat.ConnectionFactoryGetClientAOPStackRequest;
import org.jboss.jms.wireformat.ConnectionFactoryGetClientAOPStackResponse;
import org.jboss.jms.wireformat.ConnectionFactoryGetIDBlockRequest;
import org.jboss.jms.wireformat.ConnectionFactoryGetIDBlockResponse;
import org.jboss.jms.wireformat.ConnectionGetClientIDRequest;
import org.jboss.jms.wireformat.ConnectionGetClientIDResponse;
import org.jboss.jms.wireformat.ConnectionGetPreparedTransactionsRequest;
import org.jboss.jms.wireformat.ConnectionGetPreparedTransactionsResponse;
import org.jboss.jms.wireformat.ConnectionSendTransactionRequest;
import org.jboss.jms.wireformat.ConnectionSetClientIDRequest;
import org.jboss.jms.wireformat.ConnectionStartRequest;
import org.jboss.jms.wireformat.ConnectionStopRequest;
import org.jboss.jms.wireformat.ConsumerCancelInflightMessagesRequest;
import org.jboss.jms.wireformat.ConsumerChangeRateRequest;
import org.jboss.jms.wireformat.NullResponse;
import org.jboss.jms.wireformat.PacketSupport;
import org.jboss.jms.wireformat.RequestSupport;
import org.jboss.jms.wireformat.ResponseSupport;
import org.jboss.jms.wireformat.SessionAcknowledgeDeliveriesRequest;
import org.jboss.jms.wireformat.SessionAcknowledgeDeliveryRequest;
import org.jboss.jms.wireformat.SessionAddTemporaryDestinationRequest;
import org.jboss.jms.wireformat.SessionCancelDeliveriesRequest;
import org.jboss.jms.wireformat.SessionCancelDeliveryRequest;
import org.jboss.jms.wireformat.SessionCreateBrowserDelegateRequest;
import org.jboss.jms.wireformat.SessionCreateBrowserDelegateResponse;
import org.jboss.jms.wireformat.SessionCreateConsumerDelegateRequest;
import org.jboss.jms.wireformat.SessionCreateConsumerDelegateResponse;
import org.jboss.jms.wireformat.SessionCreateQueueRequest;
import org.jboss.jms.wireformat.SessionCreateQueueResponse;
import org.jboss.jms.wireformat.SessionCreateTopicRequest;
import org.jboss.jms.wireformat.SessionCreateTopicResponse;
import org.jboss.jms.wireformat.SessionDeleteTemporaryDestinationRequest;
import org.jboss.jms.wireformat.SessionRecoverDeliveriesRequest;
import org.jboss.jms.wireformat.SessionSendRequest;
import org.jboss.jms.wireformat.SessionUnsubscribeRequest;
import org.jboss.messaging.core.plugin.IDBlock;
import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.remoting.InvocationRequest;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class WireFormatTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
      
   // Static --------------------------------------------------------
      
   // Attributes ----------------------------------------------------
   
   private TestWireFormat wf;
   
   // Constructors --------------------------------------------------

   public WireFormatTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("This test should not be run in a remote config");
      }
      
      super.setUp();
      
      wf = new TestWireFormat();            
   }

   public void tearDown() throws Exception
   {      
      super.tearDown();
   }
   
   
   public void testSerialized() throws Exception
   {
      wf.testSerialized();
   }
   
   // Connection factory
   
   public void testConnectionFactoryCreateConnectionDelegate() throws Exception
   {
      wf.testConnectionFactoryCreateConnectionDelegate();
   }
   
   public void testConnectionFactoryGetIDBlock() throws Exception
   {                          
      wf.testConnectionFactoryGetIDBlock();
   }
   
   public void testConnectionFactoryGetClientAOPStack() throws Exception
   {                         
      wf.testConnectionFactoryGetClientAOPStack();
   }
   
   // Connection
   
   public void testConnectionCreateSessionDelegateRequest() throws Exception
   {                     
      wf.testConnectionCreateSessionDelegateRequest();
   }
   
   public void testConnectionGetClientIDRequest() throws Exception
   {                 
      wf.testConnectionGetClientIDRequest();
   }
   
   public void testConnectionSetClientIDRequest() throws Exception
   {                         
      wf.testConnectionSetClientIDRequest();      
   }
   
   public void testConnectionStartRequest() throws Exception
   {                         
      wf.testConnectionStartRequest();
   }
   
   public void testConnectionStopRequest() throws Exception
   {                          
      wf.testConnectionStopRequest();
   }
   
   public void testConnectionSendTransactionRequest() throws Exception
   {              
      wf.testConnectionSendTransactionRequest();
   }      
   
   public void testConnectionGetPreparedTransactionsRequest() throws Exception
   {              
      wf.testConnectionGetPreparedTransactionsRequest();
   }  
   
   // Session
   
   public void testSessionCreateConsumerDelegateRequest() throws Exception
   {                     
      wf.testSessionCreateConsumerDelegateRequest();
   } 
   
   public void testSessionCreateBrowserDelegateRequest() throws Exception
   {                        
      wf.testSessionCreateBrowserDelegateRequest();
   }
   
   public void testSessionCreateQueueRequest() throws Exception
   {           
      wf.testSessionCreateQueueRequest();
   }      
   
   public void testSessionCreateTopicRequest() throws Exception
   {           
      wf.testSessionCreateTopicRequest();
   }
   
   public void testSessionAcknowledgeDeliveriesRequest() throws Exception
   {                 
      wf.testSessionCreateTopicRequest();
   }   
   
   public void testSessionAcknowledgeDeliveryRequest() throws Exception
   {                
      wf.testSessionAcknowledgeDeliveryRequest();
   } 
   
   public void testSessionCancelDeliveriesRequest() throws Exception
   {                       
      wf.testSessionCancelDeliveriesRequest();
   }   
   
   public void testSessionCancelDeliveryRequest() throws Exception
   {           
      wf.testSessionCancelDeliveryRequest();
   } 
   
   public void testSessionAddTemporaryDestinationRequest() throws Exception
   {         
      wf.testSessionAddTemporaryDestinationRequest();
   } 
   
   public void testSessionDeleteTemporaryDestinationRequest() throws Exception
   {             
      wf.testSessionDeleteTemporaryDestinationRequest();
   } 
   
   public void testSessionUnsubscribeRequest() throws Exception
   {            
      wf.testSessionUnsubscribeRequest();
   }
   
   public void testSessionSendRequest() throws Exception
   {          
      wf.testSessionSendRequest();
   }
   
   public void testSessionRecoverDeliveriesRequest() throws Exception
   {            
      wf.testSessionRecoverDeliveriesRequest();
   }
   
   // Consumer
   
   public void testConsumerChangeRateRequest() throws Exception
   {                         
      wf.testConsumerChangeRateRequest();
   }
   
   public void testConsumerCancelInflightMessagesRequest() throws Exception
   {                       
      wf.testConsumerCancelInflightMessagesRequest();
   }
   
   // Browser
   
   public void testBrowserNextMessageRequest() throws Exception
   {                        
      wf.testBrowserNextMessageRequest();
   }
   
   public void testBrowserHasNextMessageRequest() throws Exception
   {                           
      wf.testBrowserHasNextMessageRequest();
   }
   
   public void testBrowserNextMessageBlockRequest() throws Exception
   {                    
      wf.testBrowserNextMessageBlockRequest();
   }
   
   
   public void testClosingRequest() throws Exception
   {                    
      wf.testClosingRequest();
   }
   
   public void testCloseRequest() throws Exception
   {            
      wf.testCloseRequest();
   }
   
   
   // Responses
   
   // Connection Factory
   
   public void testConnectionFactoryCreateConnectionDelegateResponse() throws Exception
   {            
      wf.testConnectionFactoryCreateConnectionDelegateResponse();
   }
   
   public void testConnectionFactoryGetIDBlockResponse() throws Exception
   {                         
      wf.testConnectionFactoryGetIDBlockResponse();
   }
   
   public void testConnectionFactoryGetClientAOPStackResponse() throws Exception
   {                        
      wf.testConnectionFactoryGetClientAOPStackResponse();
   }
   
   // Connection
   
   public void testConnectionCreateSessionDelegateResponse() throws Exception
   {                   
      wf.testConnectionCreateSessionDelegateResponse();      
   }
   
   public void testConnectionGetClientIDResponse() throws Exception
   {                            
      wf.testConnectionGetClientIDResponse();
   }
   
   public void testConnectionGetClientPreparedTransactionsResponse() throws Exception
   {                      
      wf.testConnectionGetClientPreparedTransactionsResponse();
   }
   
   
   
   // Session
   
   public void testSessionCreateConsumerDelegateResponse() throws Exception
   {                        
      wf.testSessionCreateConsumerDelegateResponse();
   }
   
   
   public void testSessionCreateBrowserDelegateResponse() throws Exception
   {                    
      wf.testSessionCreateBrowserDelegateResponse();
   }
   
   public void testSessionCreateQueueResponse() throws Exception
   {                          
      wf.testSessionCreateQueueResponse();
   }
   
   public void testSessionCreateTopicResponse() throws Exception
   {                            
      wf.testSessionCreateTopicResponse();
   }
   
   // Browser
   
   public void testBrowserNextMessageResponse() throws Exception
   {                          
      wf.testBrowserNextMessageResponse();
   }
   
   public void testBrowserHasNextMessageResponse() throws Exception
   {                           
      wf.testBrowserHasNextMessageResponse();
   }
   
   public void testBrowserNextMessageBlockResponse() throws Exception
   {                            
      wf.testBrowserNextMessageBlockResponse();
   }
   
   public void testNullResponse() throws Exception
   {                 
      wf.testNullResponse();
   }
   
   
   //We just check the first byte to make sure serialization is not be used.
   
   private class TestWireFormat extends JMSWireFormat
   {      
      private void testPacket(PacketSupport req, int id) throws Exception
      {
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);
         
         InvocationRequest ir = new InvocationRequest("session123", null, req, null, null, null);   
         
         wf.write(ir, oos);
                  
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         DataInputStream dis = new DataInputStream(bis); 
                 
         int theId = dis.readInt();
         
         assertEquals(id, theId);
      }
      
      public void testSerialized() throws Exception
      {
         Serializable obj = new SerializableObject("uyuiyiu", 234234);
         
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         OutputStream oos = new DataOutputStream(bos);

         wf.write(obj, oos);
                  
         byte[] bytes = bos.toByteArray();
         
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                  
         DataInputStream dis = new DataInputStream(bis); 
                 
         int theId = dis.readInt();
         
         assertEquals(PacketSupport.SERIALIZED, theId);
      }
      
      // Requests
      
      // Connection Factory
      
      public void testConnectionFactoryCreateConnectionDelegate() throws Exception
      {
         RequestSupport req =
            new ConnectionFactoryCreateConnectionDelegateRequest(23, (byte)77, "session123", "vm123", null, null, -1);;
                 
         testPacket(req, PacketSupport.REQ_CONNECTIONFACTORY_CREATECONNECTIONDELEGATE);                           
      }
      
      public void testConnectionFactoryGetIDBlock() throws Exception
      {
         RequestSupport req =
            new ConnectionFactoryGetIDBlockRequest(23, (byte)77, 66);;
                 
         testPacket(req, PacketSupport.REQ_CONNECTIONFACTORY_GETIDBLOCK);                           
      }
      
      public void testConnectionFactoryGetClientAOPStack() throws Exception
      {
         RequestSupport req =
            new ConnectionFactoryGetClientAOPStackRequest(23, (byte)77);;
                 
         testPacket(req, PacketSupport.REQ_CONNECTIONFACTORY_GETCLIENTAOPSTACK);                           
      }
      
      // Connection
      
      public void testConnectionCreateSessionDelegateRequest() throws Exception
      {
         RequestSupport req =
            new ConnectionCreateSessionDelegateRequest(23, (byte)77, true, 23, true);;
                 
         testPacket(req, PacketSupport.REQ_CONNECTION_CREATESESSIONDELEGATE);                           
      }
      
      public void testConnectionGetClientIDRequest() throws Exception
      {
         RequestSupport req =
            new ConnectionGetClientIDRequest(23, (byte)77);;
                 
         testPacket(req, PacketSupport.REQ_CONNECTION_GETCLIENTID);                           
      }
      
      public void testConnectionSetClientIDRequest() throws Exception
      {
         RequestSupport req =
            new ConnectionSetClientIDRequest(23, (byte)77, "blah");;
                 
         testPacket(req, PacketSupport.REQ_CONNECTION_SETCLIENTID);                           
      }
      
      public void testConnectionStartRequest() throws Exception
      {
         RequestSupport req =
            new ConnectionStartRequest(23, (byte)77);;
                 
         testPacket(req, PacketSupport.REQ_CONNECTION_START);                           
      }
      
      public void testConnectionStopRequest() throws Exception
      {
         RequestSupport req =
            new ConnectionStopRequest(23, (byte)77);;
                 
         testPacket(req, PacketSupport.REQ_CONNECTION_STOP);                           
      }
      
      public void testConnectionSendTransactionRequest() throws Exception
      {
         ClientTransaction tx = new ClientTransaction();
         
         TransactionRequest tr = new TransactionRequest(TransactionRequest.ONE_PHASE_COMMIT_REQUEST, null, tx);
         
         RequestSupport req =
            new ConnectionSendTransactionRequest(23, (byte)77, tr);
                 
         testPacket(req, PacketSupport.REQ_CONNECTION_SENDTRANSACTION);                           
      }      
      
      public void testConnectionGetPreparedTransactionsRequest() throws Exception
      {
         RequestSupport req =
            new ConnectionGetPreparedTransactionsRequest(23, (byte)77);
                 
         testPacket(req, PacketSupport.REQ_CONNECTION_GETPREPAREDTRANSACTIONS);                           
      }  
      
      // Session
      
      public void testSessionCreateConsumerDelegateRequest() throws Exception
      {
         RequestSupport req =
            new SessionCreateConsumerDelegateRequest(23, (byte)77, new JBossQueue("wibble"), null, false, null, false, -1);
                 
         testPacket(req, PacketSupport.REQ_SESSION_CREATECONSUMERDELEGATE);                           
      } 
      
      public void testSessionCreateBrowserDelegateRequest() throws Exception
      {
         RequestSupport req =
            new SessionCreateBrowserDelegateRequest(23, (byte)77, new JBossQueue("wibble"), null, -1);
                 
         testPacket(req, PacketSupport.REQ_SESSION_CREATEBROWSERDELEGATE);                           
      }
      
      public void testSessionCreateQueueRequest() throws Exception
      {
         RequestSupport req =
            new SessionCreateQueueRequest(23, (byte)77, "wibble");
                 
         testPacket(req, PacketSupport.REQ_SESSION_CREATEQUEUE);                           
      }      
      
      public void testSessionCreateTopicRequest() throws Exception
      {
         RequestSupport req =
            new SessionCreateTopicRequest(23, (byte)77, "wibble");
                 
         testPacket(req, PacketSupport.REQ_SESSION_CREATETOPIC);                           
      }
      
      public void testSessionAcknowledgeDeliveriesRequest() throws Exception
      {
         List acks = new ArrayList();
         
         acks.add(new DefaultAck(12323));
         
         RequestSupport req =
            new SessionAcknowledgeDeliveriesRequest(23, (byte)77, acks);
                 
         testPacket(req, PacketSupport.REQ_SESSION_ACKNOWLEDGEDELIVERIES);                           
      }   
      
      public void testSessionAcknowledgeDeliveryRequest() throws Exception
      {
         Ack ack = new DefaultAck(12323);
         
         RequestSupport req =
            new SessionAcknowledgeDeliveryRequest(23, (byte)77, ack);
                 
         testPacket(req, PacketSupport.REQ_SESSION_ACKNOWLEDGEDELIVERY);                           
      } 
      
      public void testSessionCancelDeliveriesRequest() throws Exception
      {
         List cancels = new ArrayList();
         
         cancels.add(new DefaultCancel(12323, 12, false, false));
         
         RequestSupport req =
            new SessionCancelDeliveriesRequest(23, (byte)77, cancels);
                 
         testPacket(req, PacketSupport.REQ_SESSION_CANCELDELIVERIES);                           
      }   
      
      public void testSessionCancelDeliveryRequest() throws Exception
      {
         Cancel cancel = (new DefaultCancel(12323, 12, false, false));
         
         RequestSupport req =
            new SessionCancelDeliveryRequest(23, (byte)77, cancel);
                 
         testPacket(req, PacketSupport.REQ_SESSION_CANCELDELIVERY);                           
      } 
      
      public void testSessionAddTemporaryDestinationRequest() throws Exception
      {
         RequestSupport req =
            new SessionAddTemporaryDestinationRequest(23, (byte)77, new JBossTemporaryQueue("blah"));
                 
         testPacket(req, PacketSupport.REQ_SESSION_ADDTEMPORARYDESTINATION);                           
      } 
      
      public void testSessionDeleteTemporaryDestinationRequest() throws Exception
      {
         RequestSupport req =
            new SessionDeleteTemporaryDestinationRequest(23, (byte)77, new JBossTemporaryQueue("blah"));
                 
         testPacket(req, PacketSupport.REQ_SESSION_DELETETEMPORARYDESTINATION);                           
      } 
      
      public void testSessionUnsubscribeRequest() throws Exception
      {
         RequestSupport req =
            new SessionUnsubscribeRequest(23, (byte)77, "blah");
                 
         testPacket(req, PacketSupport.REQ_SESSION_UNSUBSCRIBE);                           
      }
      
      public void testSessionSendRequest() throws Exception
      {
         JBossMessage msg = new JBossMessage(123);
         
         RequestSupport req =
            new SessionSendRequest(23, (byte)77, msg);
                 
         testPacket(req, PacketSupport.REQ_SESSION_SEND);                           
      }
      
      public void testSessionRecoverDeliveriesRequest() throws Exception
      { 
         List dels = new ArrayList();
         
         DeliveryRecovery info = new DeliveryRecovery();
         
         dels.add(info);
         
         RequestSupport req =
            new SessionRecoverDeliveriesRequest(23, (byte)77, dels);
                 
         testPacket(req, PacketSupport.REQ_SESSION_RECOVERDELIVERIES);                           
      }
      
      // Consumer
      
      public void testConsumerChangeRateRequest() throws Exception
      { 
         RequestSupport req =
            new ConsumerChangeRateRequest(23, (byte)77, 123.23f);
                 
         testPacket(req, PacketSupport.REQ_CONSUMER_CHANGERATE);                           
      }
      
      public void testConsumerCancelInflightMessagesRequest() throws Exception
      { 
         RequestSupport req =
            new ConsumerCancelInflightMessagesRequest(23, (byte)77, 123);
                 
         testPacket(req, PacketSupport.REQ_CONSUMER_CANCELINFLIGHTMESSAGES);                           
      }
      
      // Browser
      
      public void testBrowserNextMessageRequest() throws Exception
      { 
         RequestSupport req =
            new BrowserNextMessageRequest(23, (byte)77);
                 
         testPacket(req, PacketSupport.REQ_BROWSER_NEXTMESSAGE);                           
      }
      
      public void testBrowserHasNextMessageRequest() throws Exception
      { 
         RequestSupport req =
            new BrowserHasNextMessageRequest(23, (byte)77);
                 
         testPacket(req, PacketSupport.REQ_BROWSER_HASNEXTMESSAGE);                           
      }
      
      public void testBrowserNextMessageBlockRequest() throws Exception
      { 
         RequestSupport req =
            new BrowserNextMessageBlockRequest(23, (byte)77, 123);
                 
         testPacket(req, PacketSupport.REQ_BROWSER_NEXTMESSAGEBLOCK);                           
      }
      
      
      public void testClosingRequest() throws Exception
      { 
         RequestSupport req =  new ClosingRequest(23, (byte)77);
                 
         testPacket(req, PacketSupport.REQ_CLOSING);                           
      }
      
      public void testCloseRequest() throws Exception
      { 
         RequestSupport req =  new CloseRequest(23, (byte)77);
                 
         testPacket(req, PacketSupport.REQ_CLOSE);                           
      }
      
      
      // Responses
      
      // Connection Factory
      
      public void testConnectionFactoryCreateConnectionDelegateResponse() throws Exception
      { 
         CreateConnectionResult res = new CreateConnectionResult(123);
         
         ResponseSupport resp =
            new ConnectionFactoryCreateConnectionDelegateResponse(res);
                 
         testPacket(resp, PacketSupport.RESP_CONNECTIONFACTORY_CREATECONNECTIONDELEGATE);                           
      }
      
      public void testConnectionFactoryGetIDBlockResponse() throws Exception
      { 
         IDBlock block = new IDBlock(1, 76);
         
         ResponseSupport resp =
            new ConnectionFactoryGetIDBlockResponse(block);
                 
         testPacket(resp, PacketSupport.RESP_CONNECTIONFACTORY_GETIDBLOCK);                           
      }
      
      public void testConnectionFactoryGetClientAOPStackResponse() throws Exception
      { 
         String s = "ioqjwoijqsdoijqdoij";
         
         byte[] bytes = s.getBytes();
         
         ResponseSupport resp =
            new ConnectionFactoryGetClientAOPStackResponse(bytes);
                 
         testPacket(resp, PacketSupport.RESP_CONNECTIONFACTORY_GETCLIENTAOPSTACK);                           
      }
      
      // Connection
      
      public void testConnectionCreateSessionDelegateResponse() throws Exception
      { 
         ClientSessionDelegate del = new ClientSessionDelegate(786);
         
         ResponseSupport resp =
            new ConnectionCreateSessionDelegateResponse(del);
                 
         testPacket(resp, PacketSupport.RESP_CONNECTION_CREATESESSIONDELEGATE);                           
      }
      
      public void testConnectionGetClientIDResponse() throws Exception
      { 
         ResponseSupport resp =
            new ConnectionGetClientIDResponse("isij");
                 
         testPacket(resp, PacketSupport.RESP_CONNECTION_GETCLIENTID);                           
      }
      
      public void testConnectionGetClientPreparedTransactionsResponse() throws Exception
      { 
         MessagingXid xid = new MessagingXid("okokko".getBytes(), 23, "okjokokdd".getBytes());         
                  
         ResponseSupport resp =
            new ConnectionGetPreparedTransactionsResponse(new MessagingXid[] { xid });
                 
         testPacket(resp, PacketSupport.RESP_CONNECTION_GETPREPAREDTRANSACTIONS);                           
      }
      
      
      
      // Session
      
      public void testSessionCreateConsumerDelegateResponse() throws Exception
      { 
         ClientConsumerDelegate del = new ClientConsumerDelegate(786, 13123, 213, 123);
         
         ResponseSupport resp =
            new SessionCreateConsumerDelegateResponse(del);
                 
         testPacket(resp, PacketSupport.RESP_SESSION_CREATECONSUMERDELEGATE);                           
      }
      
      
      public void testSessionCreateBrowserDelegateResponse() throws Exception
      { 
         ClientBrowserDelegate del = new ClientBrowserDelegate(786, 12);
         
         ResponseSupport resp =
            new SessionCreateBrowserDelegateResponse(del);
                 
         testPacket(resp, PacketSupport.RESP_SESSION_CREATEBROWSERDELEGATE);                           
      }
      
      public void testSessionCreateQueueResponse() throws Exception
      { 
         ResponseSupport resp =
            new SessionCreateQueueResponse(new JBossQueue("ijoij"));
                 
         testPacket(resp, PacketSupport.RESP_SESSION_CREATEQUEUE);                           
      }
      
      public void testSessionCreateTopicResponse() throws Exception
      { 
         ResponseSupport resp =
            new SessionCreateTopicResponse(new JBossTopic("ijoij"));
                 
         testPacket(resp, PacketSupport.RESP_SESSION_CREATETOPIC);                           
      }
      
      // Browser
      
      public void testBrowserNextMessageResponse() throws Exception
      { 
         JBossMessage msg = new JBossMessage(123);
         
         ResponseSupport resp =
            new BrowserNextMessageResponse(msg);
                 
         testPacket(resp, PacketSupport.RESP_BROWSER_NEXTMESSAGE);                           
      }
      
      public void testBrowserHasNextMessageResponse() throws Exception
      { 
         ResponseSupport resp =
            new BrowserHasNextMessageResponse(true);
                 
         testPacket(resp, PacketSupport.RESP_BROWSER_HASNEXTMESSAGE);                           
      }
      
      public void testBrowserNextMessageBlockResponse() throws Exception
      { 
         JBossMessage msg = new JBossMessage(123);
         
         ResponseSupport resp =
            new BrowserNextMessageBlockResponse(new JBossMessage[] { msg });
                 
         testPacket(resp, PacketSupport.RESP_BROWSER_NEXTMESSAGEBLOCK);                           
      }
      
      public void testNullResponse() throws Exception
      { 
         ResponseSupport resp =  new NullResponse();
                 
         testPacket(resp, PacketSupport.NULL_RESPONSE);                           
      }
      
   }
   
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
}