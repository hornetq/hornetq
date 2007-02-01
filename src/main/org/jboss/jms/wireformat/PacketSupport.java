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
package org.jboss.jms.wireformat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.util.Streamable;
import org.jboss.remoting.Client;

/**
 * A PacketSupport
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class PacketSupport implements Streamable
{
   private static final int NULL = 0;
   
   private static final int NOT_NULL = 1;
   
   protected static Map ONE_WAY_METADATA;
   
   static
   {
      ONE_WAY_METADATA = new HashMap();
      ONE_WAY_METADATA.put(Client.ONEWAY_FLAG, "true");
   }
   
   // First the non request or response packet codes
   // ==============================================
   
   // General serialized object - only used for remoting internal invocations and pings
   public static final int SERIALIZED = 1;   
   
   // A message delivery from server to client
   public static final int CLIENT_DELIVERY = 2;
   
   // A connection factory update message for failover
   public static final int CONNECTIONFACTORY_UPDATE = 3;
   
   // Delivery of a list of polled callbacks - e.g. for HTTP transport
   public static final int POLLEDCALLBACKS_DELIVERY = 4;
   
   
   // Then the request codes
   // ======================
   
   // ConnectionFactory   
   // -----------------      
   
   public static final int REQ_CONNECTIONFACTORY_CREATECONNECTIONDELEGATE = 100;
   public static final int REQ_CONNECTIONFACTORY_GETIDBLOCK = 101;
   public static final int REQ_CONNECTIONFACTORY_GETCLIENTAOPSTACK = 102;
   
   // Connection
   // ----------
   
   public static final int REQ_CONNECTION_CREATESESSIONDELEGATE = 201;
   public static final int REQ_CONNECTION_GETCLIENTID = 202;
   public static final int REQ_CONNECTION_SETCLIENTID = 203;
   public static final int REQ_CONNECTION_START = 204;
   public static final int REQ_CONNECTION_STOP = 205;
   public static final int REQ_CONNECTION_SENDTRANSACTION = 206;
   public static final int REQ_CONNECTION_GETPREPAREDTRANSACTIONS = 207;
   
   // Session
   // -------
   
   public static final int REQ_SESSION_CREATECONSUMERDELEGATE = 301;
   public static final int REQ_SESSION_CREATEBROWSERDELEGATE = 302;
   public static final int REQ_SESSION_CREATEQUEUE = 303;
   public static final int REQ_SESSION_CREATETOPIC = 304;
   public static final int REQ_SESSION_ACKNOWLEDGEDELIVERIES = 305;
   public static final int REQ_SESSION_ACKNOWLEDGEDELIVERY = 306;
   public static final int REQ_SESSION_CANCELDELIVERIES = 307;
   public static final int REQ_SESSION_CANCELDELIVERY = 308;
   public static final int REQ_SESSION_ADDTEMPORARYDESTINATION = 309;
   public static final int REQ_SESSION_DELETETEMPORARYDESTINATION = 310;
   public static final int REQ_SESSION_UNSUBSCRIBE = 311;
   public static final int REQ_SESSION_SEND = 312;
   public static final int REQ_SESSION_RECOVERDELIVERIES = 313;
   
   // Consumer
   // --------
   
   public static final int REQ_CONSUMER_CHANGERATE = 401;
   public static final int REQ_CONSUMER_CANCELINFLIGHTMESSAGES = 402;
   
   // Browser
   // -------
   
   public static final int REQ_BROWSER_NEXTMESSAGE = 501;
   public static final int REQ_BROWSER_HASNEXTMESSAGE = 502;
   public static final int REQ_BROWSER_NEXTMESSAGEBLOCK = 503;
   
   // Closeable
   // ---------
   
   public static final int REQ_CLOSING = 601;
   public static final int REQ_CLOSE = 602;
   
   
   // And now the response codes   
   // ==========================
   
   public static final int NULL_RESPONSE = 100001;
   
   // Connection factory
   // -----------------------------------
   
   public static final int RESP_CONNECTIONFACTORY_CREATECONNECTIONDELEGATE = 100100;   
   public static final int RESP_CONNECTIONFACTORY_GETIDBLOCK = 100101;   
   public static final int RESP_CONNECTIONFACTORY_GETCLIENTAOPSTACK = 100102;
      
   // Connection 
   // -------------------------------------
   
   public static final int RESP_CONNECTION_CREATESESSIONDELEGATE = 100200;   
   public static final int RESP_CONNECTION_GETCLIENTID = 100201;   
   public static final int RESP_CONNECTION_GETPREPAREDTRANSACTIONS = 100202;
   
   // Session 
   // -------------------------------------
   
   public static final int RESP_SESSION_CREATECONSUMERDELEGATE = 100300;   
   public static final int RESP_SESSION_CREATEBROWSERDELEGATE = 100301;   
   public static final int RESP_SESSION_CREATEQUEUE = 100302;   
   public static final int RESP_SESSION_CREATETOPIC = 100303;
   
   // Browser
   // -----------------------
   
   public static final int RESP_BROWSER_NEXTMESSAGE = 100500;   
   public static final int RESP_BROWSER_HASNEXTMESSAGE = 100501;   
   public static final int RESP_BROWSER_NEXTMESSAGEBLOCK = 100502;
  
   
   public static PacketSupport createPacket(int id)
   {
      PacketSupport packet;
      
      switch (id)
      {
         //We put the performance critical ones at the top
         case CLIENT_DELIVERY:
            packet = new ClientDelivery();
            break;
         case REQ_SESSION_SEND:
            packet = new SessionSendRequest();
            break; 
         case REQ_SESSION_ACKNOWLEDGEDELIVERY:
            packet = new SessionAcknowledgeDeliveryRequest();
            break; 
         case REQ_SESSION_ACKNOWLEDGEDELIVERIES:
            packet = new SessionAcknowledgeDeliveriesRequest();
            break;
         case REQ_CONNECTION_SENDTRANSACTION:
            packet = new ConnectionSendTransactionRequest();
            break;                     
         case REQ_CONSUMER_CHANGERATE:
            packet = new ConsumerChangeRateRequest();
            break;
         case NULL_RESPONSE:
            packet = new NullResponse();
            break;
      
         //Then the rest follow
            
         // Requests
         // --------   
                
         // Connection Factory
         case REQ_CONNECTIONFACTORY_CREATECONNECTIONDELEGATE:         
            packet = new ConnectionFactoryCreateConnectionDelegateRequest();
            break;         
         case REQ_CONNECTIONFACTORY_GETIDBLOCK:         
            packet = new ConnectionFactoryGetIDBlockRequest();
            break;
         case REQ_CONNECTIONFACTORY_GETCLIENTAOPSTACK:
            packet = new ConnectionFactoryGetClientAOPStackRequest();
            break;
            
         // Connection   
         case REQ_CONNECTION_CREATESESSIONDELEGATE:
            packet = new ConnectionCreateSessionDelegateRequest();
            break;
         case REQ_CONNECTION_GETCLIENTID:
            packet = new ConnectionGetClientIDRequest();
            break;
         case REQ_CONNECTION_SETCLIENTID:
            packet = new ConnectionSetClientIDRequest();
            break;
         case REQ_CONNECTION_START:
            packet = new ConnectionStartRequest();
            break;
         case REQ_CONNECTION_STOP:
            packet = new ConnectionStopRequest();
            break;
         case REQ_CONNECTION_GETPREPAREDTRANSACTIONS:
            packet = new ConnectionGetPreparedTransactionsRequest();
            break;
         
         // Session
         case REQ_SESSION_CREATECONSUMERDELEGATE:
            packet = new SessionCreateConsumerDelegateRequest();
            break;
         case REQ_SESSION_CREATEBROWSERDELEGATE:
            packet = new SessionCreateBrowserDelegateRequest();
            break;
         case REQ_SESSION_CREATEQUEUE:
            packet = new SessionCreateQueueRequest();
            break;
         case REQ_SESSION_CREATETOPIC:
            packet = new SessionCreateTopicRequest();
            break;   
           
         case REQ_SESSION_CANCELDELIVERIES:
            packet = new SessionCancelDeliveriesRequest();
            break;
         case REQ_SESSION_CANCELDELIVERY:
            packet = new SessionCancelDeliveryRequest();
            break;   
         case REQ_SESSION_ADDTEMPORARYDESTINATION:
            packet = new SessionAddTemporaryDestinationRequest();
            break;   
         case REQ_SESSION_DELETETEMPORARYDESTINATION:
            packet = new SessionDeleteTemporaryDestinationRequest();
            break;    
         case REQ_SESSION_UNSUBSCRIBE:
            packet = new SessionUnsubscribeRequest();
            break; 

         case REQ_SESSION_RECOVERDELIVERIES:
            packet = new SessionRecoverDeliveriesRequest();
            break;  
            
         // Consumer

         case REQ_CONSUMER_CANCELINFLIGHTMESSAGES:
            packet = new ConsumerCancelInflightMessagesRequest();
            break;
                        
         // Browser   
            
         case REQ_BROWSER_NEXTMESSAGE:
            packet = new BrowserNextMessageRequest();
            break;
         case REQ_BROWSER_HASNEXTMESSAGE:
            packet = new BrowserHasNextMessageRequest();
            break;
         case REQ_BROWSER_NEXTMESSAGEBLOCK:
            packet = new BrowserNextMessageBlockRequest();
            break;
            
         // Closeable
            
         case REQ_CLOSE:
            packet = new CloseRequest();
            break;
         case REQ_CLOSING:
            packet = new ClosingRequest();
            break;
            
            
         // Responses
         // ---------
                  
         // Connection factory
         case RESP_CONNECTIONFACTORY_CREATECONNECTIONDELEGATE:
            packet = new ConnectionFactoryCreateConnectionDelegateResponse();
            break;
         case RESP_CONNECTIONFACTORY_GETIDBLOCK:
            packet = new ConnectionFactoryGetIDBlockResponse();
            break;
         case RESP_CONNECTIONFACTORY_GETCLIENTAOPSTACK:
            packet = new ConnectionFactoryGetClientAOPStackResponse();
            break;            
            
         // Connection 
         case RESP_CONNECTION_CREATESESSIONDELEGATE:
            packet = new ConnectionCreateSessionDelegateResponse();
            break;
         case RESP_CONNECTION_GETCLIENTID:
            packet = new ConnectionGetClientIDResponse();
            break;            
         case RESP_CONNECTION_GETPREPAREDTRANSACTIONS:
            packet = new ConnectionGetPreparedTransactionsResponse();
            break;
            
         // Session
         case RESP_SESSION_CREATECONSUMERDELEGATE:
            packet = new SessionCreateConsumerDelegateResponse();
            break;
         case RESP_SESSION_CREATEBROWSERDELEGATE:
            packet = new SessionCreateBrowserDelegateResponse();
            break;            
         case RESP_SESSION_CREATEQUEUE:
            packet = new SessionCreateQueueResponse();
            break;  
         case RESP_SESSION_CREATETOPIC:
            packet = new SessionCreateTopicResponse();
            break; 
            
         // Browser
         case RESP_BROWSER_NEXTMESSAGE:
            packet = new BrowserNextMessageResponse();
            break;
         case RESP_BROWSER_HASNEXTMESSAGE:
            packet = new BrowserHasNextMessageResponse();
            break;            
         case RESP_BROWSER_NEXTMESSAGEBLOCK:
            packet = new BrowserNextMessageBlockResponse();
            break;     
            
            
         case SERIALIZED:
            packet = new SerializedPacket();
            break;            
         case CONNECTIONFACTORY_UPDATE:
            packet = new ConnectionFactoryUpdate();
            break;
         case POLLEDCALLBACKS_DELIVERY:
            packet = new PolledCallbacksDelivery();
            break;
                        
         default:
           throw new IllegalArgumentException("Invalid packet type: " + id);
      }
      
      return packet;
   }
      
   protected int methodId;
   
   public PacketSupport()
   {      
   }
   
   public PacketSupport(int methodID)   
   {      
      this.methodId = methodID;
   }
   
   public Object getPayload()
   {
      return this;
   }
   
   public void write(DataOutputStream os) throws Exception
   {      
      os.writeInt(methodId);      
   }
   
   public abstract void read(DataInputStream is) throws Exception;
         
   protected void writeNullableString(String s, DataOutputStream os) throws Exception
   {
      if (s == null)
      {
         os.writeByte(NULL);
      }
      else
      {
         os.writeByte(NOT_NULL);
         
         os.writeUTF(s);
      }
   }
   
   protected String readNullableString(DataInputStream is) throws Exception
   {
      byte b = is.readByte();
      
      if (b == NULL)
      {
         return null;
      }
      else
      {
         String s = is.readUTF();
         
         return s;
      }
   }
}
