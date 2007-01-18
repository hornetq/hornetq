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
package org.jboss.jms.server.remoting;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.Message;

import org.jboss.aop.Dispatcher;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.remoting.CallbackManager;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.endpoint.Ack;
import org.jboss.jms.server.endpoint.Cancel;
import org.jboss.jms.server.endpoint.ClientDelivery;
import org.jboss.jms.server.endpoint.DefaultAck;
import org.jboss.jms.server.endpoint.DefaultCancel;
import org.jboss.jms.server.endpoint.DeliveryRecovery;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.IDBlock;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvocationResponse;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.invocation.InternalInvocation;
import org.jboss.remoting.marshal.Marshaller;
import org.jboss.remoting.marshal.UnMarshaller;
import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;

/**
 * 
 * A JMSWireFormat.
 * 
 * For some invocations, e.g. message sends, acknowledgements and activations it is vital that we
 * minimise the amount of data sent in the invocation so that we can maximise performance.
 * By default, Invocations and return values are sent over the wire as serialized Java objects.
 * This adds considerable overhead in terms of the amount of data sent (it adds class information
 * plus block data information) which significantly degrades performance. Therefore for the
 * invocations where performance is paramount we define a customer wire format that minimises the
 * amount of data sent.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSWireFormat implements Marshaller, UnMarshaller
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -7646123424863782043L;

   private static final Logger log = Logger.getLogger(JMSWireFormat.class);
   
   private static boolean usingJBossSerialization = true;

   // The request codes  - start from zero

   protected static final byte SERIALIZED = 0;   
   protected static final byte ACKNOWLEDGE = 1;
   protected static final byte ACKNOWLEDGE_LIST = 2;
   protected static final byte CANCEL = 3;
   protected static final byte CANCEL_LIST = 4;
   protected static final byte SEND = 5;   
   protected static final byte CHANGE_RATE = 6; 
   protected static final byte SEND_TRANSACTION = 7;
   protected static final byte GET_ID_BLOCK = 8;
   protected static final byte RECOVER_DELIVERIES = 9;
   protected static final byte CANCEL_INFLIGHT_MESSAGES = 10;
   

 

   // The response codes - start from 100

   protected static final byte MESSAGE_DELIVERY = 100;
   protected static final byte NULL_RESPONSE = 101;
   protected static final byte ID_BLOCK_RESPONSE = 102;
   protected static final byte BROWSE_MESSAGE_RESPONSE = 104;
   protected static final byte BROWSE_MESSAGES_RESPONSE = 105;
   protected static final byte CALLBACK_LIST = 106;


   // Static --------------------------------------------------------
   
   public static void setUsingJBossSerialization(boolean b)
   {
      usingJBossSerialization = b;
   }

   // Attributes ----------------------------------------------------

   protected boolean trace;

   // Constructors --------------------------------------------------

   public JMSWireFormat()
   {
      trace = log.isTraceEnabled();
   }

   // Marshaller implementation -------------------------------------

   public void write(Object obj, OutputStream out) throws IOException
   {          
      DataOutputStream dos;
      
      if (out instanceof DataOutputStream)
      {
         dos = (DataOutputStream)out;
      }
      else
      {
         //Further sanity check
         if (out instanceof ObjectOutputStream)
         {
            throw new IllegalArgumentException("Invalid stream - are you sure you have configured socket wrappers?");
         }
         
         //This would be the case for the HTTP transport for example
         //Wrap the stream
         
         //TODO Ideally remoting would let us wrap this before invoking the marshaller
         //but this does not appear to be possible
         dos = new DataOutputStream(out);
      }
                  
      handleVersion(obj, dos);

      try
      {
         if (obj instanceof InvocationRequest)
         {
            if (trace) { log.trace("writing InvocationRequest"); }
   
            InvocationRequest req = (InvocationRequest)obj;
   
            Object param;
            
            // Unwrap Callback.
            if (req.getParameter() instanceof InternalInvocation)
            {
               InternalInvocation ii = (InternalInvocation) req.getParameter();
               Object[] params = ii.getParameters();
               
               if (params != null && params.length > 0 && params[0] instanceof Callback)
               {
                  Callback callback = (Callback) params[0];
                  if (callback.getParameter() instanceof MessagingMarshallable)
                  {
                     MessagingMarshallable mm = (MessagingMarshallable)callback.getParameter();
                     param = mm.getLoad();
                  }
                  else
                  {
                     param = callback.getParameter();
                  }
               }
               else
               {
                  param = req.getParameter();
               }
            }
            else if (req.getParameter() instanceof MessagingMarshallable)
            {
               param = ((MessagingMarshallable)req.getParameter()).getLoad();
            }
            else
            {
               param = req.getParameter();
            }
   
            if (trace) { log.trace("param is " + param); }
   
            if (param instanceof MethodInvocation)
            {
               MethodInvocation mi = (MethodInvocation)param;
   
               String methodName = mi.getMethod().getName();
   
               if (trace) { log.trace("methodInvocation (" + methodName + "())"); }
   
               if (methodName.equals("send"))
               {
                  dos.writeByte(SEND);
   
                  writeHeader(mi, dos);
   
                  JBossMessage m = (JBossMessage)mi.getArguments()[0];
   
                  dos.writeByte(m.getType());
   
                  m.write(dos);
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote send()"); }
               }
               else if ("changeRate".equals(methodName))
               {
                  dos.writeByte(CHANGE_RATE);
   
                  writeHeader(mi, dos);
                  
                  Float f = (Float)mi.getArguments()[0];
                  
                  dos.writeFloat(f.floatValue());
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote activate()"); }
               }           
               else if ("acknowledgeDelivery".equals(methodName))
               {
                  dos.writeByte(ACKNOWLEDGE);
   
                  writeHeader(mi, dos);
                  
                  Ack ack = (Ack)mi.getArguments()[0];
                  
                  dos.writeLong(ack.getDeliveryID());
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote acknowledgeDelivery()"); }
               }
               else if ("acknowledgeDeliveries".equals(methodName))
               {
                  dos.writeByte(ACKNOWLEDGE_LIST);
   
                  writeHeader(mi, dos);
                  
                  List acks = (List)mi.getArguments()[0];
   
                  dos.writeInt(acks.size());
   
                  Iterator iter = acks.iterator();
   
                  while (iter.hasNext())
                  {
                     Ack ack = (Ack)iter.next();
                     dos.writeLong(ack.getDeliveryID());
                  }
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote acknowledgeDeliveries()"); }
               }
               else if ("cancelDelivery".equals(methodName))
               {
                  dos.writeByte(CANCEL);
   
                  writeHeader(mi, dos);
                  
                  Cancel cancel = (Cancel)mi.getArguments()[0];
                  
                  dos.writeLong(cancel.getDeliveryId());
                  
                  dos.writeInt(cancel.getDeliveryCount());
                  
                  dos.writeBoolean(cancel.isExpired());
                  
                  dos.writeBoolean(cancel.isReachedMaxDeliveryAttempts());
                  
                  dos.flush();
   
                  if (trace) { log.trace("wrote cancelDelivery()"); }
               }
               else if ("cancelDeliveries".equals(methodName) && mi.getArguments() != null)
               {
                  dos.writeByte(CANCEL_LIST);
   
                  writeHeader(mi, dos);
   
                  List ids = (List)mi.getArguments()[0];
   
                  dos.writeInt(ids.size());
   
                  Iterator iter = ids.iterator();
   
                  while (iter.hasNext())
                  {
                     Cancel cancel = (Cancel)iter.next();
                     
                     dos.writeLong(cancel.getDeliveryId());
                     
                     dos.writeInt(cancel.getDeliveryCount());
                     
                     dos.writeBoolean(cancel.isExpired());
                     
                     dos.writeBoolean(cancel.isReachedMaxDeliveryAttempts());
                  }
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote cancelDeliveries()"); }
               }
               else if ("recoverDeliveries".equals(methodName) && mi.getArguments() != null)
               {
                  dos.writeByte(RECOVER_DELIVERIES);
   
                  writeHeader(mi, dos);
   
                  List ids = (List)mi.getArguments()[0];
   
                  dos.writeInt(ids.size());
   
                  Iterator iter = ids.iterator();
   
                  while (iter.hasNext())
                  {
                     DeliveryRecovery d = (DeliveryRecovery)iter.next();
                     d.write(dos);
                  }
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote sendUnackedAckInfos()"); }
               }
               else if ("sendTransaction".equals(methodName))
               {
                  dos.writeByte(SEND_TRANSACTION);
   
                  writeHeader(mi, dos);
   
                  TransactionRequest request = (TransactionRequest)mi.getArguments()[0];
   
                  request.write(dos);
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote getMessageNow()"); }
               }
               else if ("getIdBlock".equals(methodName))
               {
                  dos.writeByte(GET_ID_BLOCK);
   
                  writeHeader(mi, dos);
   
                  int size = ((Integer)mi.getArguments()[0]).intValue();
   
                  dos.writeInt(size);
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote getIdBlock()"); }
               }           
               else if ("cancelInflightMessages".equals(methodName))
               {
                  dos.writeByte(CANCEL_INFLIGHT_MESSAGES);
                  
                  writeHeader(mi, dos);
                  
                  long lastDeliveryId = ((Long)mi.getArguments()[0]).longValue();
                  
                  dos.writeLong(lastDeliveryId);
                  
                  dos.flush();
               }
               else
               {
                  dos.write(SERIALIZED);
   
                  // Delegate to serialization to handle the wire format
                  serialize(dos, obj);
   
                  if (trace) { log.trace("wrote using standard serialization"); }
               }
            }
            else if (param instanceof ClientDelivery)
            {
               //Message delivery callback
   
               if (trace) { log.trace("DeliveryRunnable"); }
   
               ClientDelivery dr = (ClientDelivery)param;
   
               dos.writeByte(MESSAGE_DELIVERY);
               
               dos.writeUTF(req.getSessionId());
   
               dr.write(dos);
   
               dos.flush();
   
               if (trace) { log.trace("wrote DeliveryRunnable"); }
            }
            else
            {
               //Internal invocation
   
               dos.write(SERIALIZED);
   
               //Delegate to serialization to handle the wire format
               serialize(dos, obj);
   
               if (trace) { log.trace("wrote using standard serialization"); }
            }
         }
         else if (obj instanceof InvocationResponse)
         {
            if (trace) { log.trace("writing InvocationResponse"); }
   
            InvocationResponse resp = (InvocationResponse)obj;
   
            Object res;
   
            if (resp.getResult() instanceof MessagingMarshallable)
            {
               res = ((MessagingMarshallable)resp.getResult()).getLoad();
            }
            else
            {
               res = resp.getResult();
            }
   
            if (trace) { log.trace("result is " + res); }
   
            if (res == null && !resp.isException())
            {       
               dos.write(NULL_RESPONSE);
   
               dos.flush();
   
               if (trace) { log.trace("wrote null response"); }
            }         
            else if (res instanceof IDBlock)
            {
               //Return value from getMessageNow
               dos.write(ID_BLOCK_RESPONSE);
   
               IDBlock block = (IDBlock)res;
   
               block.write(dos);
   
               dos.flush();
   
               if (trace) { log.trace("wrote id block response"); }
            }
            else if (res instanceof JBossMessage)
            {
               //Return value from browsing message
               dos.write(BROWSE_MESSAGE_RESPONSE);
               
               JBossMessage msg = (JBossMessage)res;
               
               dos.writeByte(msg.getType());
               
               msg.write(dos);
               
               dos.flush();
               
               if (trace) { log.trace("wrote browse message response"); }
            }            
            else if (res instanceof Message[])
            {
               //Return value from browsing messages
               dos.write(BROWSE_MESSAGES_RESPONSE);
               
               Message[] msgs = (Message[])res;
               
               dos.writeInt(msgs.length);
               
               for (int i = 0; i < msgs.length; i++)
               {
                  JBossMessage m = (JBossMessage)msgs[i];
                  
                  dos.writeByte(m.getType());
                  
                  m.write(dos);
               }
               
               dos.flush();
               
               if (trace) { log.trace("wrote browse message response"); }
            }
            else if (res instanceof ArrayList &&
                     ((ArrayList) res).size() > 0 &&
                     ((ArrayList) res).get(0) instanceof Callback)
            {
               // Comes from polled Callbacks. (HTTP transport??)
               ArrayList callbackList = (ArrayList)res;
               dos.write(CALLBACK_LIST);
               dos.writeUTF(resp.getSessionId());
               dos.writeInt(callbackList.size());
               
               Iterator it = callbackList.iterator();
               while (it.hasNext())
               {
                  Callback callback = (Callback)it.next();

                  // We don't use acknowledgeable push callbacks

                  MessagingMarshallable mm = (MessagingMarshallable)callback.getParameter();
                  ClientDelivery delivery = (ClientDelivery)mm.getLoad();
                  delivery.write(dos);
                  dos.flush();
               }
            }
            else
            {
               dos.write(SERIALIZED);
   
               //Delegate to serialization to handle the wire format
               serialize(dos, obj);
   
               if (trace) { log.trace("wrote using standard serialization"); }
            }
         }
         else
         {
            throw new IllegalStateException("Invalid object " + obj);
         }
      }
      catch (Exception e)
      {
         IOException e2 = new IOException(e.getMessage());
         e2.setStackTrace(e.getStackTrace());
         throw e2;
      }
   }

   public Marshaller cloneMarshaller() throws CloneNotSupportedException
   {
      return this;
   }

   // UnMarshaller implementation -----------------------------------

   public Object read(InputStream in, Map map) throws IOException, ClassNotFoundException
   {            
      DataInputStream dis;
      
      if (in instanceof DataInputStream)
      {
         dis = (DataInputStream)in;         
      }
      else
      {        
         //Further sanity check
         if (in instanceof ObjectInputStream)
         {
            throw new IllegalArgumentException("Invalid stream - are you sure you have configured socket wrappers?");
         }
         
         //This would be the case for the HTTP transport for example
         //Wrap the stream
         
         //TODO Ideally remoting would let us wrap this before invoking the marshaller
         //but this does not appear to be possible
         dis = new DataInputStream(in);
      }
      
      // First byte read is always version

      byte version = dis.readByte();

      byte formatType = (byte)dis.read();

      if (trace) { log.trace("reading, format type is " + formatType); }
      
      try
      {
   
         switch (formatType)
         {
            case SERIALIZED:
            {
               // Delegate to serialization
               Object ret = deserialize(dis);
   
               if (trace) { log.trace("read using standard serialization"); }
   
               return ret;
            }
            case SEND:
            {
               MethodInvocation mi = readHeader(dis);
   
               byte messageType = dis.readByte();
   
               JBossMessage m = (JBossMessage)MessageFactory.createMessage(messageType);
   
               m.read(dis);
   
               Object[] args = new Object[] {m};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read send()"); }
   
               return request;
            }
            case CHANGE_RATE:
            {
               MethodInvocation mi = readHeader(dis);
               
               float f = dis.readFloat();
               
               Object[] args = new Object[] {new Float(f)};
               
               mi.setArguments(args);
                  
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read activate()"); }
   
               return request;
            }         
            case SEND_TRANSACTION:
            {
               MethodInvocation mi = readHeader(dis);
   
               TransactionRequest tr = new TransactionRequest();
   
               tr.read(dis);
   
               Object[] args = new Object[] {tr};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read sendTransaction()"); }
   
               return request;
            }
            case GET_ID_BLOCK:
            {
               MethodInvocation mi = readHeader(dis);
   
               int size = dis.readInt();
   
               Object[] args = new Object[] {new Integer(size)};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read getIdBlock()"); }
   
               return request;
            }
            case ACKNOWLEDGE:
            {
               MethodInvocation mi = readHeader(dis);
               
               long l = dis.readLong();
               
               Object[] args = new Object[] {new DefaultAck(l)};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read acknowledgeDelivery()"); }
   
               return request;
            }
            case ACKNOWLEDGE_LIST:
            {
               MethodInvocation mi = readHeader(dis);
                           
               int num = dis.readInt();
               
               List acks = new ArrayList(num);
               
               for (int i = 0; i < num; i++)
               {
                  long l = dis.readLong();                  
                  
                  acks.add(new DefaultAck(l));
               }
                           
               Object[] args = new Object[] {acks};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read acknowledgeDeliveries()"); }
   
               return request;
            }
            case CANCEL:
            {
               MethodInvocation mi = readHeader(dis);
               
               long deliveryId = dis.readLong();
               
               int deliveryCount = dis.readInt();
               
               boolean expired = dis.readBoolean();
               
               boolean reachedMaxDeliveries = dis.readBoolean();
               
               Object[] args = new Object[] {new DefaultCancel(deliveryId, deliveryCount, expired, reachedMaxDeliveries)};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read cancelDelivery()"); }
   
               return request;
            }
            case CANCEL_LIST:
            {
               MethodInvocation mi = readHeader(dis);
   
               int size = dis.readInt();
   
               List acks = new ArrayList(size);
   
               for (int i = 0; i < size; i++)
               {                  
                  long deliveryId = dis.readLong();
                  
                  int deliveryCount = dis.readInt();
                  
                  boolean expired = dis.readBoolean();
                  
                  boolean reachedMaxDeliveries = dis.readBoolean();
                  
                  DefaultCancel cancel = new DefaultCancel(deliveryId, deliveryCount, expired, reachedMaxDeliveries);
                                    
                  acks.add(cancel);
               }
   
               Object[] args = new Object[] {acks};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read cancelDeliveries()"); }
   
               return request;
            }
            case RECOVER_DELIVERIES:
            {
               MethodInvocation mi = readHeader(dis);
   
               int size = dis.readInt();
   
               List dels = new ArrayList(size);
   
               for (int i = 0; i < size; i++)
               {
                  DeliveryRecovery d = new DeliveryRecovery();
                  
                  d.read(dis);
                  
                  dels.add(d);
               }
   
               Object[] args = new Object[] {dels};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read unackedAckInfos()"); }
   
               return request;
            }
            case CANCEL_INFLIGHT_MESSAGES:
            {
               MethodInvocation mi = readHeader(dis);
               
               long lastDeliveryId = dis.readLong();
               
               Object[] args = new Object[] {new Long(lastDeliveryId)};
               
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read cancelInflightMessages()"); }
   
               return request;
               
               
            }
            case ID_BLOCK_RESPONSE:
            {
               IDBlock block = new IDBlock();
   
               block.read(dis);
   
               InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, block), false, null);
   
               if (trace) { log.trace("read id block response"); }
   
               return resp;
            }
            case BROWSE_MESSAGE_RESPONSE:
            {
               byte type = dis.readByte();
               
               JBossMessage msg = (JBossMessage)MessageFactory.createMessage(type);
               
               msg.read(dis);
               
               InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, msg), false, null);
   
               if (trace) { log.trace("read browse message response"); }
   
               return resp;
            }
            case BROWSE_MESSAGES_RESPONSE:
            {
               int num = dis.readInt();
               
               Message[] msgs = new Message[num];
               
               for (int i = 0; i < num; i++)
               {
                  byte type = dis.readByte();
                  
                  JBossMessage msg = (JBossMessage)MessageFactory.createMessage(type);
                  
                  msg.read(dis);
                  
                  msgs[i] = msg;
               }
               
               InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, msgs), false, null);
   
               if (trace) { log.trace("read browse message response"); }
   
               return resp;
            }
            case NULL_RESPONSE:
            {    
               InvocationResponse resp =
                  new InvocationResponse(null, new MessagingMarshallable(version, null), false, null);
   
               if (trace) { log.trace("read null response"); }
   
               return resp;
            }
            case MESSAGE_DELIVERY:
            {
               String sessionId = dis.readUTF();
               ClientDelivery dr = new ClientDelivery();
               
               dr.read(dis);

               // Recreate Callback.
               MessagingMarshallable mm = new MessagingMarshallable(version, dr);
               Callback callback = new Callback(mm);
               InternalInvocation ii
                  = new InternalInvocation(InternalInvocation.HANDLECALLBACK, new Object[]{callback});
               InvocationRequest request
                  = new InvocationRequest(sessionId, CallbackManager.JMS_CALLBACK_SUBSYSTEM,
                                          ii, null, null, null);
   
               if (trace) { log.trace("read callback()"); }
   
               return request;
            }
            case CALLBACK_LIST:
            {
               // Recreate ArrayList of Callbacks (for Callback polling).
               String sessionId = dis.readUTF();
               int size = dis.readInt();
               ArrayList callbackList = new ArrayList(size);
               for (int i = 0; i < size; i++)
               {
                  // We don't use acknowledgeable push callbacks

                  ClientDelivery delivery = new ClientDelivery();
                  delivery.read(dis);
                  MessagingMarshallable mm = new MessagingMarshallable(version, delivery);
                  Callback callback = new Callback(mm);

                  callbackList.add(callback);
               }

               return new InvocationResponse(sessionId, callbackList, false, null);
            }
            default:
            {
               throw new IllegalStateException("Invalid format type " + formatType);
            }
         }
      }
      catch (Exception e)
      {
         IOException e2 = new IOException(e.getMessage());
         e2.setStackTrace(e.getStackTrace());
         throw e2;
      }
   }


   public UnMarshaller cloneUnMarshaller() throws CloneNotSupportedException
   {
      return this;
   }

   public void setClassLoader(ClassLoader classloader)
   {
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void handleVersion(Object obj, DataOutputStream oos) throws IOException
   {
      Object load = null;

      if (obj instanceof InvocationRequest)
      {
         InvocationRequest ir = (InvocationRequest)obj;
         load = ir.getParameter();
      }
      else if (obj instanceof InvocationResponse)
      {
         InvocationResponse ir = (InvocationResponse)obj;
         load = ir.getResult();
      }

      byte version;
      if (load instanceof MessagingMarshallable)
      {
         // we need to write the version for the marshallable
         version = ((MessagingMarshallable)load).getVersion();         
      }
      else
      {
         // this is some kind of remoting internal invocation, we still write a version but it is
         // the version of the client code not the connection (since we are not associated to any
         // connection)
         version = Version.instance().getProviderIncrementingVersion();
      }
    
      // the first byte written for any request/response is the version
      oos.writeByte(version);
   }

   // Private -------------------------------------------------------

   private void writeHeader(MethodInvocation mi, DataOutputStream dos) throws IOException
   {
      int objectId = ((Integer)mi.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue();

      dos.writeInt(objectId);

      dos.writeLong(mi.getMethodHash());
   }

   private MethodInvocation readHeader(DataInputStream ois) throws IOException
   {
      int objectId = ois.readInt();

      long methodHash = ois.readLong();

      MethodInvocation mi = new MethodInvocation(null, methodHash, null, null, null);

      mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));

      return mi;
   }
   
   private void serialize(OutputStream os, Object obj) throws Exception
   { 
      ObjectOutputStream oos;
      
      if (usingJBossSerialization)
      {
         oos = new JBossObjectOutputStream(os);
      }
      else
      {
         oos = new ObjectOutputStream(os);
      }
      
      oos.writeObject(obj);
      
      oos.flush();      
   }
   
   private Object deserialize(InputStream is) throws Exception
   {
      ObjectInputStream ois;
      
      if (usingJBossSerialization)
      {
         ois = new JBossObjectInputStream(is);
      }
      else
      {
         ois = new ObjectInputStream(is);
      }
      
      return ois.readObject();
   }

   // Inner classes -------------------------------------------------
}
