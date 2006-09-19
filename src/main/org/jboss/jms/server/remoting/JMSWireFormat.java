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
import org.jboss.jms.client.remoting.CallbackServerFactory;
import org.jboss.jms.client.remoting.HandleMessageResponse;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.endpoint.ClientDelivery;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.IdBlock;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvocationResponse;
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
 * @version 1.1
 *
 * JMSWireFormat.java,v 1.1 2006/02/01 17:38:32 timfox Exp
 */
public class JMSWireFormat implements Marshaller, UnMarshaller
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -7646123424863782043L;

   private static final Logger log = Logger.getLogger(JMSWireFormat.class);
   
   private static boolean usingJBossSerialization;

   // The request codes  - start from zero

   protected static final byte SERIALIZED = 0;   
   protected static final byte ACKNOWLEDGE = 1;
   protected static final byte ACKNOWLEDGE_BATCH = 2;
   protected static final byte SEND = 3;   
   protected static final byte CANCEL_DELIVERIES = 4;
   protected static final byte MORE = 5;
   protected static final byte SEND_TRANSACTION = 6;
   protected static final byte GET_ID_BLOCK = 7;
 

   // The response codes - start from 100

   protected static final byte CALLBACK = 100;
   protected static final byte NULL_RESPONSE = 101;
   protected static final byte ID_BLOCK_RESPONSE = 102;
   protected static final byte HANDLE_MESSAGE_RESPONSE = 103;
   protected static final byte BROWSE_MESSAGE_RESPONSE = 104;
   protected static final byte BROWSE_MESSAGES_RESPONSE = 105;


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
      //Sanity check
      if (!(out instanceof MessagingObjectOutputStream))
      {
         throw new IllegalStateException("Must be MessagingObjectOutputStream");
      }
      
      DataOutputStream dos = (DataOutputStream)(((MessagingObjectOutputStream)out).getUnderlyingStream());
            
      handleVersion(obj, dos);

      try
      {         
         if (obj instanceof InvocationRequest)
         {
            if (trace) { log.trace("writing InvocationRequest"); }
   
            InvocationRequest req = (InvocationRequest)obj;
   
            Object param;
   
            if (req.getParameter() instanceof MessagingMarshallable)
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
               else if ("more".equals(methodName))
               {
                  dos.writeByte(MORE);
   
                  writeHeader(mi, dos);
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote activate()"); }
               }           
               else if ("acknowledge".equals(methodName))
               {
                  dos.writeByte(ACKNOWLEDGE);
   
                  writeHeader(mi, dos);
                  
                  AckInfo ack = (AckInfo)mi.getArguments()[0];
                  
                  ack.write(dos);
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote acknowledge()"); }
               }
               else if ("acknowledgeBatch".equals(methodName))
               {
                  dos.writeByte(ACKNOWLEDGE_BATCH);
   
                  writeHeader(mi, dos);
                  
                  List acks = (List)mi.getArguments()[0];
   
                  dos.writeInt(acks.size());
   
                  Iterator iter = acks.iterator();
   
                  while (iter.hasNext())
                  {
                     AckInfo ack = (AckInfo)iter.next();
                     ack.write(dos);
                  }
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote acknowledge()"); }
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
               else if ("cancelDeliveries".equals(methodName) && mi.getArguments() != null)
               {
                  dos.writeByte(CANCEL_DELIVERIES);
   
                  writeHeader(mi, dos);
   
                  List ids = (List)mi.getArguments()[0];
   
                  dos.writeInt(ids.size());
   
                  Iterator iter = ids.iterator();
   
                  while (iter.hasNext())
                  {
                     AckInfo ack = (AckInfo)iter.next();
                     ack.write(dos);
                  }
   
                  dos.flush();
   
                  if (trace) { log.trace("wrote cancelDeliveries()"); }
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
   
               dos.writeByte(CALLBACK);
   
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
            else if (res instanceof IdBlock)
            {
               //Return value from getMessageNow
               dos.write(ID_BLOCK_RESPONSE);
   
               IdBlock block = (IdBlock)res;
   
               block.write(dos);
   
               dos.flush();
   
               if (trace) { log.trace("wrote id block response"); }
            }
            else if (res instanceof HandleMessageResponse)
            {         
               //Return value from delivering messages to client
               dos.write(HANDLE_MESSAGE_RESPONSE);
   
               HandleMessageResponse response = (HandleMessageResponse)res;
   
               response.write(dos);
   
               dos.flush();
   
               if (trace) { log.trace("wrote handle message response"); }
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
      // Sanity check
      if (!(in instanceof MessagingObjectInputStream))
      {
         throw new IllegalStateException("Must be MessagingObjectInputStream");
      }
      
      DataInputStream dis = (DataInputStream)(((MessagingObjectInputStream)in).getUnderlyingStream());

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
            case MORE:
            {
               MethodInvocation mi = readHeader(dis);
   
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
               
               AckInfo info = new AckInfo();
               
               info.read(dis);
               
               Object[] args = new Object[] {info};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read acknowledge()"); }
   
               return request;
            }
            case ACKNOWLEDGE_BATCH:
            {
               MethodInvocation mi = readHeader(dis);
                           
               int num = dis.readInt();
               
               List acks = new ArrayList(num);
               
               for (int i = 0; i < num; i++)
               {
                  AckInfo ack = new AckInfo();
                  
                  ack.read(dis);
                  
                  acks.add(ack);
               }
                           
               Object[] args = new Object[] {acks};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read acknowledge()"); }
   
               return request;
            }
            case CANCEL_DELIVERIES:
            {
               MethodInvocation mi = readHeader(dis);
   
               int size = dis.readInt();
   
               List acks = new ArrayList(size);
   
               for (int i = 0; i < size; i++)
               {
                  AckInfo ack = new AckInfo();
                  
                  ack.read(dis);
                  
                  acks.add(ack);
               }
   
               Object[] args = new Object[] {acks};
   
               mi.setArguments(args);
   
               InvocationRequest request =
                  new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                        new MessagingMarshallable(version, mi), null, null, null);
   
               if (trace) { log.trace("read cancelDeliveries()"); }
   
               return request;
            }
            case ID_BLOCK_RESPONSE:
            {
               IdBlock block = new IdBlock();
   
               block.read(dis);
   
               InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, block), false, null);
   
               if (trace) { log.trace("read id block response"); }
   
               return resp;
            }
            case HANDLE_MESSAGE_RESPONSE:
            {
               HandleMessageResponse res = new HandleMessageResponse();
   
               res.read(dis);
   
               InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, res), false, null);
   
               if (trace) { log.trace("read handle message response"); }
   
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
            case CALLBACK:
            {
               ClientDelivery dr = new ClientDelivery();
               
               dr.read(dis);
   
               InvocationRequest request =
                  new InvocationRequest(null, CallbackServerFactory.JMS_CALLBACK_SUBSYSTEM,
                                        new MessagingMarshallable(version, dr), null, null, null);
   
               if (trace) { log.trace("read callback()"); }
   
               return request;
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
      
      Object obj = ois.readObject();
      
      return obj;
   }

   // Inner classes -------------------------------------------------
}
