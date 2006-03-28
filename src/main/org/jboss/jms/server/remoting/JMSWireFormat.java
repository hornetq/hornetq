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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.aop.Dispatcher;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.DeliveryRunnable;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.IdBlock;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvocationResponse;
import org.jboss.remoting.marshal.Marshaller;
import org.jboss.remoting.marshal.UnMarshaller;
import org.jboss.remoting.marshal.serializable.SerializableMarshaller;
import org.jboss.remoting.marshal.serializable.SerializableUnMarshaller;
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
   private static final long serialVersionUID = -7646123424863782043L;
   
   private static final Logger log = Logger.getLogger(JMSWireFormat.class);

   protected Marshaller serializableMarshaller;
   
   protected UnMarshaller serializableUnMarshaller;
   
   //The request codes  - start from zero
   
   protected static final byte SERIALIZED = 0;
   
   protected static final byte SEND = 1;
   
   protected static final byte ACTIVATE = 2;
   
   protected static final byte DEACTIVATE = 3;
   
   protected static final byte GETMESSAGENOW = 4;
   
   protected static final byte ACKNOWLEDGE = 5;
   
   protected static final byte SEND_TRANSACTION = 6;
   
   protected static final byte GET_ID_BLOCK = 7;
   
   protected static final byte CANCEL_DELIVERY = 8;
   
   protected static final byte CANCEL_DELIVERIES = 9;
   
   
   //The response codes - start from 100
   
   protected static final byte CALLBACK = 100;
   
   protected static final byte NULL_RESPONSE = 101;
   
   protected static final byte MESSAGE_RESPONSE = 102;
         
   protected static final byte ID_BLOCK_RESPONSE = 103;
      
   protected static final byte DEACTIVATE_RESPONSE = 104;
   
   
         
   protected boolean trace;
   
   public JMSWireFormat()
   {
      serializableMarshaller = new SerializableMarshaller();
      
      serializableUnMarshaller = new SerializableUnMarshaller();
      
      trace = log.isTraceEnabled();
   }

   public Marshaller cloneMarshaller() throws CloneNotSupportedException
   {
      return this;
   }
   
   public UnMarshaller cloneUnMarshaller() throws CloneNotSupportedException
   {
      return this;
   }
   
   public void setClassLoader(ClassLoader classloader)
   {      
   }
   
   private void writeHeader(MethodInvocation mi, ObjectOutputStream dos) throws IOException
   {
      int objectId = ((Integer)mi.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue();
      
      dos.writeInt(objectId);
      
      dos.writeLong(mi.getMethodHash());
   }
   
   private MethodInvocation readHeader(ObjectInputStream ois) throws IOException
   {
      int objectId = ois.readInt();
      
      long methodHash = ois.readLong();                  
      
      MethodInvocation mi = new MethodInvocation(null, methodHash, null, null, null);                 
      
      mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));
      
      return mi;
   }
   
   protected void handleVersion(Object obj, JBossObjectOutputStream oos) throws IOException
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
         //We need to write the version for the marshallable
         version = ((MessagingMarshallable)load).getVersion();
      }
      else
      {
         //This is some kind of remoting internal invocation, we still write a version
         //but it is the version of the client code not the connection
         //(since we are not associated to any connection)
         version = Version.instance().getProviderIncrementingVersion();
      }
      
      //The first byte written for any request/response is the version
      oos.writeByte(version);         
   }
   
   public void write(Object obj, OutputStream out) throws IOException
   {
      // sanity check
      if (!(out instanceof JBossObjectOutputStream))
      {
         log.error("out is a " + out.getClass());
         throw new IllegalStateException("OutputStream must be a JBossObjectOutputStream");
      }
           
      JBossObjectOutputStream oos = (JBossObjectOutputStream)out;
      
      handleVersion(obj, oos);

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
               oos.writeByte(SEND);
               
               writeHeader(mi, oos);
               
               JBossMessage m = (JBossMessage)mi.getArguments()[0];                              
               
               oos.writeByte(m.getType());
               
               m.writeExternal(oos);
               
               oos.flush();  
               
               if (trace) { log.trace("wrote send()"); }
            }
            else if ("activate".equals(methodName))
            {
               oos.writeByte(ACTIVATE);
               
               writeHeader(mi, oos);                            
               
               oos.flush();
               
               if (trace) { log.trace("wrote activate()"); }
            }
            else if ("deactivate".equals(methodName))
            {
               oos.writeByte(DEACTIVATE);
               
               writeHeader(mi, oos);                             
               
               oos.flush();
               
               if (trace) { log.trace("wrote deactivate()"); }
            }
            else if ("getMessageNow".equals(methodName))
            {
               oos.writeByte(GETMESSAGENOW);
               
               writeHeader(mi, oos);    
               
               boolean wait = ((Boolean)mi.getArguments()[0]).booleanValue();
               
               oos.writeBoolean(wait);
               
               oos.flush();
               
               if (trace) { log.trace("wrote getMessageNow()"); }
            }
            else if ("acknowledge".equals(methodName))
            {
               oos.writeByte(ACKNOWLEDGE);
               
               writeHeader(mi, oos);                              
               
               oos.flush();
               
               if (trace) { log.trace("wrote acknowledge()"); }
            }
            else if ("sendTransaction".equals(methodName))
            {
               oos.writeByte(SEND_TRANSACTION);
               
               writeHeader(mi, oos);    
               
               TransactionRequest request = (TransactionRequest)mi.getArguments()[0];
               
               request.writeExternal(oos);
               
               oos.flush();
               
               if (trace) { log.trace("wrote getMessageNow()"); }
            }
            else if ("getIdBlock".equals(methodName))
            {
               oos.writeByte(GET_ID_BLOCK);
               
               writeHeader(mi, oos);    
               
               int size = ((Integer)mi.getArguments()[0]).intValue();
               
               oos.writeInt(size);
               
               oos.flush();
               
               if (trace) { log.trace("wrote getIdBlock()"); }
            }
            else if ("cancelDelivery".equals(methodName))
            {
               oos.writeByte(CANCEL_DELIVERY);
               
               writeHeader(mi, oos);
               
               long id = ((Long)mi.getArguments()[0]).longValue();
               
               oos.writeLong(id);
               
               oos.flush();
               
               if (trace) { log.trace("wrote cancelDelivery)"); }              
            }
            else if ("cancelDeliveries".equals(methodName) && mi.getArguments() != null)
            {
               oos.writeByte(CANCEL_DELIVERIES);
               
               writeHeader(mi, oos);
               
               List ids = (List)mi.getArguments()[0];
               
               oos.writeInt(ids.size());
               
               Iterator iter = ids.iterator();
               
               while (iter.hasNext())
               {
                  Long l = (Long)iter.next();
                  oos.writeLong(l.longValue());
               }
                              
               oos.flush();
               
               if (trace) { log.trace("wrote cancelDeliveries()"); }              
            }
            else
            { 
               oos.write(SERIALIZED);
               
               //Delegate to serialization to handle the wire format
               serializableMarshaller.write(obj, oos);
               
               if (trace) { log.trace("wrote using standard serialization"); }
            }
         }
         else if (param instanceof DeliveryRunnable)
         {
            //Message delivery callback
            
            if (trace) { log.trace("DeliveryRunnable"); }
               
            DeliveryRunnable dr = (DeliveryRunnable)param;
            
            oos.writeByte(CALLBACK);
                    
            int consumerID = dr.getConsumerID();
            
            MessageProxy del = dr.getMessageProxy();
            
            oos.writeInt(consumerID);
            
            oos.writeByte(del.getMessage().getType());
            
            oos.writeInt(del.getDeliveryCount());
            
            del.getMessage().writeExternal(oos);
            
            oos.flush();    
            
            if (trace) { log.trace("wrote DeliveryRunnable"); }
         }            
         else
         {           
            //Internal invocation
            
            oos.write(SERIALIZED);
            
            //Delegate to serialization to handle the wire format
            serializableMarshaller.write(obj, oos);
            
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
            oos.write(NULL_RESPONSE);
            
            oos.flush();            
            
            if (trace) { log.trace("wrote null response"); }
         }
         else if (res instanceof MessageProxy)
         {
            //Return value from getMessageNow
            oos.write(MESSAGE_RESPONSE);
            
            MessageProxy del = (MessageProxy)res;
            
            oos.writeByte(del.getMessage().getType());
            
            oos.writeInt(del.getDeliveryCount());
            
            del.getMessage().writeExternal(oos);
            
            oos.flush();
            
            if (trace) { log.trace("wrote message response"); }
         }
         else if (res instanceof IdBlock)
         {
            //Return value from getMessageNow
            oos.write(ID_BLOCK_RESPONSE);
            
            IdBlock block = (IdBlock)res;
            
            block.writeExternal(oos);
            
            oos.flush();
            
            if (trace) { log.trace("wrote message response"); }
         }
         else if (res instanceof Long)
         {
            //Return value from deactivate
            oos.write(DEACTIVATE_RESPONSE);
            
            Long l = (Long)res;
            
            oos.writeLong(l.longValue());
            
            oos.flush();
               
            if (trace) { log.trace("wrote deactivate response"); }
         }
         else
         {      
            oos.write(SERIALIZED);
              
            //Delegate to serialization to handle the wire format
            serializableMarshaller.write(obj, out);
            
            if (trace) { log.trace("wrote using standard serialization"); }
         }
      }
      else
      {
         throw new IllegalStateException("Invalid object " + obj);
      }   
   }
   
   public Object read(InputStream in, Map map) throws IOException, ClassNotFoundException
   {
      //Sanity check
      if (!(in instanceof JBossObjectInputStream))
      {
         throw new IllegalStateException("InputStream must be an JBossObjectInputStream");
      }
      
      JBossObjectInputStream ois = (JBossObjectInputStream)in;
      
      //First byte read is always version
      
      byte version = ois.readByte();

      byte formatType = (byte)ois.read();
      
      if (trace) { log.trace("reading, format type is " + formatType); }
       
      switch (formatType)
      {
         case SERIALIZED:            
         {            
            //Delegate to serialization
            Object ret = serializableUnMarshaller.read(ois, map);            
            
            if (trace) { log.trace("read using standard serialization"); }
                 
            return ret;            
         }            
         case SEND:
         {            
            MethodInvocation mi = readHeader(ois);
            
            byte messageType = ois.readByte();
            
            JBossMessage m = (JBossMessage)MessageFactory.createMessage(messageType);
            
            m.readExternal(ois);
                        
            Object[] args = new Object[] {m};
            
            mi.setArguments(args);
            
            InvocationRequest request =
               new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                     new MessagingMarshallable(version, mi), null, null, null);
            
            if (trace) { log.trace("read send()"); }
            
            return request;
         }            
         case ACTIVATE:            
         {
            MethodInvocation mi = readHeader(ois);                
            
            InvocationRequest request =
               new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                     new MessagingMarshallable(version, mi), null, null, null);
            
            if (trace) { log.trace("read activate()"); }
            
            return request;    
         }
         case DEACTIVATE:            
         {
            MethodInvocation mi = readHeader(ois);
            
            InvocationRequest request =
               new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                     new MessagingMarshallable(version, mi), null, null, null);
            
            
            if (trace) { log.trace("read deactivate()"); }
            
            return request;
         }
         case GETMESSAGENOW:            
         { 
            MethodInvocation mi = readHeader(ois);
            
            boolean wait = ois.readBoolean();
                                                
            Object[] args = new Object[] {Boolean.valueOf(wait)};
            
            mi.setArguments(args);
                
            InvocationRequest request =
               new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                     new MessagingMarshallable(version, mi), null, null, null);
            
            if (trace) { log.trace("read getMessageNow()"); }
            
            return request;
         }
         case SEND_TRANSACTION:            
         { 
            MethodInvocation mi = readHeader(ois);
            
            TransactionRequest tr = new TransactionRequest();
            
            tr.readExternal(ois);
                                                          
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
            MethodInvocation mi = readHeader(ois);
            
            int size = ois.readInt();
            
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
            MethodInvocation mi = readHeader(ois);                
 
            InvocationRequest request =
               new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                     new MessagingMarshallable(version, mi), null, null, null);
            
            if (trace) { log.trace("read acknowledge()"); }
            
            return request;
         }         
         case CANCEL_DELIVERY:
         {
            MethodInvocation mi = readHeader(ois);
            
            long id = ois.readLong();
            
            Object[] args = new Object[] {new Long(id)};
            
            mi.setArguments(args);
                
            InvocationRequest request =
               new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                     new MessagingMarshallable(version, mi), null, null, null);
            
            if (trace) { log.trace("read cancelDelivery()"); }
            
            return request;
         }
         case CANCEL_DELIVERIES:
         {
            MethodInvocation mi = readHeader(ois);
            
            int size = ois.readInt();
            
            List ids = new ArrayList(size);
            
            for (int i = 0; i < size; i++)
            {
               long id = ois.readLong();
               
               ids.add(new Long(id));
            }
                        
            Object[] args = new Object[] {ids};
            
            mi.setArguments(args);
                
            InvocationRequest request =
               new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                     new MessagingMarshallable(version, mi), null, null, null);
            
            if (trace) { log.trace("read cancelDeliveries()"); }
            
            return request;
         }
         case MESSAGE_RESPONSE:
         {
            byte type = ois.readByte();
            
            int deliveryCount = ois.readInt();

            JBossMessage m = (JBossMessage)MessageFactory.createMessage(type);
            
            m.readExternal(ois);
            
            MessageProxy md = JBossMessage.createThinDelegate(m, deliveryCount);
                        
            InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, md), false, null);
            
            if (trace) { log.trace("read message response"); }
            
            return resp;
         }
         case ID_BLOCK_RESPONSE:
         {
            IdBlock block = new IdBlock();
            
            block.readExternal(ois);
                                  
            InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, block), false, null);
            
            if (trace) { log.trace("read message response"); }
            
            return resp;
         }
         case DEACTIVATE_RESPONSE:
         {
            long id = ois.readLong();
                                  
            InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, new Long(id)), false, null);
            
            if (trace) { log.trace("read deactivate response"); }
            
            return resp;
         }
         case NULL_RESPONSE:
         {
            InvocationResponse resp = new InvocationResponse(null, new MessagingMarshallable(version, null), false, null);
            
            if (trace) { log.trace("read null response"); }
            
            return resp;
         }
         case CALLBACK:
         {
            int consumerID = ois.readInt();

            byte type = ois.readByte();

            int deliveryCount = ois.readInt();

            JBossMessage m = (JBossMessage)MessageFactory.createMessage(type);

            m.readExternal(ois);

            MessageProxy md = JBossMessage.createThinDelegate(m, deliveryCount);

            DeliveryRunnable dr = new DeliveryRunnable(md, consumerID, null, trace);

            InvocationRequest request =
               new InvocationRequest(null, JMSRemotingConnection.JMS_CALLBACK_SUBSYSTEM,
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
}
