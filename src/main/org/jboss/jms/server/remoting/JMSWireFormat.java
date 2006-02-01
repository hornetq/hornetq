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
import java.io.OutputStream;
import java.util.Map;

import org.jboss.aop.Dispatcher;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageDelegate;
import org.jboss.jms.server.endpoint.DeliveryRunnable;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvocationResponse;
import org.jboss.remoting.marshal.Marshaller;
import org.jboss.remoting.marshal.UnMarshaller;
import org.jboss.remoting.marshal.serializable.SerializableMarshaller;
import org.jboss.remoting.marshal.serializable.SerializableUnMarshaller;

/**
 * 
 * A JMSWireFormat.
 * 
 * For some invocations, e.g. message sends, acknowledgements and activations it
 * is vital that we minimise the amount of data sent in the invocation so that we
 * can maximise performance.
 * By default, Invocations and return values are sent over the wire as serialized Java
 * objects.
 * This adds considerable overhead in terms of the amount of data sent
 * (It adds class information plus block data information)
 * which significantly degrades performance.
 * Therefore for the invocations where performance is paramount we define a customer wire
 * format that minimises the amount of data sent.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
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
   
   protected static final byte SERIALIZED = 0;
   
   protected static final byte SEND = 1;
   
   protected static final byte ACTIVATE = 2;
   
   protected static final byte DEACTIVATE = 3;
   
   protected static final byte GETMESSAGENOW = 4;
   
   protected static final byte ACKNOWLEDGE = 5;
   
   protected static final byte CALLBACK = 6;
   
   protected static final byte NULL_RESPONSE = 7;
   
   protected static final byte MESSAGE_RESPONSE = 8;
         
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
   
   private void writeHeader(MethodInvocation mi, DataOutputStream dos) throws IOException
   {
      int objectId = ((Integer)mi.getMetaData().getMetaData(Dispatcher.DISPATCHER, Dispatcher.OID)).intValue();
      
      dos.writeInt(objectId);
      
      dos.writeLong(mi.getMethodHash());
   }
   
   private MethodInvocation readHeader(DataInputStream dis) throws IOException
   {
      int objectId = dis.readInt();
      
      long methodHash = dis.readLong();                  
      
      MethodInvocation mi = new MethodInvocation(null, methodHash, null, null, null);                 
      
      mi.getMetaData().addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, new Integer(objectId));
      
      return mi;
   }
   
   public void write(Object obj, OutputStream out) throws IOException
   {
      //Sanity check
      if (!(out instanceof JMSObjectOutputStream))
      {
         throw new IllegalStateException("OutputStream must be an JMSObjectOutputStream");
      }
           
      JMSObjectOutputStream oos = (JMSObjectOutputStream)out;
      DataOutputStream dos = oos.getDataOutputStream();
      
      if (obj instanceof InvocationRequest)
      {         
         if (trace) { log.trace("Writing InvocationRequest"); }
         
         InvocationRequest req = (InvocationRequest)obj;
         
         Object param = req.getParameter();
         
         if (trace) { log.trace("Param is " + param); }
         
         if (param instanceof MethodInvocation)
         {            
            MethodInvocation mi = (MethodInvocation)param;
            
            String methodName = mi.getMethod().getName();
            
            if (trace) { log.trace("MethodInvocation, Method name:" + methodName); }
            
            if (methodName.equals("send"))
            {               
               dos.writeByte(SEND);
               
               writeHeader(mi, dos);
               
               JBossMessage m = (JBossMessage)mi.getArguments()[0];                              
               
               dos.writeByte(m.getType());
               
               m.writeExternal(oos);
               
               oos.flush();  
               
               if (trace) { log.trace("Wrote send"); }
               
            }
            else if ("activate".equals(methodName))
            {
               dos.writeByte(ACTIVATE);
               
               writeHeader(mi, dos);                            
               
               dos.flush();
               
               if (trace) { log.trace("Wrote activate"); }
            }
            else if ("deactivate".equals(methodName))
            {
               dos.writeByte(DEACTIVATE);
               
               writeHeader(mi, dos);                             
               
               dos.flush();
               
               if (trace) { log.trace("Wrote deactivate"); }
            }
            else if ("getMessageNow".equals(methodName))
            {
               dos.writeByte(GETMESSAGENOW);
               
               writeHeader(mi, dos);    
               
               boolean wait = ((Boolean)mi.getArguments()[0]).booleanValue();
               
               dos.writeBoolean(wait);
               
               dos.flush();
               
               if (trace) { log.trace("Wrote getMessageNow"); }
            }
            else if ("acknowledge".equals(methodName))
            {
               dos.writeByte(ACKNOWLEDGE);
               
               writeHeader(mi, dos);                              
               
               dos.flush();
               
               if (trace) { log.trace("Wrote acknowledge"); }
            }
            else
            {
               dos.write(SERIALIZED);
               
               //Delegate to serialization to handle the wire format
               serializableMarshaller.write(obj, oos);
               
               if (trace) { log.trace("Wrote using standard serialization"); }
            }
         }
         else if (param instanceof DeliveryRunnable)
         {
            //Message delivery callback
            
            if (trace) { log.trace("DeliveryRunnable"); }
               
            DeliveryRunnable dr = (DeliveryRunnable)param;
            
            dos.writeByte(CALLBACK);
                    
            int consumerID = dr.getConsumerID();
            
            MessageDelegate del = dr.getMessageDelegate();
            
            dos.writeInt(consumerID);
            
            dos.writeByte(del.getMessage().getType());
            
            dos.writeInt(del.getDeliveryCount());
            
            del.getMessage().writeExternal(oos);
            
            oos.flush();    
            
            if (trace) { log.trace("Wrote DeliveryRunnable"); }
         }            
         else
         {           
            dos.write(SERIALIZED);
            
            //Delegate to serialization to handle the wire format
            serializableMarshaller.write(obj, oos);
            
            if (trace) { log.trace("Wrote using standard serialization"); }
         }
      }
      else if (obj instanceof InvocationResponse)
      {
         if (trace) { log.trace("Writing InvocationResponse"); }
         
         InvocationResponse resp = (InvocationResponse)obj;
         
         Object res = resp.getResult();
         
         if (trace) { log.trace("Result is " + res); }
         
         if (res == null && !resp.isException())
         {
            dos.write(NULL_RESPONSE);
            
            dos.flush();            
            
            if (trace) { log.trace("Wrote null response"); }
         }
         else if (res instanceof MessageDelegate)
         {
            //Return value from getMessageNow
            dos.write(MESSAGE_RESPONSE);
            
            MessageDelegate del = (MessageDelegate)res;
            
            dos.writeByte(del.getMessage().getType());
            
            dos.writeInt(del.getDeliveryCount());
            
            del.getMessage().writeExternal(oos);
            
            oos.flush();
            
            if (trace) { log.trace("Wrote message response"); }
         }
         else
         {      
            dos.write(SERIALIZED);
              
            //Delegate to serialization to handle the wire format
            serializableMarshaller.write(obj, out);
            
            if (trace) { log.trace("Wrote using standard serialization"); }
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
      if (!(in instanceof JMSObjectInputStream))
      {
         throw new IllegalStateException("InputStream must be an JMSObjectInputStream");
      }
      
      JMSObjectInputStream ois = (JMSObjectInputStream)in;
      DataInputStream dis = ois.getDataInputStream();
      
      byte formatType = (byte)dis.read();
      
      if (trace) { log.trace("Reading, format type=" + formatType); }
       
      switch (formatType)
      {
         case SERIALIZED:            
         {            
            //Delegate to serialization
            Object ret = serializableUnMarshaller.read(ois, map);            
            
            if (trace) { log.trace("Read using standard serialization"); }
            
            return ret;            
         }            
         case SEND:
         {            
            MethodInvocation mi = readHeader(dis);
            
            byte messageType = dis.readByte();
            
            JBossMessage m = (JBossMessage)MessageFactory.createMessage(messageType);
            
            m.readExternal(ois);
                        
            Object[] args = new Object[] {m};
            
            mi.setArguments(args);
            
            InvocationRequest request = new InvocationRequest(null, null, mi, null, null, null);
            
            if (trace) { log.trace("Read send"); }
            
            return request; 
         }            
         case ACTIVATE:            
         {
            MethodInvocation mi = readHeader(dis);                
            
            InvocationRequest request = new InvocationRequest(null, null, mi, null, null, null);
            
            if (trace) { log.trace("Read activate"); }
            
            return request;      
         }
         case DEACTIVATE:            
         {
            MethodInvocation mi = readHeader(dis);
            
            InvocationRequest request = new InvocationRequest(null, null, mi, null, null, null);
            
            if (trace) { log.trace("Read deactivate"); }
            
            return request;      
         }
         case GETMESSAGENOW:            
         { 
            MethodInvocation mi = readHeader(dis);
            
            boolean wait = dis.readBoolean();
                                                
            Object[] args = new Object[] {Boolean.valueOf(wait)};
            
            mi.setArguments(args);
                
            InvocationRequest request = new InvocationRequest(null, null, mi, null, null, null);
            
            if (trace) { log.trace("Read getMessageNow"); }
            
            return request;      
         }
         case ACKNOWLEDGE:
         {
            MethodInvocation mi = readHeader(dis);                
 
            InvocationRequest request = new InvocationRequest(null, null, mi, null, null, null);
            
            if (trace) { log.trace("Read acknowledge"); }
            
            return request;
         }         
         case CALLBACK:
         {
            int consumerID = dis.readInt();
            
            byte type = dis.readByte();
            
            int deliveryCount = dis.readInt();

            JBossMessage m = (JBossMessage)MessageFactory.createMessage(type);
            
            m.readExternal(ois);
            
            MessageDelegate md = JBossMessage.createThinDelegate(m, deliveryCount);
            
            DeliveryRunnable dr = new DeliveryRunnable(md, null, consumerID, trace);
            
            InvocationRequest request = new InvocationRequest(null, null, dr, null, null, null);
            
            if (trace) { log.trace("Read callback"); }
            
            return request;            
         }
         case MESSAGE_RESPONSE:
         {
            byte type = dis.readByte();
            
            int deliveryCount = dis.readInt();

            JBossMessage m = (JBossMessage)MessageFactory.createMessage(type);
            
            m.readExternal(ois);
            
            MessageDelegate md = JBossMessage.createThinDelegate(m, deliveryCount);
                        
            InvocationResponse resp = new InvocationResponse(null, md, false, null);
            
            if (trace) { log.trace("Read message response"); }
            
            return resp;            
         }
         case NULL_RESPONSE:
         {
            InvocationResponse resp = new InvocationResponse(null, null, false, null);
            
            if (trace) { log.trace("Read null response"); }
            
            return resp;
         }
         default:
         {
            throw new IllegalStateException("Invalid format type " + formatType);
         }           
      }      
   }  
}
