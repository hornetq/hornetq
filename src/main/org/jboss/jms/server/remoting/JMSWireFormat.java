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
import java.util.List;
import java.util.Map;

import org.jboss.jms.wireformat.ClientDelivery;
import org.jboss.jms.wireformat.ConnectionFactoryUpdate;
import org.jboss.jms.wireformat.NullResponse;
import org.jboss.jms.wireformat.PacketSupport;
import org.jboss.jms.wireformat.PolledCallbacksDelivery;
import org.jboss.jms.wireformat.RequestSupport;
import org.jboss.jms.wireformat.ResponseSupport;
import org.jboss.jms.wireformat.SerializedPacket;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvocationResponse;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.invocation.InternalInvocation;
import org.jboss.remoting.invocation.OnewayInvocation;
import org.jboss.remoting.marshal.Marshaller;
import org.jboss.remoting.marshal.UnMarshaller;

/**
 * 
 * A JMSWireFormat.
 * 
 * We do not use Java or JBoss serialization to send data over the wire.
 * Serialization adds considerable overhead in terms of the amount of data sent (it adds class information
 * plus block data information) which significantly degrades performance.
 * 
 * Instead we define a customer wire format that minimises the
 * amount of data sent.
 * 
 * The only exception to this rule is when sending an ObjectMessage which contains a user
 * defined object whose type is only known at run-time. In this case we use serialization.
 * 
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSWireFormat implements Marshaller, UnMarshaller
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = -7646123424863782043L;

   private static final Logger log = Logger.getLogger(JMSWireFormat.class);
   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   protected boolean trace;

   // Constructors ---------------------------------------------------------------------------------

   public JMSWireFormat()
   {
      trace = log.isTraceEnabled();
   }

   // Marshaller implementation --------------------------------------------------------------------

   public void write(Object obj, OutputStream out) throws IOException
   {          
      if (trace) { log.trace("Writing " + obj); }
      
      DataOutputStream dos;
      
      if (out instanceof DataOutputStream)
      {
         //For non HTTP transports - we should ALWAYS be passed a DataOutputStream
         //We do this by specifying socket wrapper classes on the locator uri
         dos = (DataOutputStream)out;
         
         if (trace) { log.trace("Stream is a DataOutputStream"); }
      }
      else
      {
         // Further sanity check
         if (out instanceof ObjectOutputStream)
         {
            throw new IllegalArgumentException("Invalid stream - are you sure you have " +
                                               "configured socket wrappers?");
         }
         
         // This would be the case for the HTTP transport for example. Wrap the stream.
         
         //FIXME - remoting should be fixed to allow socket wrappers to be specified for
         //all transports - not just socket
         //until then we have no choice but to create a wrapping stream on every invocation
         //for HTTP transport
         dos = new DataOutputStream(out);
         
         if (trace) { log.trace("Stream is NOT a DataOutputStream - must be using HTTP transport"); }
      }                  
      
      try
      {
         PacketSupport packet = null;
         
         if (obj instanceof InvocationRequest)
         {                        
            InvocationRequest req = (InvocationRequest)obj;
            
            Object param = req.getParameter();
            
            if (param instanceof PacketSupport)
            {
               // A JBM invocation
               packet = (PacketSupport)param;    
               
               if (trace) { log.trace("JBM Request"); }
            }
            else if (param instanceof OnewayInvocation)
            {
               if (trace) { log.trace("It's a OnewayInvocation"); }
               
               // This is fucking horrendous - remoting should not force us to deal with
               // its internal objects
               
               // Hmmm... hidden somewhere in the depths of this crap there must be the callback,
               // The search begins....

               OnewayInvocation oneWay = (OnewayInvocation)param;
               
               param = oneWay.getParameters()[0];
               
               if (param instanceof RequestSupport)
               {
                  //A JBM invocation being sent one way (e.g. changeRate)
                  
                  if (trace) { log.trace("JBM oneway request"); }
                  
                  packet = (PacketSupport)param;
               }
               else if (param instanceof InternalInvocation)
               {                      
                  InternalInvocation ii = (InternalInvocation)param;
                  
                  Object[] params = ii.getParameters();
                  
                  if (trace) { log.trace("Params is " + params); }
                  
                  if (params != null && params.length > 0 && params[0] instanceof Callback)
                  {
                     Callback callback = (Callback) params[0];
                     
                     if (trace) { log.trace("It's a callback: " + callback); }
                     
                     if (callback.getParameter() instanceof ClientDelivery)
                     {
                        // Whooooppee!! found the callback. Hurrah for remoting!
                        // What a simple and intuitive API ;)
                        
                        packet = (ClientDelivery)callback.getParameter();
                        
                        if (trace) { log.trace("Message delivery callback"); }
                     }
                     else if (callback.getParameter() instanceof ConnectionFactoryUpdate)
                     {
                        packet = (ConnectionFactoryUpdate)callback.getParameter();
                        
                        if (trace) { log.trace("Connection factory update callback"); }
                     }
                  }
               }
               else
               {
                  throw new IllegalArgumentException("Invalid request param " + param);
               }
            }
            else
            {               
               //Some other remoting internal thing, e.g. PING, DISCONNECT, add listener etc
               
               packet = new SerializedPacket(req);               
            }
         }         
         else if (obj instanceof InvocationResponse)
         {
            InvocationResponse resp = (InvocationResponse)obj;
            
            Object param = resp.getResult();
            
            if (param instanceof ResponseSupport)
            {
               // A JBM invocation response
               
               packet = (ResponseSupport)param;
             
               if (trace) { log.trace("JBM Response"); }
            }
            else if (param instanceof List)
            {
               // List of polled Callbacks, this is how messages are delivered when using
               // polled callbacks e.g. the HTTP transport
               
               // Sanity check
               if (((List)param).isEmpty())
               {
                  log.warn("Got a polled callback list - but it is empty!!! " +
                     "See http://jira.jboss.org/jira/browse/JBMESSAGING-818");
               }
               
               packet = new PolledCallbacksDelivery((List)param, resp.getSessionId());             
            }
            else if (param == null)
            {
               // Null response
               packet = new NullResponse();
               
               if (trace) { log.trace("Null Response"); }
            }
            else
            {
               // Return value from some remoting internal invocation e.g. PONG - return value from PING
               packet = new SerializedPacket(obj);
            }
         }
         else
         {
            //Actually this should never occur
            packet = new SerializedPacket(obj);
         }
             
         if (trace) { log.trace("Writing packet: " + packet); }
         
         packet.write(dos);
         
         if (trace) { log.trace("Wrote packet"); }
      }
      catch (Exception e)
      {
         log.error("Failed to write packet", e);
         IOException e2 = new IOException(e.getMessage());
         e2.setStackTrace(e.getStackTrace());
         throw e2;
      }
   }
   
   public Marshaller cloneMarshaller() throws CloneNotSupportedException
   {
      return this;
   }

   // UnMarshaller implementation ------------------------------------------------------------------

   public Object read(InputStream in, Map map) throws IOException, ClassNotFoundException
   {            
      if (trace) { log.trace("Reading"); }
      
      DataInputStream dis;
      
      if (in instanceof DataInputStream)
      {
         //For non HTTP transports - we should ALWAYS be passed a DataInputStream
         //We do this by specifying socket wrapper classes on the locator uri
         dis = (DataInputStream)in;         
         
         if (trace) { log.trace("Stream is already DataInputStream :)"); }
      }
      else
      {        
         // Further sanity check
         if (in instanceof ObjectInputStream)
         {
            throw new IllegalArgumentException("Invalid stream - are you sure you have " +
                                               "configured socket wrappers?");
         }
         
         // This would be the case for the HTTP transport for example. Wrap the stream
         
         //FIXME Ideally remoting would let us wrap this before invoking the marshaller
         //     but this does not appear to be possible
         dis = new DataInputStream(in);
         
         if (trace) { log.trace("Stream is NOT DataInputStream - must be using HTTP transport"); }
      }
      
      int id = dis.readInt();
      
      PacketSupport packet = PacketSupport.createPacket(id);
      
      if (trace) { log.trace("Created packet " + packet); }
      
      try
      {
         if (trace) { log.trace("Reading packet"); }
         
         packet.read(dis);
         
         if (trace) { log.trace("Read packet"); }
      }
      catch (Exception e)
      {
         IOException e2 = new IOException(e.getMessage());
         e2.setStackTrace(e.getStackTrace());
         throw e2;
      }
      
      Object payload = packet.getPayload();
      
      if (trace) { log.trace("Returning payload: " + payload); }
      
      return payload;
   }
      
   public UnMarshaller cloneUnMarshaller() throws CloneNotSupportedException
   {
      return this;
   }

   public void setClassLoader(ClassLoader classloader)
   {
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------      

   // Inner classes --------------------------------------------------------------------------------
}
