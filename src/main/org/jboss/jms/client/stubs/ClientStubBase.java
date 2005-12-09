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
package org.jboss.jms.client.stubs;

import java.io.Serializable;

import org.jboss.aop.Advised;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.InvocationResponse;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.invocation.unified.marshall.InvocationMarshaller;
import org.jboss.invocation.unified.marshall.InvocationUnMarshaller;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.marshal.MarshalFactory;
import org.jboss.remoting.marshal.Marshaller;
import org.jboss.remoting.marshal.UnMarshaller;

/**
 * Base class for all client stub classes.
 * 
 * Client stub classes provide an emptuy implementation of the appropriate delegate
 * interface.
 * The classes are advised using JBoss AOP to proviede the client side
 * advice stack.
 * The delegate methods in the stub class will never actually be invoked
 * since they will either be handled in the advice stack or invoked on the server
 * before reaching the stub.
 * The stubs are created on the server and serialized back to the client.
 * When they arrive on the client, the init() method is called which causes
 * the advices to be bound to the advised class.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class ClientStubBase implements Interceptor, Serializable
{
   private static final long serialVersionUID = 8005108339439737469L;

   private static final Logger log = Logger.getLogger(ClientStubBase.class);
   
   protected String objectID;
   
   protected InvokerLocator locator;
   
   private Client client;         
   
   public ClientStubBase(String objectID, InvokerLocator locator)
   {
      this.objectID = objectID;
      this.locator = locator;      
   }
   
   /**
    * ClientStubBase also acts as an interceptor - the last interceptor in the chain
    * which invokes on the server
    */
   public Object invoke(Invocation invocation) throws Throwable
   {      
      if (log.isTraceEnabled()) { log.trace("invoking " + ((MethodInvocation)invocation).getMethod().getName() + " on server"); }
      
      invocation.getMetaData().addMetaData(Dispatcher.DISPATCHER,
                                          Dispatcher.OID,
                                          objectID,
                                          PayloadKey.AS_IS);
                        
      InvocationResponse response = (InvocationResponse)client.invoke(invocation, null);
      invocation.setResponseContextInfo(response.getContextInfo());
            
      if (log.isTraceEnabled()) { log.trace("got server response for " + ((MethodInvocation)invocation).getMethod().getName()); }

      return response.getResponse();
   }
   
   public String getName()
   {
      return "Invoker";
   }
   
   /**
    *  Add Invoking interceptor and prepare the stack for invocations
    */
   public void init()
   {          
      ((Advised)this)._getInstanceAdvisor().appendInterceptor(this);
      
      checkMarshallers();
      
      try
      {
         client = new Client(locator, "JMS");
      }
      catch (Exception e)
      {
         throw new RuntimeException("Failed to create client", e);  
      }
      
      //Add to meta data         
      getMetaData().addMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.INVOKER_CLIENT, client, PayloadKey.TRANSIENT);
      
      getMetaData()
      .addMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.INVOKER_LOCATOR, locator, PayloadKey.TRANSIENT);
      
   }
   
   /**
    *  Check that the correct marshallers have been registered wit the MarshalFactory
    *  This is only done once.
    *
    */
   private synchronized void checkMarshallers()
   {
      if (checked)
      {
         return;
      }
      //We shouldn't have to do this programmatically - it should pick it up from the params
      //on the locator uri, but that doesn't seem to work
      Marshaller marshaller = new InvocationMarshaller();
      UnMarshaller unmarshaller = new InvocationUnMarshaller();
      MarshalFactory.addMarshaller(InvocationMarshaller.DATATYPE, marshaller, unmarshaller);
      checked = true;
   }
   
   private static boolean checked;
   
   protected SimpleMetaData getMetaData()
   {
      return ((Advised)this)._getInstanceAdvisor().getMetaData();
   } 
    
}
