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
package org.jboss.jms.client.container;

import java.io.ObjectStreamException;
import java.io.Serializable;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.util.PayloadKey;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.marshal.MarshalFactory;
import org.jboss.remoting.marshal.Marshaller;
import org.jboss.remoting.marshal.UnMarshaller;

/**
 * 
 * This interceptor has the responsibility of ensuring a remoting client has been created
 * 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemotingClientInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 5573067291291115600L;

   private static final Logger log = Logger.getLogger(RemotingClientInterceptor.class);

   public static final RemotingClientInterceptor singleton = new RemotingClientInterceptor();

   public static final String REMOTING = "REMOTING";
   public static final String CLIENT = "CLIENT";
   public static final String INVOKER_LOCATOR = "INVOKER_LOCATOR";
   public static final String SUBSYSTEM = "SUBSYSTEM";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   // InvokerInterceptor implementation -----------------------------

   public String getName() { return "RemotingClientInterceptor"; }

   public Object invoke(Invocation invocation) throws Throwable
   {
      Client client = (Client)invocation.getMetaData(REMOTING, CLIENT);
      
      if (client == null)
      {

         if (log.isTraceEnabled()) { log.trace("creating client"); }


         //We shouldn't have to do this programmatically - it should pick it up from the params
         //on the locator uri, but that doesn't seem to work
         Marshaller marshaller = new org.jboss.invocation.unified.marshall.InvocationMarshaller();
         UnMarshaller unmarshaller = new org.jboss.invocation.unified.marshall.InvocationUnMarshaller();
         MarshalFactory.addMarshaller("invocation", marshaller, unmarshaller);
         
         
         InvokerLocator locator = (InvokerLocator)invocation.getMetaData(REMOTING, INVOKER_LOCATOR);
         if (locator == null)
         {
            throw new RuntimeException("No InvokerLocator supplied.  Can't invoke remotely!");
         }

         String subsystem = (String)invocation.getMetaData(REMOTING, SUBSYSTEM);
         if (subsystem == null)
         {
            throw new RuntimeException("No subsystem supplied.  Can't invoke remotely!");
         }

         client = new Client(locator, subsystem);

         if (log.isTraceEnabled()) { log.trace("created client, locator=" + locator + ", subsystem=" + subsystem); }

         invocation.getMetaData().addMetaData(REMOTING, CLIENT, client, PayloadKey.TRANSIENT);
         getHandler(invocation).metadata.addMetaData(REMOTING, CLIENT, client, PayloadKey.TRANSIENT);
      }

      return invocation.invokeNext();
   }

   // Public --------------------------------------------------------

   Object readResolve() throws ObjectStreamException
   {
      return singleton;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }
 
   
   // Inner classes -------------------------------------------------   
}
