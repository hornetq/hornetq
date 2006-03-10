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
package org.jboss.jms.client.delegate;

import java.io.Serializable;

import org.jboss.aop.Advised;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.serialization.SerializationStreamFactory;

/**
 * Base class for all client-side delegate classes.
 * 
 * Client-side delegate classes provide an empty implementation of the appropriate delegate
 * interface. The classes are advised using JBoss AOP to provide the client side advice stack.
 * The methods in the delegate class will never actually be invoked since they will either be
 * handled in the advice stack or invoked on the server before reaching the delegate.
 *
 * The delegates are created on the server and serialized back to the client. When they arrive on
 * the client, the init() method is called which causes the advices to be bound to the advised
 * class.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DelegateSupport implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 8005108339439737469L;

   private static final Logger log = Logger.getLogger(DelegateSupport.class);

   // Attributes ----------------------------------------------------

   protected int id;
   
   protected HierarchicalState state;
   
   private boolean trace;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DelegateSupport(int objectID)
   {
      this.id = objectID;    
      
      trace = log.isTraceEnabled();
   }
   
   public DelegateSupport()
   {      
   }

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "Invoker";
   }

   /**
    * DelegateSupport also acts as an interceptor - the last interceptor in the chain which
    * invokes on the server.
    */
   public Object invoke(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("invoking " + ((MethodInvocation)invocation).getMethod().getName() + " on server"); }

      invocation.getMetaData().addMetaData(Dispatcher.DISPATCHER,
                                           Dispatcher.OID,
                                           new Integer(id), PayloadKey.AS_IS);
      
      byte version = getState().getVersionToUse().getProviderIncrementingVersion();
      
      MessagingMarshallable mm = new MessagingMarshallable(version, invocation);

      mm = (MessagingMarshallable)getClient().invoke(mm, null);            

      if (trace) { log.trace("got server response for " + ((MethodInvocation)invocation).getMethod().getName()); }

      return mm.getLoad();
   }

   // Public --------------------------------------------------------

   public HierarchicalState getState()
   {
      return state;
   }
   
   public void setState(HierarchicalState state)
   {
      this.state = state;
   }
   
   
   /**
    *  Add Invoking interceptor and prepare the stack for invocations.
    */
   public void init()
   {          
      ((Advised)this)._getInstanceAdvisor().appendInterceptor(this);
      
      checkMarshallers();
        
   }

   public int getID()
   {
      return id;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected SimpleMetaData getMetaData()
   {
      return ((Advised)this)._getInstanceAdvisor().getMetaData();
   }
   
   protected abstract Client getClient();


   // Private -------------------------------------------------------

   private static boolean checked;

   /**
    * Check that the correct marshallers have been registered with the MarshalFactory.
    * This is only done once.
    */
   private synchronized void checkMarshallers()
   {
      if (checked)
      {
         return;
      }
      
      //We explicitly associate the datatype "jms" with our customer SerializationManager
      //This is vital for performance reasons.
      try
      {
         SerializationStreamFactory.setManagerClassName(
            "jms", "org.jboss.remoting.serialization.impl.jboss.JBossSerializationManager");
      }
      catch (Exception e)
      {
         log.error("Failed to set SerializationManager, e");
      }
      
      checked = true;
   }

   // Inner classes -------------------------------------------------

}
