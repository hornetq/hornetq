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

   private static boolean trace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   //This is set on the server
   protected int id;

   //This is set on the client
   //The reason we don't use the meta-data to store the state for the delegate is to avoid
   //the extra HashMap lookup that would entail.
   //This can be significant since the state could be queried for many aspects
   //in an a single invocation
   protected transient HierarchicalState state;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DelegateSupport(int objectID)
   {
      this.id = objectID;
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
                                           new Integer(id),
                                           PayloadKey.AS_IS);

      byte version = getState().getVersionToUse().getProviderIncrementingVersion();

      MessagingMarshallable request = new MessagingMarshallable(version, invocation);
      MessagingMarshallable response = (MessagingMarshallable)getClient().invoke(request, null);

      if (trace) { log.trace("got server response for " + ((MethodInvocation)invocation).getMethod().getName()); }

      return response.getLoad();
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

    /**
     * During HA events, a new object is created on the new server and the state on that new object
     * has to be transfered to this actual object. For example, a Connection will have to assume the
     * ObjectID of the new connection endpoint and the new RemotingConnection.
     */
    public void copyAttributes(DelegateSupport newDelegate)
    {
        id = newDelegate.getID();
    }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected SimpleMetaData getMetaData()
   {
      return ((Advised)this)._getInstanceAdvisor().getMetaData();
   }

   protected abstract Client getClient() throws Exception;


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

      checked = true;
   }

   // Inner classes -------------------------------------------------

}
