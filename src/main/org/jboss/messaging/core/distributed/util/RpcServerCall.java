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
package org.jboss.messaging.core.distributed.util;

import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.util.RspList;
import org.jgroups.util.Rsp;
import org.jgroups.Address;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;
import java.util.ArrayList;

/**
 * A distributed method call addressed to RpcDispatchers configured with RpcServers. It is a simple
 * extension of the MethodCall that offers built-in support for server category. It also offers
 * convenience methods to return results in a normalized format.
 * 
 * @see org.jboss.messaging.core.distributed.util.RpcServer
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RpcServerCall extends MethodCall
{
   private static final Logger log = Logger.getLogger(RpcServerCall.class);

   protected Serializable serverCategory;

   /**
    * Must be used by externalization only.
    */
   public RpcServerCall()
   {
      super();
   }

   /**
    * @param serverCategory - the server category. Will be used by the remote RpcServer to select
    *        a set of equivalent sub-servers.
    * @param methodName - the remote method name.
    * @param args - the remote method's arguments.
    * @param types - the argument types (fully qualified class names).
    */
   public RpcServerCall(Serializable serverCategory, String methodName,
                        Object[] args, String[] types)
   {
      super("invoke",
            new Object[] {serverCategory,
                          methodName,
                          args,
                          types},
            new String[] {"java.io.Serializable",
                          "java.lang.String",
                          "[Ljava.lang.Object;",
                          "[Ljava.lang.String;"});
      this.serverCategory = serverCategory;
   }

   public Serializable getServerCategory()
   {
      return serverCategory;
   }

   /**
    * Synchronously invokes the RpcServerCall on <i>all</i> server delegates registered under the
    * RPCServerCall's current category, across all RpcServers of the group.
    *
    * @param dispatcher - the dispatcher to use to sent the call from.
    * @param timeout
    *
    * @return a Collection of ServerResponses.
    */
   public Collection remoteInvoke(RpcDispatcher dispatcher, long timeout)
   {
      return remoteInvoke(dispatcher, (Vector)null, timeout);
   }


   /**
    * Synchronously invokes the RpcServerCall on all server delegates registered under the
    * RPCServerCall's current category, across select RpcServers.
    *
    * @param dispatcher - the dispatcher to use to sent the call from.
    * @param destinations - contains the Addresses of the group members that runs the rpcServers
    *        we want to invoke on.
    * @param timeout
    *
    * @return a Collection of ServerResponses.
    */

   public Collection remoteInvoke(RpcDispatcher dispatcher, Vector destinations, long timeout)
   {
      // TODO for the time being, I am doing synchronous for all
      Vector dests = null;
      int mode = GroupRequest.GET_ALL;
      Collection results = new ArrayList();

      if (log.isTraceEnabled()) { log.trace("calling " + this + " on dispatcher"); }

      RspList rspList = dispatcher.callRemoteMethods(dests, this, mode, timeout);

      for(int i = 0; i < rspList.size(); i++)
      {
         try
         {
            Rsp response = (Rsp)rspList.elementAt(i);

            Address address = response.getSender();

            Object result = response.getValue();
            if (result instanceof Throwable)
            {
               // the remote "invoke" has thrown an exception, the whole category has problems
               results.add(new ServerResponse(address, serverCategory, null, result));
               continue;
            }

            Collection subServerResponses = (Collection)result;
            for(Iterator j = subServerResponses.iterator(); j.hasNext(); )
            {
               SubordinateServerResponse ssr = (SubordinateServerResponse)j.next();
               ServerResponse r = new ServerResponse(address,
                                                     serverCategory,
                                                     ssr.getSubServerID(),
                                                     ssr.getInvocationResult());
               results.add(r);
            }
         }
         catch(Exception e)
         {
            results.add(new ServerResponse(null, serverCategory, null, e));
         }
      }
      return results;
   }

   /**
    * Convenientce method for the case the remote call is invoked on a <i>single</i> rpcServer
    * and the server category we're invoking on has a <i>unique</i> server delegate.
    *
    * @param dispatcher - the dispatcher to use to sent the call from.
    * @param destination - the Address of the group member that runs the rpcServer
    * @param timeout
    *
    * @return the result of the invocation or null. The returned instance is never an exception
    *         instance, unless the remote method chooses to return an exception as the result of
    *         the invocation.
    *
    * @throws Exception - the exception that was thrown by the remote invocation.
    * @throws ClassCastException - for unexpected result types
    * @throws IllegalStateException - when receiving 0 or more than 1 replies. This usually happens
    *         when there is more than one server delegate listening on the category.
    */
   public Object remoteInvoke(RpcDispatcher dispatcher, Address destination, long timeout)
         throws Exception
   {
      int mode = GroupRequest.GET_ALL;

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      Object result = dispatcher.callRemoteMethod(destination, this, mode, timeout);

      if (result instanceof Exception)
      {
         throw (Exception)result;
      }

      Collection results = (Collection)result;
      if (results.size() != 1)
      {
         throw new IllegalStateException("Expecting exactly one remote result, got " +
                                         results.size() + " instead");
      }
      SubordinateServerResponse r = (SubordinateServerResponse)results.iterator().next();
      Object ir = r.getInvocationResult();
      if (ir instanceof Exception)
      {
         throw (Exception)ir;
      }
      return ir;
   }
}
