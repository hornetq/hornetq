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

import org.jgroups.Address;
import org.jboss.messaging.core.distributed.util.RpcServer;

import java.io.Serializable;

/**
 * A wrapper around a response coming from a <i>single</i> server delegate registered with
 * a RpcServer. Returned by the rpcServerCalls.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerResponse
{
   // Attributes ----------------------------------------------------

   protected Address address;
   protected Serializable category;
   protected Serializable subordinateID;
   protected Object result;

   // Constructors --------------------------------------------------

   public ServerResponse(Address address, Serializable category,
                         Serializable subordinateID, Object result)
   {
      this.address = address;
      this.category = category;
      this.subordinateID = subordinateID;
      this.result = result;
   }

   // Public --------------------------------------------------------

   /**
    * Can be null.
    */
   public Address getAddress()
   {
      return address;
   }

   public Serializable getCategory()
   {
      return category;
   }

   public Serializable getSubordinateID()
   {
      return subordinateID;
   }


   /**
    * Return the result as it was returned by the remote sub-server (it can be null), or a
    * Throwable, if the remote invocation generated an exception.
    *
    * @return - the result, null or a Throwable.
    */
   public Object getInvocationResult()
   {
      return result;
   }


   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      sb.append(RpcServer.subordinateToString(category, subordinateID, address));
      sb.append(" result: ");
      if (result == null)
      {
         sb.append(result);
      }
      else
      {
         sb.append(result.getClass().getName());
      }
      return sb.toString();
   }
}
