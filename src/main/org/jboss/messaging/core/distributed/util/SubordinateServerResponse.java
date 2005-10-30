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


import java.io.Serializable;

/**
 * A carrier for a response coming from a single sub-server object. Instances of this class will
 * be sent over the network.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class SubordinateServerResponse implements Serializable
{
   // Attributes ----------------------------------------------------

   protected Serializable subServerID;
   protected Object result;

   // Constructors --------------------------------------------------

   public SubordinateServerResponse(Serializable subServerID, Object result)
   {
      this.subServerID = subServerID;
      this.result = result;
   }

   // Public --------------------------------------------------------

   public Serializable getSubServerID()
   {
      return subServerID;
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
}
