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
package org.jboss.jms.client.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.client.api.ClientConnection;
import org.jboss.messaging.util.ProxyFactory;
import org.jboss.messaging.util.Streamable;

/**
 * 
 * A CreateConnectionResult
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class CreateConnectionResult implements Streamable
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   private static final int NULL = 0;
   
   private static final int NOT_NULL = 1;

   // Attributes ----------------------------------------------------

   private ClientConnectionImpl delegate;

   private int actualFailoverNodeID;

   // TODO: Get rid of this
   transient private ClientConnection proxiedDelegate;


   // Constructors --------------------------------------------------
   
   public CreateConnectionResult()
   {      
   }

   public CreateConnectionResult(ClientConnectionImpl delegate)
   {
      this(delegate, Integer.MIN_VALUE);
   }

   public CreateConnectionResult(int actualFailoverNodeID)
   {
      this(null, actualFailoverNodeID);
   }

   private CreateConnectionResult(ClientConnectionImpl delegate,
                                  int actualFailoverNodeId)
   {
      this.delegate = delegate;
      this.actualFailoverNodeID = actualFailoverNodeId;
   }

   // Public --------------------------------------------------------

   public ClientConnection getInternalDelegate()
   {
      return delegate;
   }

   public ClientConnection getProxiedDelegate()
   {
      // TODO: Get rid of this Proxy
      if (proxiedDelegate == null)
      {
         proxiedDelegate = (ClientConnection) ProxyFactory.proxy(delegate, ClientConnection.class);

      }
      return proxiedDelegate;
   }

   public int getActualFailoverNodeID()
   {
      return actualFailoverNodeID;
   }

   public String toString()
   {
      return "CreateConnectionResult[" + delegate + ", failover node " + actualFailoverNodeID + "]";
   }
   
   // Streamable implementation ------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      actualFailoverNodeID = in.readInt();
      
      int b = in.readByte();
      
      if (b == NOT_NULL)
      {
         delegate = new ClientConnectionImpl();
         
         delegate.read(in);
      }
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(actualFailoverNodeID);
      
      if (delegate == null)
      {
         out.writeByte(NULL);
      }
      else
      {
         out.writeByte(NOT_NULL);
         
         delegate.write(out);
      }         
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
