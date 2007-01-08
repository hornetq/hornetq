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
package org.jboss.jms.server.endpoint;

import java.io.Serializable;

import org.jboss.jms.delegate.ConnectionDelegate;

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
public class CreateConnectionResult implements Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 4311863642735135167L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ConnectionDelegate delegate;

   private int actualFailoverNodeID;

   // Constructors --------------------------------------------------

   public CreateConnectionResult(ConnectionDelegate delegate)
   {
      this(delegate, Integer.MIN_VALUE);
   }

   public CreateConnectionResult(int actualFailoverNodeID)
   {
      this(null, actualFailoverNodeID);
   }

   private CreateConnectionResult(ConnectionDelegate delegate,
                                  int actualFailoverNodeId)
   {
      this.delegate = delegate;
      this.actualFailoverNodeID = actualFailoverNodeId;
   }

   // Public --------------------------------------------------------

   public ConnectionDelegate getDelegate()
   {
      return delegate;
   }

   public int getActualFailoverNodeID()
   {
      return actualFailoverNodeID;
   }

   public String toString()
   {
      return "CreateConnectionResult[" + delegate + ", failover node " + actualFailoverNodeID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
