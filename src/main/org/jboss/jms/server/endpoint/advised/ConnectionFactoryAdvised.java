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
package org.jboss.jms.server.endpoint.advised;

import javax.jms.JMSException;

import org.jboss.jms.server.endpoint.ConnectionFactoryEndpoint;
import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.messaging.core.plugin.IDBlock;

/**
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>1.5</tt>
 *
 * ConnectionFactoryAdvised.java,v 1.3 2006/03/01 22:56:51 ovidiu Exp
 */
public class ConnectionFactoryAdvised extends AdvisedSupport implements ConnectionFactoryEndpoint
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConnectionFactoryEndpoint endpoint;

   // Constructors --------------------------------------------------

   public ConnectionFactoryAdvised()
   {
   }

   public ConnectionFactoryAdvised(ConnectionFactoryEndpoint endpoint)
   {
      this.endpoint = endpoint;
   }

   // ConnectionFactoryEndpoint implementation -----------------------

   public CreateConnectionResult createConnectionDelegate(String username,
                                                          String password,
                                                          int failedNodeId)
      throws JMSException
   {
      return endpoint.createConnectionDelegate(username, password, failedNodeId);
   }

   public byte[] getClientAOPStack() throws JMSException
   {
      return endpoint.getClientAOPStack();
   }

   // AdvisedSupport override ---------------------------------------

   public Object getEndpoint()
   {
      return endpoint;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "ConnectionFactoryAdvised->" + endpoint;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
