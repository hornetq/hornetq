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
package org.jboss.jms.server.endpoint.delegate;

import javax.jms.JMSException;

import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.endpoint.ConnectionEndpoint;
import org.jboss.jms.tx.TransactionRequest;

/**
 * 
 * Delegate class for ConnectionEndpoint
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ConnectionEndpointDelegate extends EndpointDelegateBase  implements ConnectionEndpoint
{
   protected ConnectionEndpoint del;
   
   public ConnectionEndpointDelegate(ConnectionEndpoint del)
   {
      this.del = del;
   }
   
   public Object getEndpoint()
   {
      return del;
   }

   public void close() throws JMSException
   {
      del.close();
   }

   public void closing() throws JMSException
   {
      del.closing();
   }

   public SessionDelegate createSessionDelegate(boolean transacted, int acknowledgmentMode, boolean isXA) throws JMSException
   {
      return del.createSessionDelegate(transacted, acknowledgmentMode, isXA);
   }

   public String getClientID() throws JMSException
   {
      return del.getClientID();
   }

   public void sendTransaction(TransactionRequest request) throws JMSException
   {
      del.sendTransaction(request);
   }

   public void setClientID(String id) throws JMSException
   {
      del.setClientID(id);
   }

   public void start() throws JMSException
   {
      del.start();
   }

   public void stop() throws JMSException
   {
      del.stop();
   }
   
}
