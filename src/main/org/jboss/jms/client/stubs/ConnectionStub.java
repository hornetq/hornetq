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
package org.jboss.jms.client.stubs;

import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.remoting.InvokerLocator;

/**
 * 
 * The client stub class for ConnectionDelegate
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ConnectionStub extends ClientStubBase implements ConnectionDelegate
{
   private static final long serialVersionUID = 6680015509555859038L;
   
   public ConnectionStub(String objectID, InvokerLocator locator)
   {
      super(objectID, locator);
   }
   
   public void close() throws JMSException
   {      
   }

   public void closing() throws JMSException
   {
   }

   public JBossConnectionConsumer createConnectionConsumer(Destination dest, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException
   {
      return null;
   }

   public SessionDelegate createSessionDelegate(boolean transacted, int acknowledgmentMode, boolean isXA) throws JMSException
   {
      return null;
   }

   public String getClientID() throws JMSException
   {
      return null;
   }

   public ConnectionMetaData getConnectionMetaData() throws JMSException
   {
      return null;
   }

   public ExceptionListener getExceptionListener() throws JMSException
   {
      return null;
   }

   public void sendTransaction(TransactionRequest request) throws JMSException
   {     
   }

   public void setClientID(String id) throws JMSException
   {
   }

   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
   }

   public void start() throws JMSException
   {
   }

   public void stop() throws JMSException
   {
   }

}
