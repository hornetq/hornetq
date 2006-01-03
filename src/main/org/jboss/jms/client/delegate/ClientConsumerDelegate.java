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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.remoting.InvokerLocator;
import org.jboss.messaging.util.Util;

/**
 * The client-side Consumer delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConsumerDelegate extends DelegateSupport implements ConsumerDelegate
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -2578195153435251519L;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClientConsumerDelegate(String objectID, InvokerLocator locator)
   {
      super(objectID, locator);
   }

   // ConsumerDelegate implementation -------------------------------

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void activate() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void cancelMessage(Serializable messageID) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void close() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
  public void closing() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void deactivate() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public MessageListener getMessageListener()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public Message getMessageNow() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public Message receive(long timeout) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }   

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setMessageListener(MessageListener listener)
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }  
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public boolean getNoLocal()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public Destination getDestination()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public String getMessageSelector()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   // Public --------------------------------------------------------

   public void init()
   {
      super.init();
      getMetaData().addMetaData(MetaDataConstants.JMS, MetaDataConstants.CONSUMER_ID,
                                id, PayloadKey.TRANSIENT);
   }

   public String toString()
   {
      return "ConsumerDelegate[" + Util.guidToString(id) + "]";
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
