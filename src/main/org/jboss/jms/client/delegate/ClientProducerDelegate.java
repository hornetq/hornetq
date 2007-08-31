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

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;

/**
 * The client-side Producer delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientProducerDelegate extends DelegateSupport implements ProducerDelegate
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = -6976930316308905681L;

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // DelegateSupport overrides --------------------------------------------------------------------

   public void synchronizeWith(DelegateSupport nd) throws Exception
   {
      super.synchronizeWith(nd);

      ClientProducerDelegate newDelegate = (ClientProducerDelegate)nd;

      // synchronize server endpoint state

      // synchronize (recursively) the client-side state

      state.synchronizeWith(newDelegate.getState());
   }

   // ProducerDelegate implementation --------------------------------------------------------------

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
   public long closing() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public int getDeliveryMode() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public JBossDestination getDestination() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public boolean getDisableMessageID() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public int getPriority() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public long getTimeToLive() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void send(JBossDestination destination, Message message, int deliveryMode,
                    int priority, long timeToLive) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }


   public void send(JBossDestination destination, Message message, int deliveryMode, int priority, long timeToLive, boolean keepOriginalID) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void send(JBossMessage message) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");  
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setDestination(JBossDestination dest)
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setDisableMessageID(boolean value) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setPriority(int defaultPriority) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ProducerDelegate[" + System.identityHashCode(this) + ", ID=" + id + "]";
   }

   // Protected ------------------------------------------------------------------------------------
   
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner Classes --------------------------------------------------------------------------------

}
