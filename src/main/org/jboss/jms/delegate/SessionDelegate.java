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
package org.jboss.jms.delegate;

import java.io.Serializable;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.transaction.xa.XAResource;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.BytesMessageProxy;
import org.jboss.jms.message.MapMessageProxy;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.message.ObjectMessageProxy;
import org.jboss.jms.message.StreamMessageProxy;
import org.jboss.jms.message.TextMessageProxy;

/**
 * Represents the minimal set of operations to provide session functionality.
 * Some of the methods may be implemented on the server, others will be handled in the advice stack.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface SessionDelegate extends SessionEndpoint
{
   MessageProxy createMessage() throws JMSException;

   BytesMessageProxy createBytesMessage() throws JMSException;

   MapMessageProxy createMapMessage() throws JMSException;

   ObjectMessageProxy createObjectMessage() throws JMSException;

   ObjectMessageProxy createObjectMessage(Serializable object) throws JMSException;

   StreamMessageProxy createStreamMessage() throws JMSException;

   TextMessageProxy createTextMessage() throws JMSException;

   TextMessageProxy createTextMessage(String text) throws JMSException;

   void preDeliver(DeliveryInfo deliveryInfo) throws JMSException;

   void postDeliver() throws JMSException;

   MessageListener getMessageListener() throws JMSException;

   void setMessageListener(MessageListener listener) throws JMSException;

   void run();

   XAResource getXAResource();

   void addAsfMessage(MessageProxy m, int consumerID, String queueName,
                      int maxDeliveries, SessionDelegate connectionConsumerDelegate);

   boolean getTransacted();

   int getAcknowledgeMode();

   void commit() throws JMSException;

   void rollback() throws JMSException;

   void recover() throws JMSException;

   void redeliver(List deliveryInfos) throws JMSException;

   ProducerDelegate createProducerDelegate(JBossDestination destination) throws JMSException;

   void acknowledgeAll() throws JMSException;
}
