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
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;

/**
 * Represents the minimal set of operations to provide session functionality.
 * Some of the methods may be implemented on the server, others will be handled in the advice stack.
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface SessionDelegate extends SessionEndpoint
{
   JBossMessage createMessage() throws JMSException;

   JBossBytesMessage createBytesMessage() throws JMSException;

   JBossMapMessage createMapMessage() throws JMSException;

   JBossObjectMessage createObjectMessage() throws JMSException;

   JBossObjectMessage createObjectMessage(Serializable object) throws JMSException;

   JBossStreamMessage createStreamMessage() throws JMSException;

   JBossTextMessage createTextMessage() throws JMSException;

   JBossTextMessage createTextMessage(String text) throws JMSException;

   void preDeliver(DeliveryInfo deliveryInfo) throws JMSException;

   boolean postDeliver() throws JMSException;

   MessageListener getMessageListener() throws JMSException;

   void setMessageListener(MessageListener listener) throws JMSException;

   void run();

   XAResource getXAResource();

   void addAsfMessage(JBossMessage m, String consumerID, String queueName,
                      int maxDeliveries, SessionDelegate connectionConsumerDelegate,
                      boolean shouldAck);

   boolean getTransacted();

   int getAcknowledgeMode();

   void commit() throws JMSException;

   void rollback() throws JMSException;

   void recover() throws JMSException;

   void redeliver(List deliveryInfos) throws JMSException;

   ProducerDelegate createProducerDelegate(JBossDestination destination) throws JMSException;

   void acknowledgeAll() throws JMSException;
}
