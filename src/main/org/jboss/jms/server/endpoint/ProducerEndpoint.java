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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.client.Closeable;

/**
 * Represents the set of methods from the ProducerDelegate that are handled on the server.
 * The rest of the methods are handled in the advice stack.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ProducerEndpoint extends Closeable
{
   /**
    * Sends a message to the JMS provider.
    *
    * @param destination - the destination to send the message to. If null, the message will be sent
    *        to the producer's default destination.
    * @param message - the message to be sent.
    * @param deliveryMode - the delivery mode to use when sending this message. Must be one of
    *        DeliveryMode.PERSISTENT or DeliveryMode.NON_PERSISTENT. If -1, the message will be sent
    *        using the producer's default delivery mode.
    * @param priority - the priority to use when sending this message. A valid priority must be in
    *        the 0-9 range. If -1, the message will be sent using the producer's default priority.
    * @param timeToLive - the time to live to use when sending this message (in ms). Long.MIN_VALUE
    *        means the message will be sent using the producer's default timeToLive. 0 means live
    *        forever. For any other negative value, the message will be already expired when it is
    *        sent.
    *
    * @throws JMSException
    */
   void send(Destination destination,
             Message message,
             int deliveryMode,
             int priority,
             long timeToLive) throws JMSException;
}

