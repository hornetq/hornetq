/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnectionFactory;

/**
 * An aggregate interface for the JMS connection factories
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.com">Jesper Pedersen</a>
 * @version $Revision: $
 */
public interface JBMConnectionFactory extends ConnectionFactory, 
                                              TopicConnectionFactory,
                                              QueueConnectionFactory, 
                                              XAConnectionFactory,
                                              XAQueueConnectionFactory,
                                              XATopicConnectionFactory,
                                              Serializable
{
   /** Connection factory capable of handling connections */
   public static final int CONNECTION = 0;

   /** Connection factory capable of handling queues */
   public static final int QUEUE_CONNECTION = 1;

   /** Connection factory capable of handling topics */
   public static final int TOPIC_CONNECTION = 2;

   /** Connection factory capable of handling XA connections */
   public static final int XA_CONNECTION = 3;

   /** Connection factory capable of handling XA queues */
   public static final int XA_QUEUE_CONNECTION = 4;

   /** Connection factory capable of handling XA topics */
   public static final int XA_TOPIC_CONNECTION = 5;
}
