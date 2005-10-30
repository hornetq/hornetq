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
package org.jboss.jms.server;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface DestinationManager
{
   public static final String DEFAULT_QUEUE_CONTEXT = "/queue";
   public static final String DEFAULT_TOPIC_CONTEXT = "/topic";

   /**
    * Creates and binds in JNDI a queue. The queue name is unique per JMS provider instance.
    *
    * @param name - the queue name.
    * @param jndiName - the JNDI name to bind the newly created queue to. If null, the queue
    *        will be bound in JNDI under the default context using the specified queue name.
    *
    * @throws Exception
    */
   public void createQueue(String name, String jndiName) throws Exception;

   /**
    * Removes the queue both from JNDI and DestinationManager.
    */
   public void destroyQueue(String name) throws Exception;

   /**
    * Creates and binds in JNDI a topic. The topic name is unique per JMS provider instance.
    *
    * @param name - the topic name.
    * @param jndiName - the JNDI name to bind the newly created topic to. If null, the topic
    *        will be bound in JNDI under the default context using the specified topic name.
    *
    * @throws Exception
    */
   public void createTopic(String name, String jndiName) throws Exception;

   /**
    * Removes the topic both from JNDI and DestinationManager.
    */
   public void destroyTopic(String name) throws Exception;


}
