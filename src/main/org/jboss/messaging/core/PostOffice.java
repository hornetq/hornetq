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
package org.jboss.messaging.core;

import java.util.List;
import java.util.Map;

/**
 * 
 * PostOffice maintains bindings of conditions to Queue instances
 * and knows how to route messages to queues based on a Condition.
 * 
 * Conditions can also be added without queues.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface PostOffice extends MessagingComponent
{
   Queue addQueue(Condition condition, String name, Filter filter,
                  boolean durable, boolean temporary, boolean allNodes) throws Exception;
   
   void addCondition(Condition condition);
   
   boolean removeCondition(Condition condition);
   
   boolean containsCondition(Condition condition);

   boolean removeQueue(Condition condition, String name, boolean allNodes) throws Exception;
   
   void route(Condition condition, Message message) throws Exception;
   
   void routeFromCluster(Condition condition, Message message) throws Exception;
   
   List<Binding> getBindingsForQueueName(String name) throws Exception;
   
   List<Binding> getBindingsForCondition(Condition condition) throws Exception;
   
   //For testing
   Map<Condition, List<Binding>> getMappings();
}
