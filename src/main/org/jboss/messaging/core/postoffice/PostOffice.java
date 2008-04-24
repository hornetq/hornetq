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
package org.jboss.messaging.core.postoffice;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.server.MessagingComponent;

/**
 * 
 * A PostOffice instance maintains a mapping of a String address to a Queue. Multiple Queue instances can be bound
 * with the same String address.
 * 
 * Given a message and an address a PostOffice instance will route that message to all the Queue instances that are
 * registered with that address.
 * 
 * Addresses can be any String instance.
 * 
 * A Queue instance can only be bound against a single address in the post office.
 * 
 * The PostOffice also maintains a set of "allowable addresses". These are the addresses that it is legal to
 * route to.
 * 
 * Finally, a PostOffice maintains a set of FlowControllers - one for each unique address. These are used, where
 * appropriate to control the flow of messages sent to a particular address
 *  
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface PostOffice extends MessagingComponent
{   
   boolean addDestination(String address, boolean temporary) throws Exception;
   
   boolean removeDestination(String address, boolean temporary) throws Exception;
   
   boolean containsDestination(String address);

   Binding addBinding(String address, String queueName, Filter filter,
                      boolean durable, boolean temporary) throws Exception;
   
   Binding removeBinding(String queueName) throws Exception;
   
   List<Binding> getBindingsForAddress(String address) throws Exception;
   
   Binding getBinding(String queueName) throws Exception;
      
   List<MessageReference> route(String address, Message message) throws Exception;
   
  // void routeFromCluster(String address, Message message) throws Exception;
   
   //Flow control
   
   FlowController getFlowController(String address);
     
   //For testing only
   Map<String, List<Binding>> getMappings();

   Set<String> listAllDestinations();
}
