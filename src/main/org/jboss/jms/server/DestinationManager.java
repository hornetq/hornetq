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

import java.util.Set;

import org.jboss.jms.server.destination.ManagedDestination;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface DestinationManager extends MessagingComponent
{
   /**
    * Method called by a destination service to register itself with the server peer. The server
    * peer will create and maintain state on behalf of the destination until the destination
    * unregisters itself.
    *
    * @return the name under which the destination was bound in JNDI.
    */
   void registerDestination(ManagedDestination destination) throws Exception;

   /**
    * Method called by a destination service to unregister itself from the server peer. The server
    * peer is supposed to clean up the state maintained on behalf of the unregistered destination.
    */
   void unregisterDestination(ManagedDestination destination) throws Exception;
   
   ManagedDestination getDestination(String name, boolean isQueue);
   
   Set getDestinations();   
}
