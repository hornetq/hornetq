/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.client.plugin;

import java.util.Random;

import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RoundRobinLoadBalancingPolicy implements LoadBalancingPolicy
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 5215940403016586462L;
   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private static final Random random = new Random();

   // The index of the next connectionFactory to be used
   private int next;

   private ClientConnectionFactoryImpl[] delegates;

   // Constructors ---------------------------------------------------------------------------------

   public RoundRobinLoadBalancingPolicy(ClientConnectionFactoryImpl[] delegates)
   {
      next = -1;
      this.delegates = delegates;
   }

   // LoadBalancingPolicy implementation -----------------------------------------------------------

   public synchronized ClientConnectionFactoryImpl getNext()
   {
      if (next >= delegates.length)
      {
         next = 0;
      }
      
      if (next < 0)
      {
         next = random.nextInt(delegates.length);
      }

      return delegates[next++];
   }

   public synchronized void updateView(ClientConnectionFactoryImpl[] delegates)
   {
      next = -1;
      this.delegates = delegates;
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
