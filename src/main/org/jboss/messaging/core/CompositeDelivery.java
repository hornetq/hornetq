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

import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a> $Id$
 * @version <tt>$Revision$</tt>
 */
class CompositeDelivery extends SimpleDelivery
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Set deliveries;

   // Constructors --------------------------------------------------

   public CompositeDelivery(DeliveryObserver observer, Set deliveries)
   {
      for(Iterator i = deliveries.iterator(); i.hasNext(); )
      {
         Delivery d = (Delivery)i.next();
         d.setObserver(observer);
      }
      this.deliveries = new HashSet(deliveries);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
