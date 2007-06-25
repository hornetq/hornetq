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
package org.jboss.messaging.core.contract;

import java.util.Iterator;


/**
 * A Distributor takes a message and distributes it to one or more Receivers
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * $Id$
 */
public interface Distributor extends Receiver
{
	/**
	 * Does the distributor already contain the specified Receiver?
	 * @param receiver
	 * @return
	 */
   boolean contains(Receiver receiver);

   /**
    * @return an iterator of receivers
    */
   Iterator iterator();

   /**
    * Add a local receiver to this distributor.
    *
    * @return true if the distributor did not already contain the specified receiver and the
    *         receiver was added to the distributor, false otherwise.
    */
   boolean add(Receiver receiver);

   /**
    * Remove a local receiver from this distributor.
    *
    * @return true if this distributor contained the specified receiver.
    */
   boolean remove(Receiver receiver);

   /**
    * Remove all receivers.
    */
   void clear();
   
   /**
    * 
    * @return The number of receivers in the distributor
    */
   int getNumberOfReceivers();
}
