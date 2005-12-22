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

import org.jboss.messaging.core.tx.Transaction;

/**
 * A component that handles routable instances. Handling means consumption or
 * synchronous/asynchronous forwarding to another receiver(s).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Receiver
{

   /**
    * A receiver can return an active, "done" or null delivery. The method returns null in case
    * the receiver doesn't accept the message. The return value is <i>unspecified</i> when the
    * message is submitted in the context of a transaction (tx not null).
    *
    * @param observer - the component the delivery should be acknowledged to.
    * 
    * @see org.jboss.messaging.core.Delivery
    * @see org.jboss.messaging.core.DeliveryObserver
    */
   Delivery handle(DeliveryObserver observer, Routable routable, Transaction tx);
     
}
