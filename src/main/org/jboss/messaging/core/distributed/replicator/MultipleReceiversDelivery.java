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
package org.jboss.messaging.core.distributed.replicator;

import org.jboss.messaging.core.Delivery;

import java.util.Iterator;

/**
 * TODO Get rid of that. Not used anymore.
 *
 * A message delivery. It can be "done" or active.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Added tx support
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface MultipleReceiversDelivery extends Delivery
{

   boolean cancelOnMessageRejection();

   /**
    * @return true if the acknowledgment was acted upon, false if it doesn't come from any receivers
    *         monitored by this delivery and it was ignored.
    * @throws Throwable
    */
   boolean handle(Acknowledgment ack) throws Throwable;

   /**
    * @param receiver either a direct reference or an ID of the receiver that is expected to
    *        send acknowledgments back.
    */
   void add(Object receiver);

   /**
    * @param receiver either a direct reference or an ID of the receiver that is expected to
    *        send acknowledgments back.
    */
   boolean remove(Object receiver);


   /**
    * Return an iterator of direct receiver references or receiver IDs.
    */
   Iterator iterator();

}
