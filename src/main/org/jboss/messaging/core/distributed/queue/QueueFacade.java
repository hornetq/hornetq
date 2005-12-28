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
package org.jboss.messaging.core.distributed.queue;

import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.distributed.PeerFacade;
import org.jboss.messaging.core.distributed.PeerIdentity;

import java.util.List;

/**
 * Exposes methods to be invoked remotely by queue peers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface QueueFacade extends PeerFacade
{
   List remoteBrowse(PeerIdentity originator, Filter filter);

   /**
    * TODO: experimental
    *
    * The originator requests the first undelivered message maintained by this queue peer to be
    * forwared to it. This queue peer should return true if it has undelivered messages and it
    * asynchronously initiated the forwarding process, or false otherwise.
    */
   boolean forward(PeerIdentity originator);

}
