/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.messaging.core.local.AbstractRouter;

import java.io.Serializable;

/**
 * A LocalQueue implements a Point to Point messaging domain. It sends a message to one and only one
 * receiver. All receivers are in the same address space. By default a queue is configured as an
 * asynchronous Channel.
 *
 * @see org.jboss.messaging.core.Channel

 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalQueue extends AbstractDestination
{
   // Constructors --------------------------------------------------

   public LocalQueue(Serializable id)
   {
      super(id);
   }

   // AbstractDestination implementation ----------------------------

   protected AbstractRouter createRouter()
   {
       return new PointToPointRouter("P2PRouter");
   }

}
