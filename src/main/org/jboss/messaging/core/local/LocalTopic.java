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
 * A LocalTopic implements a Publishes/Subscriber messaging domain. It sends a message to all
 * receivers connected at the time the message is sent. All receivers are in the same address space.
 * By default a topic is configured as an asynchronous Channel.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalTopic extends AbstractDestination
{
   // Constructors --------------------------------------------------

   public LocalTopic(Serializable id)
   {
       super(id);

       // set the input pipe to be synchronous
       inputPipe.setSynchronous(true);
   }

   // AbstractDestination implementation ----------------------------

   protected AbstractRouter createRouter()
   {
       return new PointToMultipointRouter("P2MPRouter");
   }

}
