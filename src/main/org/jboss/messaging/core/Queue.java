/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

/**
 * A Queue implements a Point to Point messaging domain. It sends a message to one and only one
 * receiver.

 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Queue extends AbstractDestination
{
   // Constructors --------------------------------------------------

   public Queue()
   {
      super();
   }

   // AbstractDestination implementation ----------------------------

   protected AbstractRouter createRouter()
   {
       return new PointToPointRouter();
   }

   // DEBUG ---------------------------------------------------------

   public String dump()
   {
      return "Queue: "+super.dump();
   }
}
