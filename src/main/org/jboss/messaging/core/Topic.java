/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

/**
 * A Topic implements a Publishes/Subscriber messaging domain. It sends a message to all receivers
 * connected at the time the message is sent.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Topic extends AbstractDestination
{
   // Constructors --------------------------------------------------

   public Topic()
   {
       super();

       // set the input pipe to be synchronous
       inputPipe.setSynchronous(true);
   }

   // AbstractDestination implementation ----------------------------

   protected AbstractRouter createRouter()
   {
       return new PointToMultipointRouter();
   }

   // DEBUG ---------------------------------------------------------

   public String dump()
   {
      return "Topic: "+super.dump();
   }
}
