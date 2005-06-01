/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.util.Set;

/**
 * Represents a routable's state. It could be a ChannelNACK or it could contain a combination of
 * Acknowledgments and NonCommitted.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface State
{
   /**
    * Returns true if this state represents a Channel NACK: the message was not delivered to any
    * receiver yet. Returns false if at least one receiver saw the message, and returned either a
    * positive or a negative acknowledgment.
    */
   public boolean isChannelNACK();

   /**
    * Returns the number of NACKs maintained by the current state. Does not count a Channel NACK.
    */
   public int nackCount();

   /**
    * @return a Set of Acknowledgments or an empty set if there are no NACKs.
    */
   public Set getNACK();

   /**
    * Returns the number of ACKs maintained by the current state.
    */
   public int ackCount();

   /**
    * @return a Set of Acknowledgments or an empty set if there are no ACKs.
    */
   public Set getACK();

   /**
    * Returns the total number of stored acknowlegments (positive or negative). Does not include
    * a Channel NACK.
    */
   public int size();

   /**
    * Returns the number of transactions this routable was not yet committed for.
    */
   public int nonCommittedCount();

   /**
    * @return a Set of NonCommitted instances of an empty set if the state does not have any
    *         NonCommited instances.
    */
   public Set getNonCommitted();
}
