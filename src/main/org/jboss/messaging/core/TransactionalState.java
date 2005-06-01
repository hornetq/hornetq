/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;


/**
 * A state component representing the transactional state of a message.
 *  
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class TransactionalState implements StateComponent
{

   public boolean isAcknowledgment()
   {
      return false;
   }

   public abstract String getTxID();
}
