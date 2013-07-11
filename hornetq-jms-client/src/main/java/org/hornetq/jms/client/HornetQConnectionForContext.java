/**
 *
 */
package org.hornetq.jms.client;

import javax.jms.JMSContext;

/**
 * Interface created to support reference counting all contexts using it.
 * <p>
 * Necessary to support {@code JMSContext.close()} conditions.
 * @see JMSContext
 */
public interface HornetQConnectionForContext extends javax.jms.Connection
{
   JMSContext createContext(int sessionMode);

   void closeFromContext();
}
