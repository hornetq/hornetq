/**
 *
 */
package org.hornetq.jms.client;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Session;

public abstract class HornetQConnectionForContextImpl implements HornetQConnectionForContext
{

   private final Object jmsContextLifeCycleGuard = new Object();
   private int contextReferenceCount;

   public JMSContext createContext(int sessionMode)
   {
      synchronized (jmsContextLifeCycleGuard)
      {
         Session session;
         try
         {
            session = createSession(sessionMode);
         }
         catch (JMSException e)
         {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
         }
         contextReferenceCount++;
         return new HornetQJMSContext(this, session, sessionMode);
      }
   }

   @Override
   public void closeFromContext()
   {
      synchronized (jmsContextLifeCycleGuard)
      {
         contextReferenceCount--;
         if (contextReferenceCount == 0)
         {
            try
            {
               close();
            }
            catch (JMSException e)
            {
               throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
         }
      }
   }
}
