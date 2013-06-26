/**
 *
 */
package org.hornetq.jms.client;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Session;

import org.hornetq.utils.ReferenceCounterUtil;
import org.hornetq.utils.ReferenceCounter;

public abstract class HornetQConnectionForContextImpl implements HornetQConnectionForContext
{

   final Runnable closeRunnable = new Runnable()
   {
      public void run()
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
   };

   final ReferenceCounter refCounter = new ReferenceCounterUtil(closeRunnable);

   public JMSContext createContext(int sessionMode)
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

      refCounter.increment();

      return new HornetQJMSContext(this, session, sessionMode);
   }

   @Override
   public void closeFromContext()
   {
      refCounter.decrement();
   }
}
