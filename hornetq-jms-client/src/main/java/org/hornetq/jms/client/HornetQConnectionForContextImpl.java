/**
 *
 */
package org.hornetq.jms.client;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Session;
import javax.jms.XAJMSContext;

import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.utils.ReferenceCounter;
import org.hornetq.utils.ReferenceCounterUtil;

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
            throw JmsExceptionUtils.convertToRuntimeException(e);
         }
      }
   };

   final ReferenceCounter refCounter = new ReferenceCounterUtil(closeRunnable);

   public JMSContext createContext(int sessionMode)
   {
      switch (sessionMode)
      {
         case Session.AUTO_ACKNOWLEDGE:
         case Session.CLIENT_ACKNOWLEDGE:
         case Session.DUPS_OK_ACKNOWLEDGE:
         case Session.SESSION_TRANSACTED:
         case HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE:
         case HornetQJMSConstants.PRE_ACKNOWLEDGE:
            break;
         default:
            throw new JMSRuntimeException("Invalid ackmode: " + sessionMode);
      }
      refCounter.increment();

      return new HornetQJMSContext(this, sessionMode);
   }

   public XAJMSContext createXAContext()
   {
      refCounter.increment();

      return new HornetQXAJMSContext(this);
   }

   @Override
   public void closeFromContext()
   {
      refCounter.decrement();
   }
}
