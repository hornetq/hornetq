package org.hornetq.jms.client;

import org.hornetq.utils.ConcurrentHashSet;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.util.Set;

/**
 * Restricts what can be called on context passed in wrapped CompletionListener.
 */
public class ThreadAwareContext
{
   /**
    * Necessary in order to assert some methods ({@link javax.jms.JMSContext#stop()}
    * {@link javax.jms.JMSContext#close()} etc) are not getting called from within a
    * {@link javax.jms.CompletionListener}.
    * @see ThreadAwareContext#assertNotMessageListenerThread()
    */
   private Thread completionListenerThread;

   /**
    * Use a set because JMSContext can create more than one JMSConsumer
    * to receive asynchronously from different destinations.
    */
   private Set<Long> messageListenerThreads = new ConcurrentHashSet<Long>();

   /**
    * Sets current thread to the context
    * <p>
    * Meant to inform an JMSContext which is the thread that CANNOT call some of its methods.
    * </p>
    * @param isCompletionListener : indicating whether current thread is from CompletionListener
    * or from MessageListener.
    */
   public void setCurrentThread(boolean isCompletionListener)
   {
      if (isCompletionListener)
      {
         completionListenerThread = Thread.currentThread();
      }
      else
      {
         messageListenerThreads.add(Thread.currentThread().getId());
      }
   }


   /**
    * Clear current thread from the context
    *
    * @param isCompletionListener : indicating whether current thread is from CompletionListener
    * or from MessageListener.
    */
   public void clearCurrentThread(boolean isCompletionListener)
   {
      if (isCompletionListener)
      {
         completionListenerThread = null;
      }
      else
      {
         messageListenerThreads.remove(Thread.currentThread().getId());
      }
   }

   /**
    * Asserts a {@link javax.jms.CompletionListener} is not calling from its own {@link javax.jms.JMSContext}.
    * <p>
    * Note that the code must work without any need for further synchronization, as there is the
    * requirement that only one CompletionListener be called at a time. In other words,
    * CompletionListener calling is single-threaded.
    * @see javax.jms.JMSContext#close()
    * @see javax.jms.JMSContext#stop()
    * @see javax.jms.JMSContext#commit()
    * @see javax.jms.JMSContext#rollback()
    */
   public void assertNotCompletionListenerThreadRuntime()
   {
      if (completionListenerThread == Thread.currentThread())
      {
         throw HornetQJMSClientBundle.BUNDLE.callingMethodFromCompletionListenerRuntime();
      }
   }

   /**
    * Asserts a {@link javax.jms.CompletionListener} is not calling from its own {@Link javax.jms.Connection} or from
    * a {@Link javax.jms.MessageProducer} .
    * <p>
    * Note that the code must work without any need for further synchronization, as there is the
    * requirement that only one CompletionListener be called at a time. In other words,
    * CompletionListener calling is single-threaded.
    *
    * @see javax.jms.Connection#close()
    * @see javax.jms.MessageProducer#close()
    */
   public void assertNotCompletionListenerThread() throws javax.jms.IllegalStateException
   {
      if (completionListenerThread == Thread.currentThread())
      {
         throw HornetQJMSClientBundle.BUNDLE.callingMethodFromCompletionListener();
      }
   }

   /**
    * Asserts a {@link javax.jms.MessageListener} is not calling from its own {@link javax.jms.JMSContext}.
    * <p>
    * Note that the code must work without any need for further synchronization, as there is the
    * requirement that only one MessageListener be called at a time. In other words,
    * MessageListener calling is single-threaded.
    * @see javax.jms.JMSContext#close()
    * @see javax.jms.JMSContext#stop()
    */
   public void assertNotMessageListenerThreadRuntime()
   {
      if (messageListenerThreads.contains(Thread.currentThread().getId()))
      {
         throw HornetQJMSClientBundle.BUNDLE.callingMethodFromListenerRuntime();
      }
   }

   /**
    * Asserts a {@link javax.jms.MessageListener} is not calling from its own {@link javax.jms.Connection} or
    *  {@Link javax.jms.MessageConsumer}.
    * <p>
    * Note that the code must work without any need for further synchronization, as there is the
    * requirement that only one MessageListener be called at a time. In other words,
    * MessageListener calling is single-threaded.
    *
    * @see javax.jms.Connection#close()
    * @see javax.jms.MessageConsumer#close()
    */
   public void assertNotMessageListenerThread() throws IllegalStateException
   {
      if (messageListenerThreads.contains(Thread.currentThread().getId()))
      {
         throw HornetQJMSClientBundle.BUNDLE.callingMethodFromListener();
      }
   }

}
