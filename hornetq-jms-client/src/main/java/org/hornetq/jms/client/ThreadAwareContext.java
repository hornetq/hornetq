package org.hornetq.jms.client;

/**
 * Restricts what can be called on context passed in wrapped CompletionListener.
 */
interface ThreadAwareContext
{

   /**
    * Sets a thread.
    * <p>
    * Meant to inform an JMSContext which is the thread that CANNOT call some of its methods.
    * @param thread
    */
   public void setCurrentThread(Thread thread);

}
