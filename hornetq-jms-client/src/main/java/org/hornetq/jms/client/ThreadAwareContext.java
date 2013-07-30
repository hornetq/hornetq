package org.hornetq.jms.client;

/**
 * Restricts what can be called on context passed in wrapped CompletionListener.
 */
interface ThreadAwareContext
{

   /**
    * Sets current thread to the context
    * <p>
    * Meant to inform an JMSContext which is the thread that CANNOT call some of its methods.
    * </p>
    * @param isCompletionListener : indicating whether current thread is from CompletionListener
    * or from MessageListener.
    */
   void setCurrentThread(boolean isCompletionListener);

   /**
    * Clear current thread from the context
    * 
    * @param isCompletionListener : indicating whether current thread is from CompletionListener
    * or from MessageListener.
    */
   void clearCurrentThread(boolean isCompletionListener);

}
