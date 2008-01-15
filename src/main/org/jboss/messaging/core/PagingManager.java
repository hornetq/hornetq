package org.jboss.messaging.core;

/**
 * 
 * A PagingManager
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface PagingManager extends MessagingComponent
{
   void pageReference(Queue queue, MessageReference ref);
   
   MessageReference depageReference(Queue queue);
}
