package org.jboss.messaging.newcore.intf;

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
