package org.jboss.messaging.core.paging;

import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingComponent;
import org.jboss.messaging.core.server.Queue;

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
