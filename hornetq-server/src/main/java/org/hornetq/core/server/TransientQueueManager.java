package org.hornetq.core.server;

import org.hornetq.api.core.SimpleString;
import org.hornetq.utils.ReferenceCounter;

/**
 * @author Clebert Suconic
 */

public interface TransientQueueManager extends ReferenceCounter
{
   SimpleString getQueueName();
}
