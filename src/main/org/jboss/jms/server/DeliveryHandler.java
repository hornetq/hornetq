/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.util.Collection;

/**
 * Handles the actual distribution of of messages to consumers. In short, this
 * is what defines the behavior of a given {@link Destination} as either a
 * queue, a topic, neither, or some odd combination of the two.  This means that
 * there is no domain specific behavior in any other server class (with the
 * exception of a {@link DurableConsumer} which is unavoidable) and provides a
 * very simple interface that one can implement to change the way a
 * <code>Destination</code> delivers messages.<br/>
 * </br>
 * For example, perhaps one wants to have the behavior of a queue, but instead
 * of distributing a message in a round robin fashion, you want to weight
 * specific consumers more heavly than others.  A custom implementation of this,
 * coupled with the metadata provided by the {@link ConsumerMetaData} affiliated
 * with the {@link Consumer} provides one the tools necessary to implement this
 * solution quickly, easly, and with minimal code.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public interface DeliveryHandler
{
    public void setDestination(Destination destination);

    public void deliver(
            MessageReference messageReference,
            ConsumerGroup consumers);

    public void deliver(Collection messages, ConsumerGroup consumers);
}