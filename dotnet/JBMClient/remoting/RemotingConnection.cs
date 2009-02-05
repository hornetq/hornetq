/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

namespace JBoss.JBM.Client.remoting
{
    using JBoss.JBM.Client.remoting.spi;

    using System.Collections.Generic;

    using JBoss.JBM.Client.exception;

    /**
     * A RemotingConnection
     * 
     * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
     * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
     */
    public interface RemotingConnection : BufferHandler
    {
        object getID();

        string RemoteAddress
        {
            get;
        }

        Channel GetChannel(long channelID, int windowSize, bool block);

        long GenerateChannelID();

        void AddFailureListener(FailureListener listener);

        bool RemoveFailureListener(FailureListener listener);

        List<FailureListener> GetFailureListeners();

        void GetFailureListeners(List<FailureListener> listeners);

        MessagingBuffer CreateBuffer(int size);

        void Fail(MessagingException me);

        void Destroy();

        bool IsExpired(long now);

        void StartPinger();

        void SyncIDGeneratorSequence(long id);

        long GetIDGeneratorSequence();

        void Activate();

        void Freeze();

        RemotingConnection GetReplicatingConnection();

        void SetReplicatingConnection(RemotingConnection connection);
    }
}