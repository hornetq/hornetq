/*
 * JBoss, Home of Professional Open Source Copyright 2005, JBoss Inc., and individual contributors as indicated by the
 * @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is free
 * software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
namespace JBoss.JBM.Client.remoting
{


    /**
     * A Channel A Channel *does not* support concurrent access by more than one thread!
     * 
     * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
     */
    public interface Channel
    {
        long ID
        {
            get;
        }

        object Lock
        {
            get;
        }

        ChannelHandler Handler
        {
            set;
        }


        Channel ReplicatingChannel
        {
            get;
        }

        int LastReceivedCommandID
        {
            get;
        }


        void Send(Packet packet);

        Packet SendBlocking(Packet packet);

        // This is only used on server.
        // Verify if there are possible future use for this
        //DelayedResult PeplicatePacket(Packet packet);

        //void ReplicateComplete();

        void Close();

        void TransferConnection(RemotingConnection newConnection);

        void ReplayCommands(int lastReceivedCommandID);

        void DoLock();

        void DoUnlock();

        void ReturnBlocking();

        RemotingConnection getConnection();

        void ReplicatingChannelDead();

        void Confirm(Packet packet);
    }
}