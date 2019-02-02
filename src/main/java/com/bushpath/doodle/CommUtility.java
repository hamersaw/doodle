package com.bushpath.doodle;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.GeneratedMessageV3;

import com.bushpath.doodle.protobuf.DoodleProtos.ControlListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileDeleteResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileMkdirResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteSearchResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeCloseResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeOpenResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeWriteResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class CommUtility {
    public static GeneratedMessageV3 send(int messageType,
            GeneratedMessageV3 request,
            String ipAddress, short port) throws Exception {
        Socket socket = null;
        try {
            // send request
            socket = new Socket(ipAddress, port);

            DataOutputStream out =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeInt(messageType);
            request.writeDelimitedTo(out);

            // recv response
            int responseMessageType = in.readInt();
            if (responseMessageType == MessageType.FAILURE.getNumber()) {
                // recv failure -> throw exception
                Failure failure = Failure.parseDelimitedFrom(in);
                throw new RuntimeException(failure.getText());
            } else if (responseMessageType != messageType) {
                // recv unexpected response message type
                throw new RuntimeException("Received unexpected message type '"
                    + responseMessageType + "'");
            }

            return parseResponse(responseMessageType, in);
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    protected static GeneratedMessageV3 parseResponse(int messageType,
            DataInputStream in) throws Exception {
        switch(MessageType.forNumber(messageType)) {
            case CONTROL_LIST:
                return ControlListResponse.parseDelimitedFrom(in);
            case CONTROL_SHOW:
                return ControlShowResponse.parseDelimitedFrom(in);
            case FILE_CREATE:
                return FileCreateResponse.parseDelimitedFrom(in);
            case FILE_DELETE:
                return FileDeleteResponse.parseDelimitedFrom(in);
            case FILE_LIST:
                return FileListResponse.parseDelimitedFrom(in);
            case FILE_MKDIR:
                return FileMkdirResponse.parseDelimitedFrom(in);
            case GOSSIP:
                return GossipResponse.parseDelimitedFrom(in);
            case JOURNAL_OPERATION:
                return JournalOperationResponse.parseDelimitedFrom(in);
            case JOURNAL_WRITE:
                return JournalWriteResponse.parseDelimitedFrom(in);
            case JOURNAL_WRITE_SEARCH:
                return JournalWriteSearchResponse.parseDelimitedFrom(in);
            case NODE_LIST:
                return NodeListResponse.parseDelimitedFrom(in);
            case NODE_SHOW:
                return NodeShowResponse.parseDelimitedFrom(in);
            case PIPE_CLOSE:
                return PipeCloseResponse.parseDelimitedFrom(in);
            case PIPE_OPEN:
                return PipeOpenResponse.parseDelimitedFrom(in);
            case PIPE_WRITE:
                return PipeWriteResponse.parseDelimitedFrom(in);
            case PLUGIN_LIST:
                return PluginListResponse.parseDelimitedFrom(in);
            case SKETCH_LIST:
                return SketchListResponse.parseDelimitedFrom(in);
            case SKETCH_SHOW:
                return SketchShowResponse.parseDelimitedFrom(in);
            case SKETCH_WRITE:
                return SketchWriteResponse.parseDelimitedFrom(in);
            default:
                throw new UnsupportedOperationException("CommUtility does not"
                    + " support parsing message type '" + messageType + "'");
        }
    }
}
