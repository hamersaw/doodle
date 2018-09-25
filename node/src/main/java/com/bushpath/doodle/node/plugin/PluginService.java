package com.bushpath.doodle.node.plugin;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginShowResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class PluginService implements Service {
    protected PluginManager pluginManager;

    public PluginService(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.PLUGIN_LIST.getNumber(),
                MessageType.PLUGIN_SHOW.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        switch (MessageType.forNumber(messageType)) {
            case PLUGIN_LIST:
                // parse request
                PluginListRequest pluginListRequest =
                    PluginListRequest.parseDelimitedFrom(in);

                // init response
                PluginListResponse.Builder pluginListBuilder =
                    PluginListResponse.newBuilder();

                // TODO - populate builder

                // write to out
                out.writeInt(messageType);
                pluginListBuilder.build().writeDelimitedTo(out);
                break;
            case PLUGIN_SHOW:
                // parse request
                PluginShowRequest pluginShowRequest =
                    PluginShowRequest.parseDelimitedFrom(in);

                // init response
                PluginShowResponse.Builder pluginShowBuilder =
                    PluginShowResponse.newBuilder();

                // TODO - populate builder

                // write to out
                out.writeInt(messageType);
                pluginShowBuilder.build().writeDelimitedTo(out);
                break;
            default:
                // unreachable code
        }
    }
}
