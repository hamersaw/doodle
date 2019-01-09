package com.bushpath.doodle.node.filesystem;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DoodleFile extends DoodleEntry {
    protected Map<Integer, Integer> observations;

    public DoodleFile(String name) {
        super(name);
        this.observations = new HashMap();
    }

    public Set<Map.Entry<Integer, Integer>> getObservationEntrySet() {
        return this.observations.entrySet();
    }

    @Override
    public FileType getFileType() {
        return FileType.REGULAR;
    }

    @Override
    public void update(File file) throws Exception {
        for (Map.Entry<Integer, Integer> entry :
                file.getObservationsMap().entrySet()) {
            this.observations.put(entry.getKey(), entry.getValue());
        }
    }
}
