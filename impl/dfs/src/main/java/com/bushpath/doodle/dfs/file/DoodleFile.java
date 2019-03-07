package com.bushpath.doodle.dfs.file;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import com.bushpath.doodle.dfs.format.Format;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DoodleFile extends DoodleEntry {
    protected Format format;
    protected Query query;
    protected ByteString queryByteString;
    protected boolean initialized;
    //protected int featureCount;
    //protected Map<Integer, Integer> observations;

    public DoodleFile(String name, Format format, Query query,
            ByteString queryByteString, int featureCount) {
        super(name);
        this.format = format;
        this.query = query;
        this.queryByteString = queryByteString;
        this.initialized = false;
        //this.featureCount = featureCount;
        //this.observations = new HashMap();
    }

    public Query getQuery() {
        return query;
    }

    /*public void addObservations(int nodeId, int observationCount) {
        this.observations.put(nodeId, observationCount);
    }

    public int getFeatureCount() {
        return this.featureCount;
    }*/

    public Format getFormat() {
        return this.format;
    }

    public boolean getInitialized() {
        return this.initialized;
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    /*public Set<Map.Entry<Integer, Integer>> getObservationEntrySet() {
        return this.observations.entrySet();
    }*/

    @Override
    public void buildProtobuf(File.Builder builder) {
        /*builder.setQuery(this.queryByteString);
        builder.setFileFormat(this.format.getFileFormat());

        for (Map.Entry<Integer, Integer> entry :
                this.observations.entrySet()) {
            builder.putObservations(entry.getKey(), entry.getValue());
        }*/
    }

    @Override
    public FileType getFileType() {
        return FileType.REGULAR;
    }

    @Override
    public long getSize() {
        /*int totalObservationCount = 0;
        for (Integer observationCount : this.observations.values()) {
            totalObservationCount += observationCount;
        }

        return this.format
            .length(this.featureCount, totalObservationCount);*/
        return -1;
    }

    @Override
    public void update(File file) throws Exception {
        /*for (Map.Entry<Integer, Integer> entry :
                file.getObservationsMap().entrySet()) {
            this.observations.put(entry.getKey(), entry.getValue());
        }*/
    }
}
