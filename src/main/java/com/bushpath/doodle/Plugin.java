package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.Variable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

public abstract class Plugin {
    protected static final Logger log =
        LoggerFactory.getLogger(Plugin.class);

    protected String id;
    protected AtomicBoolean frozen;
    protected Map<String, Map<String, Set<String>>> variables;

    public Plugin(String id) {
        this.id = id;
        this.frozen = new AtomicBoolean(false);
        this.variables = new TreeMap();
    }

    public Plugin(DataInputStream in) throws IOException {
        // read id
        int idLength = in.readInt();
        byte[] idBytes = new byte[idLength];
        in.readFully(idBytes);
        this.id = new String(idBytes);

        // read frozen
        this.frozen = new AtomicBoolean(in.readBoolean());

        // initialize variables
        this.variables = new TreeMap();
    }

    public void checkFrozen() throws Exception {
        if (this.frozen.get()) {
            throw new RuntimeException("Unable to support operation"
                + ", plugin '" + this.id + "' is frozen");
        }
    }

    public String getId() {
        return this.id;
    }

    public void freeze() {
        this.frozen.set(true);
    }

    public boolean frozen() {
        return this.frozen.get();
    }

    public List<Variable> getVariables() {
        List<Variable> variables = new ArrayList();
        for (String type : this.variables.keySet()) {
            for (String name : this.variables.get(type).keySet()) {
                Variable.Builder builder = Variable.newBuilder()
                    .setType(type)
                    .setName(name);

                for (String value : this.variables.get(type).get(name)) {
                    builder.addValues(value);
                }

                variables.add(builder.build());
            }
        }

        return variables;
    }

    /*public void handleVariableOperation(VariableOperation variableOperation) {
        // check if operation has already been processed
        if (this.operations.containsKey(variableOperation.getTimestamp())) {
            return;
        }

        // process operation
        Variable variable = variableOperation.getVariable();
        Map<String, Set<String>> nameMap = null;
        if (this.variables.containsKey(variable.getType())) {
            nameMap = this.variables.get(variable.getType());
        } else {
            nameMap = new TreeMap();
            this.variables.put(variable.getType(), nameMap);
        }

        Set<String> valueSet = null;
        if (nameMap.containsKey(variable.getName())) {
            valueSet = nameMap.get(variable.getName());
        } else {
            valueSet = new TreeSet();
            nameMap.put(variable.getName(), valueSet);
        }


        switch(variableOperation.getOperation()) {
            case ADD:
                for (String value : variable.getValuesList()) {
                    if (!valueSet.contains(value)) {
                        this.addVariable(variable.getType(),
                            variable.getName(), value);
                    }

                    valueSet.add(value);
                }

                log.info("'{}': added {} value(s) to variable '{}:{}'",
                    this.id, variable.getValuesCount(),
                    variable.getType(), variable.getName());

                break;
            case DELETE:
                for (String value : variable.getValuesList()) {
                    if (valueSet.contains(value)) {
                        this.deleteVariable(variable.getType(),
                            variable.getName(), value);
                    }

                    valueSet.remove(value);
                }

                log.info("'{}': deleted {} value(s) from variable '{}:{}'",
                    this.id, variable.getValuesCount(),
                    variable.getType(), variable.getName());

                break;
        }

        // cleanup structure
        if (valueSet.isEmpty()) {
            nameMap.remove(variable.getName());
        }

        if (nameMap.isEmpty()) {
            this.variables.remove(variable.getType());
        }

        // add operation to processed list
        this.operations.put(variableOperation.getTimestamp(), variableOperation);
    }*/

    /*public void replayVariableOperations() {
        Map<Long, VariableOperation> operations = this.operations;
        this.operations = new TreeMap();

        for (VariableOperation operation : operations.values()) {
            this.handleVariableOperation(operation);
        }
    }*/

    public void serializePlugin(DataOutputStream out)
            throws IOException {
        // write this.id
        out.writeInt(this.id.length());
        out.write(this.id.getBytes());

        // write frozen
        out.writeBoolean(this.frozen.get());
    }

    public abstract void addVariable(String type, String name, String value);
    public abstract void deleteVariable(String type, String name, String value);
    public abstract void serialize(DataOutputStream out)
        throws IOException;
}
