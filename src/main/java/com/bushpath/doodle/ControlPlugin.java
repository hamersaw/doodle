package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.CRC32;

public abstract class ControlPlugin {
    protected Map<Long, VariableOperation> operations;
    protected Map<String, Map<String, Set<String>>> variables;

    public ControlPlugin() {
        this.operations = new TreeMap();
        this.variables = new TreeMap();
    }

    public void handleVariableOperation(VariableOperation variableOperation) {
        // check if operation has already been processed
        if (this.operations.containsKey(variableOperation.getTimestamp())) {
            return;
        }

        // process operation
        PluginVariable variable = variableOperation.getVariable();
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
                break;
            case DELETE:
                for (String value : variable.getValuesList()) {
                    if (valueSet.contains(value)) {
                        this.deleteVariable(variable.getType(),
                            variable.getName(), value);
                    }

                    valueSet.remove(value);
                }
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
    }

    public abstract void addVariable(String type, String name, String value);
    public abstract void deleteVariable(String type, String name, String value);

    public List<PluginVariable> getVariables() {
        List<PluginVariable> pluginVariables = new ArrayList();
        for (String type : this.variables.keySet()) {
            for (String name : this.variables.get(type).keySet()) {
                PluginVariable.Builder builder = PluginVariable.newBuilder()
                    .setType(type)
                    .setName(name);

                for (String value : this.variables.get(type).get(name)) {
                    builder.addValues(value);
                }

                pluginVariables.add(builder.build());
            }
        }

        return pluginVariables;
    }

    @Override
    public int hashCode() {
        CRC32 crc32 = new CRC32();
        for (String type : this.variables.keySet()) {
            crc32.update(type.getBytes());
            for (String name : this.variables.get(type).keySet()) {
                crc32.update(name.getBytes());
                for (String value : this.variables.get(type).get(name)) {
                    crc32.update(value.getBytes());
                }
            }
        }

        return (int) crc32.getValue();
    }
}
