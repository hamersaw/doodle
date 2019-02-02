package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.Variable;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (!this.frozen.get()) {
            this.init();
            this.frozen.set(true);
        }
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

                for (String value
                        : this.variables.get(type).get(name)) {
                    builder.addValues(value);
                }

                variables.add(builder.build());
            }
        }

        return variables;
    }

    public void processVariable(Variable variable,
            OperationType operationType) {
        // process operation
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

        switch(operationType) {
            case ADD:
                valueSet.addAll(variable.getValuesList());
                log.info("'{}': added {} value(s) to variable '{}:{}'",
                    this.id, variable.getValuesCount(),
                    variable.getType(), variable.getName());

                break;
            case DELETE:
                valueSet.removeAll(variable.getValuesList());
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
    }

    public abstract void init();
}
