package com.bushpath.doodle;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

public abstract class Inflator {
    public abstract List<float[]> process(ObjectInputStream in)
        throws Exception;
    public abstract void process(Serializable serializable,
        List<float[]> observations) throws Exception;
}
