package com.bushpath.doodle;

import java.io.Serializable;
import java.util.List;

public abstract class Inflator {
    public abstract List<float[]> process(Serializable serializable)
        throws Exception;
}
