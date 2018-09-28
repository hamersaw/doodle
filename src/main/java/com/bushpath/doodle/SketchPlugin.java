package com.bushpath.doodle;

import java.util.List;

public abstract class SketchPlugin extends Plugin {
    public abstract Transform getTransform(List<String> fields);
}
