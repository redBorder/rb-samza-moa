package Exceptions;

import weka.core.DenseInstance;
import weka.core.Instance;

/**
 * Created by Chorro on 06/08/15.
 */
public class RBInstance extends DenseInstance {

    private String event = "<Null>";

    public RBInstance(int numAtt){
        super(numAtt);
    }

    public RBInstance(Instance instance) {
        super(instance);
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }
}
