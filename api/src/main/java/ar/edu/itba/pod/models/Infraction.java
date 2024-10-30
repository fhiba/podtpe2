package ar.edu.itba.pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;
import java.io.Serializable;

public class Infraction implements DataSerializable, Serializable {
    private String code;
    private String definition;

    public Infraction(String code, String definition) {
        this.code = code;
        this.definition = definition;
    }
    public Infraction() {
        // Constructor sin argumentos necesario para la deserialización
    }
    public String getCode() {
        return code;
    }

    public String getDefinition() {
        return definition;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(code);
        out.writeUTF(definition);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        code = in.readUTF();
        definition = in.readUTF();
    }
}
