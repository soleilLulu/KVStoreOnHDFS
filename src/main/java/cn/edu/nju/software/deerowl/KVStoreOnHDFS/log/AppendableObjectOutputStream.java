package cn.edu.nju.software.deerowl.KVStoreOnHDFS.log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Created by rale on 11/13/17.
 */
public class AppendableObjectOutputStream extends ObjectOutputStream {
    public AppendableObjectOutputStream(OutputStream out) throws IOException {
        super(out);
    }

    protected AppendableObjectOutputStream() throws IOException, SecurityException {
    }

    @Override
    protected void writeStreamHeader() throws IOException {
        reset();
    }
}
