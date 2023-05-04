package nexmark.sources;

import java.io.IOException;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

public class test {

    public static void main(String[] args) throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer
            .packMapHeader(2)
            .packString("__COM_TYPE__")
            .packString("RUN_FUN")
            .packString("__MSG__")
            .packMapHeader(5)
            .packString("__OP_NAME__")
            .packString("bidsSource")
            .packString("__KEY__")
            .packInt(1)
            .packString("__FUN_NAME")
            .packString("read")
            .packString("__PARAMS__")
            .packArrayHeader(2)
            .packString("grandpa")
            .packInt(88)
            .packString("__PARTITION__")
            .packInt(0);

        System.out.write(packer.toByteArray());

    }
    
}
