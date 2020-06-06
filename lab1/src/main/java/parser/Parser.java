package parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public interface Parser {
    void parse(InputStream stream) throws IOException;
}
