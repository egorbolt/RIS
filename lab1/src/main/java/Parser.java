import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public interface Parser {
    Map<String, Integer> usersCorrections = new HashMap<>();
    Map<String, Integer> uniqueKeys = new HashMap<>();

    void parse(InputStream stream) throws IOException;
}
