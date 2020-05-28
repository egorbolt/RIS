import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;

public class Main {
    private static Logger log = LoggerFactory.getLogger(Main.class);
    private static final String archiveName = "RU-NVS.osm.bz2";
    private static File input = new File(archiveName);

    public static void main(String[] args) throws IOException {
        log.info("Process has been started, standby");
        if (!input.exists()) {
            log.error("Error: input doesn't exist");
            return;
        }
        XMLParser XMLparser = new XMLParser();
        BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(
                new FileInputStream(input)
        );
        XMLparser.parse(inputStream);
    }
}
