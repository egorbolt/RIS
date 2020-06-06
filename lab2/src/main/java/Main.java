import database.InsertType;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parser.XMLParser;

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
        BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(
                new FileInputStream(input)
        );

        XMLParser XMLparser = new XMLParser(InsertType.statement);
        XMLparser.parse(inputStream);

        XMLparser = new XMLParser(InsertType.preparedStatement);
        XMLparser.parse(inputStream);

        XMLparser = new XMLParser(InsertType.batch);
        XMLparser.parse(inputStream);
    }
}