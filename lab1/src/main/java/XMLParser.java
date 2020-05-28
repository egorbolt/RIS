import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class XMLParser implements Parser {
    private static Logger log = LoggerFactory.getLogger(XMLParser.class);

    public XMLParser() {}

    public void parse(InputStream stream) throws IOException {
        final String fieldNode = "node";
        final String fieldUser = "user";
        final String fieldUID = "uid";

        log.info("Parsing has been started");
        try {
            XMLEventReader xmlEventReader = XMLInputFactory.newInstance().createXMLEventReader(stream);
            while (xmlEventReader.hasNext()) {
                XMLEvent xmlEvent = xmlEventReader.nextEvent();
                if (xmlEvent.isStartElement()) {
                    StartElement startElement = xmlEvent.asStartElement();
                    if (startElement.getName().getLocalPart().equals(fieldNode)) {
                        findFields(usersCorrections, startElement, fieldUser);
                        findFields(uniqueKeys, startElement, fieldUID);
                    }
                }
            }
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        log.info("Parsing has been stopped");

        printUserCorrections();
        printUniqueKeys();

        log.info("Process has been finished, check the results");
        stream.close();
    }

    // MARK: - Search function
    private void findFields(Map<String, Integer> map, StartElement startElement, String field) {
        Attribute attribute = startElement.getAttributeByName(new QName(field));
        if (attribute != null) {
            if (!map.containsKey(attribute.getValue())) {
                map.put(attribute.getValue(), 1);
            }
            else {
                map.put(
                        attribute.getValue(),
                        map.get(attribute.getValue()) + 1
                );
            }
        }
    }

    // MARK: - Print functions
    private void printUserCorrections() {
        usersCorrections
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .map(e ->
                        "User: " + e.getKey() + ", corrections: " + e.getValue()
                )
                .forEach(System.out::println);
    }

    private void printUniqueKeys() {
        uniqueKeys
                .entrySet()
                .stream()
                .map(e ->
                        "Unique key: " + e.getKey().toString() + ", number of unique tags: " + e.getValue()
                )
                .forEach(System.out::println);
    }
}
