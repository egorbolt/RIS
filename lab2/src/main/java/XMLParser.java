import dao.*;

import org.openstreetmap.osm._0.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import java.io.InputStream;

import java.util.*;

public class XMLParser implements Parser {
    private static Logger log = LoggerFactory.getLogger(XMLParser.class);
    private PostgresDB database;
    private InsertType insertType;

    public XMLParser(InsertType insertType) {
        database = new PostgresDB();
        this.insertType = insertType;

        try {
            database.dropDB();
            database.createDB();
            database.createTables();
        } catch (Exception e) {
            disconnectFromDB();
        }
    }

    public void parse(InputStream stream) {
        final String fieldNode = "node";
        int nodesAmount = 0;
        NodeDAO nodeDao;
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

        try {
            XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(stream);
            JAXBContext jaxbContext = JAXBContext.newInstance(Node.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            nodeDao = new NodeDAO(database.getConnection());
            ArrayList<Node> nodes = new ArrayList<>();

            while (xmlStreamReader.hasNext()) {
                int event = xmlStreamReader.next();
                if (fieldNode.equals(xmlStreamReader.getLocalName()) && event == XMLEvent.START_ELEMENT) {
                    Node node = (Node) unmarshaller.unmarshal(xmlStreamReader);
                    switch (insertType) {
                        case statement:
                            nodeDao.insertStatement(node);
                            break;
                        case preparedStatement:
                            nodeDao.insertPreparedStatement(node);
                            break;
                        case batch:
                            nodes.add(node);
                            nodesAmount++;
                            if (nodesAmount == 20) {
                                nodesAmount = 0;
                                nodeDao.insertBatch(nodes);
                                nodes = new ArrayList<>();
                            }
                            break;
                    }
                }
            }

            log.info("Speed (nodes): " + nodeDao.timeSpent() + " records per second");
            disconnectFromDB();
        } catch (XMLStreamException e) {
            disconnectFromDB();
            log.error("Error: something wrong with XMLStream");
        } catch (JAXBException e) {
            disconnectFromDB();
            log.error("Error: something wrong with JAXBContext");
        }
    }

    private void disconnectFromDB() {
        if (database.getConnection() != null) {
            database.disconnectFromDB();
            log.info("Warning: disconnected from DB");
        }
    }
}


