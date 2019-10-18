package io.helidon.examples.sport.graph;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Gpx2Json {
    public static void main(String[] args) throws IOException, XPathExpressionException, ParseException {
        File xmlFile = new File(new File("").getAbsoluteFile(),
                "sport-graph-generator/src/main/resources/io/helidon/examples/sport/graph/Afternoon_Run.gpx");
        XPath xPath = XPathFactory.newInstance().newXPath();
        String name = xPath.evaluate("//*[local-name()='trk']/*[local-name()='name']/text()", new InputSource(new FileReader(xmlFile)));
        NodeList trkptList = (NodeList) xPath.evaluate("//*[local-name()='trkpt']", new InputSource(new FileReader(xmlFile)), XPathConstants.NODESET);

        JsonObjectBuilder rootBuilder = Json.createObjectBuilder();
        JsonArrayBuilder trkArrayBuilder = Json.createArrayBuilder();
        rootBuilder.add("name", name);

        DateFormat isoDt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        DateFormat isoDtNoTz = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        for (int i = 0; i < trkptList.getLength(); i++) {
            Element trkPtEl = (Element) trkptList.item(i);
            JsonObjectBuilder trkBuilder = Json.createObjectBuilder();
            Date date = isoDt.parse(trkPtEl.getElementsByTagName("time").item(0).getTextContent());
            trkBuilder.add("ele", trkPtEl.getElementsByTagName("ele").item(0).getTextContent());
            trkBuilder.add("time", isoDtNoTz.format(date));
            trkBuilder.add("lat", trkPtEl.getAttribute("lat"));
            trkBuilder.add("lon", trkPtEl.getAttribute("lon"));
            trkArrayBuilder.add(trkBuilder);
        }
        rootBuilder.add("trkseg", trkArrayBuilder);

        System.out.println(rootBuilder.build().toString());
    }
}
