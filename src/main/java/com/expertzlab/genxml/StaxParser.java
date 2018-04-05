package com.expertzlab.genxml;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FileNotFoundException;
import java.io.FileReader;
import javax.xml.stream.XMLStreamConstants;

/**
 * Created by admin on 05/04/18.
 */
public class StaxParser {

    public static void main(String[] args) throws FileNotFoundException, XMLStreamException {

        XMLInputFactory xinf = XMLInputFactory.newFactory();
        XMLEventReader er = xinf.createXMLEventReader( new FileReader("students.xml"));

        while(er.hasNext()){

            XMLEvent event = er.nextEvent();
            switch(event.getEventType()){

                case XMLStreamConstants.START_ELEMENT:
                    StartElement el = event.asStartElement();
                    String qn = el.getName().getLocalPart();
                    if(!"students".equals(qn)) {
                        System.out.println(qn+":");
                    }
                    break;

                case XMLStreamConstants.CHARACTERS:
                    Characters chrs = event.asCharacters();
                    System.out.println(chrs.getData());
                    break;
            }

        }


    }
}
