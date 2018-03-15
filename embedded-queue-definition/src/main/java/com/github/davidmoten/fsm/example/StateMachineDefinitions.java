package com.github.davidmoten.fsm.example;

import static java.util.Collections.singleton;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import com.github.davidmoten.fsm.model.State;
import com.github.davidmoten.fsm.model.StateMachineDefinition;

public final class StateMachineDefinitions implements Supplier<List<StateMachineDefinition<?>>> {

    @Override
    public List<StateMachineDefinition<?>> get() {
        return Arrays.asList(createReaderStateMachine());

    }

    private static StateMachineDefinition<Reader> createReaderStateMachine() {
        StateMachineDefinition<Reader> m = StateMachineDefinition.create(Reader.class);
        State<Reader, ReaderAdded> created = m.createState("Created") //
                .event(ReaderAdded.class) //
                .documentation("<pre>entry/\n" //
                        + "signal FirstSegmentRequest(reader: self, startingFrom: reader.offset) to store;\n"
                        + "</pre>");
        State<Reader, FirstSegment> hasSegment = m.createState("Has Segment") //
                .event(FirstSegment.class) //
                .documentation("<pre>entry/\n" //
                        + "signal OpenFile to self");
        State<Reader, OpenFile> fileOpened = m.createState("File Opened") //
                .event(OpenFile.class);
        State<Reader, Read> reading = m.createState("Reading") //
                .event(Read.class).documentation("<pre>entry/\n" //
                        + "");
        State<Reader, RequestsMet> metRequests = m.createState("Met Requests") //
                .event(RequestsMet.class);
        State<Reader, EndOfFile> closedFile = m.createState("Closed File") //
                .event(EndOfFile.class) //
                .documentation("<pre>entry/\n" //
                        + "close file;\n" //
                        + "signal RequestNextSegment(reader: self, segmentId: reader.segment.id) to store;\n" //
                        + "</pre>");
        State<Reader, LatestWasRead> latestWasRead = m.createState("Latest Was Read") //
                .event(LatestWasRead.class); 

        created //
                .initial() //
                .to(hasSegment) //
                .to(fileOpened) //
                .to(reading) //
                .to(metRequests);
        reading.to(closedFile);
        reading.to(latestWasRead);

        return m;
    }

}
