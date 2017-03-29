package org.mp.naumann.reporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class FileReporter implements Reporter {

    private final File file;

    public FileReporter(String fileName) {
        this.file = new File(fileName);
    }

    @Override
    public void writeNewLine(Object[] objects) throws IOException {
        try (BufferedWriter out = new BufferedWriter(new FileWriter(file, true))) {
            String line = Arrays.stream(objects).map(Object::toString).collect(Collectors.joining(","));
            out.write(line);
            out.newLine();
        }
    }
}
