package fr.sii.atlantique.bdf.split;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.batch.item.util.FileUtils;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SplitFlatFileItemWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SplitItemWriter.class);

    private static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");

    private Resource resource;

    private OutputState state = null;

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public void write(List<String> items) {
        LOGGER.debug("Writing to flat file with {} items.", items.size());

        state = getOutputState();

        StringBuilder lines = new StringBuilder();
        for (String item : items) {
            lines.append(item + DEFAULT_LINE_SEPARATOR);
        }
        try {
            state.write(lines.toString());
        } catch (IOException e) {
            throw new WriteFailedException("Could not write data.  The file may be corrupt.", e);
        }
    }

    public void close() {
        if (state != null) {
            state.close();
            if (state.linesWritten == 0) {
                try {
                    resource.getFile().delete();
                } catch (IOException e) {
                    throw new ItemStreamException("Failed to delete empty file on close", e);
                }
            }
            state = null;
        }
    }

    private OutputState getOutputState() {
        if (state == null) {
            File file;
            try {
                file = resource.getFile();
            } catch (IOException e) {
                throw new ItemStreamException("Could not convert resource to file: [" + resource + "]", e);
            }
            Assert.state(!file.exists() || file.canWrite(), "Resource is not writable: [" + resource + "]");
            state = new OutputState();
        }
        return state;
    }

    private class OutputState {

        private FileOutputStream os;

        private Writer outputBufferedWriter;

        private long linesWritten = 0;

        private boolean initialized = false;

        private void initializeBufferedWriter() throws IOException {

            File file = resource.getFile();
            FileUtils.setUpOutputFile(file, false, false, true);
            os = new FileOutputStream(file.getAbsolutePath(), true);
            outputBufferedWriter = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
            outputBufferedWriter.flush();

            Assert.state(outputBufferedWriter != null, "Could not write file : " + file.getAbsolutePath());
            initialized = true;
        }

        void write(String line) throws IOException {
            if (!initialized) {
                initializeBufferedWriter();
            }
            outputBufferedWriter.write(line);
            outputBufferedWriter.flush();
            linesWritten++;
        }

        void close() {
            initialized = false;
            try {
                if (outputBufferedWriter != null) {
                    outputBufferedWriter.close();
                }
            } catch (IOException ioe) {
                throw new ItemStreamException("Unable to close the the ItemWriter", ioe);
            } finally {
                closeStream();
            }
        }

        private void closeStream() {
            try {
                if (os != null) {
                    os.close();
                }
            } catch (IOException ioe) {
                throw new ItemStreamException("Unable to close the the ItemWriter", ioe);
            }
        }
    }
}