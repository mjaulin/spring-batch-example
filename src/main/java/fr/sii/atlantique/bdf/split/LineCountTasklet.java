package fr.sii.atlantique.bdf.split;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import java.io.*;

public class LineCountTasklet implements Tasklet, InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(LineCountTasklet.class);

    private Resource resource;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        int lineCount = getLineCount(resource.getFile());

        chunkContext.getStepContext().getStepExecution().getExecutionContext().put("line.count", lineCount);

        LOGGER.info("line count : {}", lineCount);
        return RepeatStatus.FINISHED;
    }

    @Override
    public void afterPropertiesSet() {
        if (resource == null) {
            throw new NullPointerException("Resource mandatory");
        }

        try {
            resource.getFile();
        } catch (IOException e) {
            throw new RuntimeException("Could not convert resource to file: [" + resource + "]", e);
        }
    }

    private int getLineCount(File file) throws IOException {
        try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                byte lastChar = -1;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                    lastChar = c[i];
                }
                if (lastChar != -1 && lastChar != '\n') count++;
            }
            return (count == 0 && !empty) ? 1 : count;
        }
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }
}
