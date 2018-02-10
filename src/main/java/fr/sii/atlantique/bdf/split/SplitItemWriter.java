package fr.sii.atlantique.bdf.split;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.FileSystemResource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SplitItemWriter implements ItemWriter<String>, InitializingBean {

    private int inputLineCount;

    private int nbFiles;

    private List<SplitFlatFileItemWriter> writers;

    private int readCount = 0;

    @Override
    public void afterPropertiesSet() {
        writers = IntStream.range(1, nbFiles + 1)
                .mapToObj(this::createFlateFileItemWriter)
                .collect(Collectors.toList());
    }

    private SplitFlatFileItemWriter createFlateFileItemWriter(int index) {
        SplitFlatFileItemWriter flatFileItemWriter = new SplitFlatFileItemWriter();
        flatFileItemWriter.setResource(new FileSystemResource("src/main/resources/input/partitioner/partitioner" + index + ".csv"));
        return flatFileItemWriter;
    }

    @Override
    public void write(List<? extends String> items) {
        Map<Integer, List<String>> itemsMap = new HashMap<>();

        for (String item : items) {
            int index = Math.floorDiv(readCount, (int) Math.ceil(inputLineCount / (double) writers.size()));
            if (!itemsMap.containsKey(index)) {
                itemsMap.put(index, new ArrayList<>());
            }
            itemsMap.get(index).add(item);
            readCount++;
        }
        itemsMap.forEach((key, value) -> writers.get(key).write(value));
        if (readCount >= inputLineCount) {
            writers.forEach(SplitFlatFileItemWriter::close);
        }
    }

    public void setInputLineCount(int inputLineCount) {
        this.inputLineCount = inputLineCount;
    }

    public void setNbFiles(int nbFiles) {
        this.nbFiles = nbFiles;
    }
}
