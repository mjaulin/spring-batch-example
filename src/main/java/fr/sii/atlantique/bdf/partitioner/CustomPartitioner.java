package fr.sii.atlantique.bdf.partitioner;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Autowired
    private ResourcePatternResolver resoursePatternResolver;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        List<Resource> resources = getResources();
        Map<String, ExecutionContext> map = new HashMap<>(resources.size());
        int i = 0;
        for (Resource resource : resources) {
            Assert.state(resource.exists(), "Resource does not exist: " + resource);
            ExecutionContext context = new ExecutionContext();
            context.putString("file", resource.getFilename());
            map.put("partition" + i, context);
            i++;
        }
        return map;
    }

    private List<Resource> getResources() {
        try {
            return Arrays.asList(resoursePatternResolver.getResources("file:src/main/resources/input/partitioner/*.csv"));
        } catch (IOException e) {
            throw new RuntimeException("I/O problems when resolving the input file pattern.", e);
        }
    }
}
