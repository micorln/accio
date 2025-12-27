package com.example.accio.jobs;

import com.example.accio.Coordinator;
import com.example.accio.utils.Buffer;
import com.example.accio.utils.FileSplitUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.stream.Stream;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MapJob {

    private int mapJobId;
    private Buffer buffer;
    private String selector;
    private int reducers;
    // This will be a config
    private int linesPerUnit = 128;
    
    private volatile boolean busy;
    private Coordinator coordinator;

    private ScheduledExecutorService heartbeatUpdater;
    private ExecutorService mapExecutor;

    // Reducers will be a config
    public MapJob(int mapJobId, Coordinator coordinator, String selector) {
        this.mapJobId = mapJobId;
        this.buffer = new Buffer(reducers);
        this.busy = false;
        this.coordinator = coordinator;
        this.heartbeatUpdater = Executors.newSingleThreadScheduledExecutor();
        this.mapExecutor = Executors.newSingleThreadExecutor();
        this.selector = selector;
        scheduleHeartbeatUpdater();
        
    }

    private void scheduleHeartbeatUpdater() {
        heartbeatUpdater.scheduleWithFixedDelay(
            () -> coordinator.updateHeartbeat(mapJobId, busy),
            10,
            10,
            TimeUnit.SECONDS
        );
    }

    private void emit(String key, String value) {
        buffer.add(key, value);
    }

    public void scheduleMapRun(FileSplitUnit fileSplitUnit) {
        mapExecutor.submit(() -> run(fileSplitUnit));
    }

    private void run(FileSplitUnit currentFileSplitUnit) {
        busy = true;
        HashMap<Long, String> linesMap = setupLinesMap(currentFileSplitUnit);
        for (int i = 0; i < linesPerUnit; i++) {
            String line = getLineAtNumber(linesMap, currentFileSplitUnit.getLineNumber() + i);
            if (line.contains(selector)) {
                emit(currentFileSplitUnit.getFileName(), line);
            }
        }
        busy = false;
    }

    private String getLineAtNumber(HashMap <Long, String> linesMap, long lineIndex) {
        if (linesMap.containsKey(lineIndex)) {
            return linesMap.get(lineIndex);
        }
        return null;
    }

    private HashMap <Long, String> setupLinesMap(FileSplitUnit currentFileSplitUnit) {
        HashMap <Long, String> linesMap = new HashMap<>();
        long lineNumber = currentFileSplitUnit.getLineNumber();
        Path path = currentFileSplitUnit.getFilePath();
        try (Stream<String> lines = Files.lines(path).skip(lineNumber)) {
            Iterator<String> iterator = lines.iterator();
            for (int i = 0; i < linesPerUnit; i++) {
                if (!iterator.hasNext()) {
                    break;
                }
                linesMap.put(lineNumber + i, iterator.next());
            }
            return linesMap;
        } catch (Exception ex) {
            System.out.println("File not found " + ex);
            throw new RuntimeException("File not found!", ex);
        }
    }
}
