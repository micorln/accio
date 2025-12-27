package com.example.accio;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.example.accio.jobs.MapJob;
import com.example.accio.jobs.ReduceJob;
import com.example.accio.utils.FileSplitUnit;

/*
 * Class that handles the spawning of the map and the reduce jobs.
 * Map and reduce both do a sort
 * Map's sort, sorts all the keys in each reducer output
 * Reducer's sort performs merge sort across the outputs of all the maps.
 */ 

public class Coordinator {
    
    private Path logDirectoryPath;
    private int numMapJobs = 5; // modify to come from configs
    private int numReduceJobs = 3; // modify to come from configs 

    private int linesPerUnit = 128; // Has to rely on configs
    private int taskBufferSize = 5;

    ExecutorService mapThreadPool = Executors.newFixedThreadPool(4);

    private ArrayList<MapJob> mapJobs = new ArrayList<>(numMapJobs);
    private ArrayList<Boolean> mapHeartbeats = new ArrayList<>();

    private Queue<FileSplitUnit> taskBuffer = new LinkedList<>() ; 
    private List<Path> logFileList = new ArrayList<>();
    private Iterator<Path> fileIterator;

    private ScheduledExecutorService createTaskScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService mapScheduler = Executors.newSingleThreadScheduledExecutor();

    private String selector;

    Coordinator(Path logDirectoryPath, String selector) {
        this.logDirectoryPath = logDirectoryPath;
        try (Stream<Path> files = Files.list(this.logDirectoryPath)) {
            logFileList = files.collect(Collectors.toList());
            fileIterator = logFileList.iterator();
        } catch (Exception ex) {
            // To DO : Add log here
            throw new RuntimeException("File list crapped out!");
        }
        spawnMapJobs();
        createTaskScheduler.scheduleAtFixedRate(() -> {
            createTasksForMap();
        }, 1L, 5L, TimeUnit.SECONDS);
        mapScheduler.scheduleAtFixedRate(() -> assignTasksToMapJob(), 5L, 5L, TimeUnit.SECONDS);
        this.selector = selector;
    }

    public void updateHeartbeat(int mapJobId, boolean busy) {
        mapHeartbeats.set(mapJobId, busy);
    }

    private void spawnMapJobs() {
        for (int i = 0; i < numMapJobs; i++) {
            mapJobs.add(new MapJob(i, this, selector));
        }
    }

    private void createTasksForMap() {
        while (fileIterator.hasNext() && taskBuffer.size() < taskBufferSize) {
            createNewMapTask();
        }
        if (!fileIterator.hasNext()) {
            mapScheduler.shutdown();
        }
    }

    private void createNewMapTask() {
        Path file = fileIterator.next();
        long lineNumber = 0;
        
        try {
            Long numberOfLines = Files.lines(file).count();
            while (lineNumber < numberOfLines) {
                taskBuffer.add(new FileSplitUnit(file, lineNumber, file.getFileName().toString()));
                lineNumber += linesPerUnit;
            }          
        } catch (Exception ex) {
            throw new RuntimeException("File read crapped out!");
        }   
    }

    private void assignTasksToMapJob() {
        for (int i = 0; i < mapHeartbeats.size(); i++) {
            if (!mapHeartbeats.get(i)) {
                if (!taskBuffer.isEmpty()) {
                    mapJobs.get(i).scheduleMapRun(taskBuffer.poll());
                }
            }
        }
    }


}
