package com.example.accio.utils;

import java.util.ArrayList;
import java.util.HashMap;

/*
To do : if buffer size crosses a limit, it has to be spilled to the disk.
create a prune buffer method. Schedule the regular execution of this method in the buffer job.

Regarding sorting :
 -- it's buffer's responsibility to make sure that the spill files are sorted.
 -- it's also buffer's responsibility to make sure that all the spill files are merge sorted at the end.
 -- the final merge of all the spill over files uses the k-way merge algorithm
*/ 

public class Buffer {

    ArrayList<HashMap <String, String>> buffer;
    int bufferLimit = 1000;
    String spillFilePath = "src/main/resources/spill_files/";

    public Buffer(int reducers) {
        this.buffer = new ArrayList<HashMap<String, String>> ();
        for (int i = 0; i < reducers; i++) {
            buffer.add(new HashMap<>());
        }
    }

    public void add(String key, String value) {
        int index = key.hashCode() % buffer.size();
        buffer.get(index).put(key, value);
        
    }

    // write a method to spill over the buffer to disk
    public void spillOver() {
        // mapIndex is used to track the index of the current map in the buffer. Each map corresponds to a reducer.
        int mapIndex = 0;
        for (HashMap<String, String> map : buffer) {
            // write this map to a spill file
            // spill file name can be spillFilePath + "spill_" + i + ".txt"
            // after writing clear the map
            if (map.size() > bufferLimit) {
                try {
                    StringBuilder sb = new StringBuilder();
                    for (HashMap.Entry<String, String> entry : map.entrySet()) {
                        sb.append(entry.getKey()).append("\t").append(entry.getValue()).append("\n");
                    }
                    java.nio.file.Files.write(
                        java.nio.file.Paths.get(spillFilePath + "spill_" + mapIndex + ".txt"),
                        sb.toString().getBytes()
                    );
                } catch (Exception e) {
                    e.printStackTrace();
                }
                map.clear();
            }
        }
        
    }   
}
