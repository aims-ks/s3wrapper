/*
 *  Copyright (C) 2018 Australian Institute of Marine Science
 *
 *  Contact: Gael Lafond <g.lafond@aims.gov.au>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package au.gov.aims.aws.s3.entity;

import org.json.JSONObject;

import java.util.Map;
import java.util.TreeMap;

public class S3List {
    private Long executionTime = null;
    private Map<String, S3File> dirs;
    private Map<String, S3File> files;

    public S3List() {
        this.dirs = new TreeMap<String, S3File>();
        this.files = new TreeMap<String, S3File>();
    }

    public S3File putFile(S3File s3File) {
        return this.files.put(s3File.getS3Uri().getKey(), s3File);
    }
    public Map<String, S3File> getFiles() {
        return this.files;
    }

    public S3File putDir(S3File s3File) {
        return this.dirs.put(s3File.getS3Uri().getKey(), s3File);
    }
    public Map<String, S3File> getDirs() {
        return this.dirs;
    }

    public void setExecutionTime(Long executionTime) {
        this.executionTime = executionTime;
    }
    public Long getExecutionTime() {
        return this.executionTime;
    }

    public void putAll(S3List otherList) {
        this.dirs.putAll(otherList.dirs);
        this.files.putAll(otherList.files);
    }

    public JSONObject toJSON() {
        JSONObject jsonDirs = new JSONObject();
        for (Map.Entry<String, S3File> dir : this.dirs.entrySet()) {
            jsonDirs.put(dir.getKey(), dir.getValue().toJSON());
        }

        JSONObject jsonFiles = new JSONObject();
        for (Map.Entry<String, S3File> file : this.files.entrySet()) {
            jsonFiles.put(file.getKey(), file.getValue().toJSON());
        }


        JSONObject json = new JSONObject();

        if (jsonDirs.length() > 0) {
            json.put("directories", jsonDirs);
        }

        if (jsonFiles.length() > 0) {
            json.put("files", jsonFiles);
        }

        json.put("executionTime", this.executionTime);

        return json;
    }

    @Override
    public String toString() {
        return this.toJSON().toString(4);
    }
}
