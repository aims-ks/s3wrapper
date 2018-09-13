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

import java.util.Set;
import java.util.TreeSet;

public class S3List {
	private Long executionTime = null;
	private Set<S3File> dirs;
	private Set<S3File> files;

	public S3List() {
		this.dirs = new TreeSet<S3File>();
		this.files = new TreeSet<S3File>();
	}

	public boolean addFile(S3File s3File) {
		return this.files.add(s3File);
	}
	public Set<S3File> getFiles() {
		return this.files;
	}

	public boolean addDir(S3File s3File) {
		return this.dirs.add(s3File);
	}
	public Set<S3File> getDirs() {
		return this.dirs;
	}

	public void setExecutionTime(Long executionTime) {
		this.executionTime = executionTime;
	}
	public Long getExecutionTime() {
		return this.executionTime;
	}

	public void addAll(S3List otherList) {
		this.dirs.addAll(otherList.dirs);
		this.files.addAll(otherList.files);
	}

	public JSONObject toJSON() {
		JSONObject jsonDirs = new JSONObject();
		for (S3File dir : this.dirs) {
			jsonDirs.put(dir.getS3Uri().getKey(), dir.toJSON());
		}

		JSONObject jsonFiles = new JSONObject();
		for (S3File file : this.files) {
			jsonFiles.put(file.getS3Uri().getKey(), file.toJSON());
		}


		JSONObject json = new JSONObject();

		if (!jsonDirs.isEmpty()) {
			json.put("directories", jsonDirs);
		}

		if (!jsonFiles.isEmpty()) {
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
