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

import au.gov.aims.aws.s3.S3Utils;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.json.JSONObject;

import java.io.File;
import java.util.Date;
import java.util.Map;

public class S3File implements Comparable<S3File> {
	public static final String USER_METADATA_LAST_MODIFIED_KEY = "lastmodified";

	private final AmazonS3URI s3Uri;
	private final S3Bucket bucket;

	private Long lastModified;
	private Long fileSize;
	private Long expiration;

	private String eTag;
	private String md5sum;
	private String versionId;

	// Used with upload / download
	private File localFile;

	public S3File(AmazonS3URI s3Uri) {
		this.s3Uri = s3Uri;
		this.bucket = new S3Bucket(s3Uri.getBucket());
	}

	public S3File(AmazonS3URI s3Uri, ObjectMetadata objectMetadata) {
		this(s3Uri);

		Map<String, String> userMetadata = objectMetadata.getUserMetadata();

		// Attempt to get the last modified date from the custom metadata
		Long metadataLastModified = null;
		if (userMetadata != null) {
			String lastModifiedStr = userMetadata.get(S3File.USER_METADATA_LAST_MODIFIED_KEY);
			if (lastModifiedStr != null) {
				metadataLastModified = Long.parseLong(lastModifiedStr);
			}
		}
		// If the last modification date could not be found in the custom metadata, fall back
		// to AWS last modification date.
		if (metadataLastModified == null) {
			this.setLastModified(objectMetadata.getLastModified());
		} else {
			this.setLastModified(metadataLastModified);
		}

		this.setExpiration(objectMetadata.getExpirationTime());
		this.setMd5sum(objectMetadata.getContentMD5());
		this.setETag(objectMetadata.getETag());
		this.setVersionId(objectMetadata.getVersionId());
		this.setFileSize(objectMetadata.getContentLength());
	}


	public S3Bucket getBucket() {
		return this.bucket;
	}

	public AmazonS3URI getS3Uri() {
		return this.s3Uri;
	}

	public String getETag() {
		return this.eTag;
	}

	public void setETag(String eTag) {
		this.eTag = eTag;
	}

	public String getMd5sum() {
		return this.md5sum;
	}

	public void setMd5sum(String md5sum) {
		this.md5sum = md5sum;
	}

	public String getVersionId() {
		return this.versionId;
	}

	public void setVersionId(String versionId) {
		this.versionId = versionId;
	}

	public File getLocalFile() {
		return this.localFile;
	}

	public void setLocalFile(File localFile) {
		this.localFile = localFile;
	}


	public Long getLastModified() {
		return this.lastModified;
	}

	public void setLastModified(Date lastModifiedDate) {
		if (lastModifiedDate == null) {
			this.lastModified = null;
		} else {
			this.setLastModified(lastModifiedDate.getTime());
		}
	}
	public void setLastModified(Long lastModified) {
		this.lastModified = lastModified;
	}


	public Long getExpiration() {
		return this.expiration;
	}

	public void setExpiration(Date expirationDate) {
		if (expirationDate == null) {
			this.expiration = null;
		} else {
			this.setExpiration(expirationDate.getTime());
		}
	}
	public void setExpiration(Long expiration) {
		this.expiration = expiration;
	}


	public Long getFileSize() {
		return this.fileSize;
	}

	public void setFileSize(Long fileSize) {
		this.fileSize = fileSize;
	}

	public boolean isPublic(S3Client client) {
		AccessControlList s3FileAcl = client.getS3().getObjectAcl(this.s3Uri.getBucket(), this.s3Uri.getKey());
		if (s3FileAcl == null) {
			return false;
		}

		return S3Utils.isPublic(s3FileAcl);
	}


	public JSONObject toJSON() {
		JSONObject json = new JSONObject();

		json.put("key", this.s3Uri.getKey());
		json.put("uri", this.s3Uri.getURI().toString());
		json.put("filename", S3Utils.getFilename(this.s3Uri));
		json.put("directory", S3Utils.getDirectoryName(this.s3Uri));
		json.put("localFile", this.localFile);

		json.put("size", this.fileSize);
		json.put("lastModified", this.lastModified);

		json.put("eTag", this.eTag);
		json.put("md5sum", this.md5sum);
		json.put("versionId", this.versionId);

		return json;
	}

	@Override
	public String toString() {
		return this.toJSON().toString(4);
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof S3File)) {
			return false;
		}
		return this.toString().equals(o.toString());
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	@Override
	public int compareTo(S3File o) {
		return this.toString().compareTo(o.toString());
	}
}
